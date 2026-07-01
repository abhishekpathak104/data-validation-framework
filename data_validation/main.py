"""
data_validation/main.py
-----------------------
Entry point for the data validation framework.

Reads job configuration from a local ``.conf`` file, establishes a MySQL
connection, initialises Spark and BigQuery clients, then routes execution to
the appropriate validation processor (GCS file, Cloud SQL table, or BigQuery
table) based on the ``job_type`` argument.

Usage::

    spark-submit main.py <job_type> <object_id> <job_id> [<extra_args>...]

    job_type: 'gcs' | 'cloudsql' | 'bigquery'

Environment variables:
    CONF_FILE_GCS - GCS path to the .conf file (e.g. 'gs://my-bucket/config/data_validation.conf').
    DB_PASSWORD   - MySQL password (overrides the value in the .conf file).
"""

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta

import mysql.connector
import pandas as pd
from google.cloud import bigquery
from mysql.connector import Error
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_validation.connectors.source_extract import (
    load_data_from_cloudsql,
    load_table,
    load_table_cnt,
    write_results,
)
from data_validation.notifications.mailer import notification_alert
from data_validation.validators.business import val_exe_tbl
from data_validation.validators.custodial import (
    process_bigquery,
    process_cloudsql,
    process_gcs,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants (populated by _load_config())
# ---------------------------------------------------------------------------
_CONF_FILE_PATH = "/root/conf/datavalidation.conf"

_PERIOD_TO_MINS = {
    "full table": 15_778_800,
    "daily": 1_440,
    "monthly": 43_830,
    "yearly": 15_778_800,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config(conf_path):
    """Parse a simple ``key=value`` configuration file into a dictionary.

    Lines that do not contain an ``=`` character are silently skipped.

    Args:
        conf_path (str): Absolute path to the configuration file.

    Returns:
        dict: Mapping of configuration key → value strings.
    """
    config = {}
    with open(conf_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                key, _, value = line.partition("=")
                config[key.strip()] = value.strip()
    return config


def get_connection(host, database, user, password):
    """Open and return a MySQL database connection.

    Args:
        host (str): MySQL host IP or hostname.
        database (str): MySQL database/schema name.
        user (str): MySQL username.
        password (str): MySQL password.

    Returns:
        mysql.connector.connection.MySQLConnection | None: An open connection,
        or ``None`` if the connection attempt failed.
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )
        if conn.is_connected():
            logger.info("Connected to MySQL database.")
    except Error as exc:
        logger.error("Failed to connect to MySQL: %s", exc)
    return conn


def _get_run_id(client, dataset_id, obj_log_table, object_id):
    """Fetch the next run ID for ``object_id`` from BigQuery.

    Args:
        client: ``google.cloud.bigquery.Client`` instance.
        dataset_id (str): BigQuery dataset identifier.
        obj_log_table (str): Object log table name.
        object_id (int | str): Object identifier.

    Returns:
        int: Next run ID (defaults to ``1`` when no prior runs exist).
    """
    run_id = 1
    query = (
        f"SELECT MAX(Object_log_id) + 1 AS RunId "
        f"FROM `{dataset_id}.{obj_log_table}` "
        f"WHERE CAST(Last_Update_Date AS date) = CURRENT_DATE "
        f"AND object_id = {object_id}"
    )
    try:
        for row in client.query(query).result():
            if row.RunId is not None:
                run_id = int(row.RunId)
    except Exception as exc:
        logger.warning("Could not fetch run_id, defaulting to 1: %s", exc)
    return run_id


def calculate_validation_date(
    object_id,
    time_interval,
    validation_period,
    validation_frequency,
    job_id,
    run_id,
    client,
    dataset_id,
    execution_stat_table,
):
    """Determine the validation date window and insert an execution status row.

    Queries the execution status table to find the previous run's date
    boundaries and calculates the current and next window accordingly.  A new
    row is inserted with ``Val_Status = 'Executing'``.

    Args:
        object_id (int | str): Object identifier.
        time_interval (int | None): Fixed interval in minutes.  When ``None``,
            ``validation_period`` is used to look up the duration.
        validation_period (str): Period key — one of ``'full table'``,
            ``'daily'``, ``'monthly'``, ``'yearly'``.
        validation_frequency (str): Frequency description (stored as metadata).
        job_id (str): Job identifier stored in the execution status row.
        run_id (int): Current run identifier.
        client: ``google.cloud.bigquery.Client`` instance.
        dataset_id (str): BigQuery dataset identifier.
        execution_stat_table (str): Execution status table name.

    Returns:
        tuple: ``(val_st_dt, val_end_dt, run_id)`` — the start datetime,
        end datetime, and (possibly updated) run ID.
    """
    last_update_date = datetime.utcnow()

    duration_min = (
        int(time_interval)
        if time_interval is not None
        else int(_PERIOD_TO_MINS[validation_period])
    )

    query = (
        f"SELECT Prev_Val_St_Dt, Prev_Val_End_Dt, Curr_Val_St_Dt, Curr_Val_End_Dt, "
        f"Next_Val_St_Dt, Next_Val_End_Dt, Val_Status, Run_Id "
        f"FROM `{dataset_id}.{execution_stat_table}` "
        f"WHERE Object_ID = {object_id} "
        f"AND Last_Upd_Dt = ("
        f"  SELECT MAX(Last_Upd_Dt) FROM `{dataset_id}.{execution_stat_table}` "
        f"  WHERE Object_ID = {object_id})"
    )

    df = client.query(query).to_dataframe()
    logger.debug("Execution status rows:\n%s", df)

    insert_sql = (
        f"INSERT INTO `{dataset_id}.{execution_stat_table}` "
        "(Object_ID, Validation_Period, Validation_frequency, Run_Type, "
        "Prev_Val_St_Dt, Prev_Val_End_Dt, Curr_Val_St_Dt, Curr_Val_End_Dt, "
        "Next_Val_St_Dt, Next_Val_End_Dt, Val_Status, Last_Upd_Dt, Run_Id, Job_Id) "
        "VALUES ({obj_id}, '{val_period}', '{val_freq}', 'Automatic', "
        "'{prev_st}', '{prev_end}', '{curr_st}', '{curr_end}', "
        "'{next_st}', '{next_end}', 'Executing', '{upd_dt}', {run_id}, '{job_id}')"
    )

    val_st_dt = None
    val_end_dt = None

    if not df.empty:
        val_status = df["Val_Status"][0]

        if val_status == "Success":
            if validation_period == "full table":
                prev_val_st_dt = df["Curr_Val_St_Dt"][0]
                prev_val_end_dt = df["Curr_Val_End_Dt"][0]
                val_st_dt = datetime(1900, 1, 1, 0, 0, 0)
                val_end_dt = datetime.utcnow()
                next_val_st_dt = val_st_dt
                next_val_end_dt = datetime.utcnow() + timedelta(days=1)
                logger.debug("Success — full table mode.")
            else:
                prev_val_st_dt = df["Next_Val_St_Dt"][0] + timedelta(minutes=-duration_min)
                prev_val_end_dt = df["Next_Val_End_Dt"][0] + timedelta(minutes=-duration_min)
                val_st_dt = df["Next_Val_St_Dt"][0]
                val_end_dt = df["Next_Val_End_Dt"][0]
                next_val_st_dt = df["Next_Val_St_Dt"][0] + timedelta(minutes=duration_min)
                next_val_end_dt = df["Next_Val_End_Dt"][0] + timedelta(minutes=duration_min)
                logger.debug("Success — delta mode.")

            client.query(insert_sql.format(
                obj_id=object_id,
                val_period=validation_period,
                val_freq=validation_frequency,
                prev_st=prev_val_st_dt,
                prev_end=prev_val_end_dt,
                curr_st=val_st_dt,
                curr_end=val_end_dt,
                next_st=next_val_st_dt,
                next_end=next_val_end_dt,
                upd_dt=last_update_date,
                run_id=run_id,
                job_id=job_id,
            ))

        elif val_status == "Executing":
            val_st_dt = df["Curr_Val_St_Dt"][0]
            val_end_dt = df["Curr_Val_End_Dt"][0]
            run_id = df["Run_Id"][0]

        elif val_status == "Failure":
            logger.warning("Previous execution for object_id=%s ended in Failure.", object_id)
            val_st_dt = df["Curr_Val_St_Dt"][0]
            val_end_dt = df["Curr_Val_End_Dt"][0]

    else:
        logger.info(
            "No past records found for object_id=%s in %s. "
            "Inserting a new row and validating for today.",
            object_id,
            execution_stat_table,
        )
        if validation_period == "full table":
            val_st_dt = datetime(1900, 1, 1, 0, 0, 0)
            val_end_dt = datetime.utcnow()
            prev_val_st_dt = "NULL"
            prev_val_end_dt = "NULL"
            next_val_st_dt = datetime(1900, 1, 1, 0, 0, 0)
            next_val_end_dt = datetime.utcnow() + timedelta(days=1)
        else:
            val_end_dt = datetime.utcnow()
            val_st_dt = val_end_dt + timedelta(minutes=-duration_min)
            prev_val_st_dt = "NULL"
            prev_val_end_dt = "NULL"
            next_val_st_dt = val_st_dt + timedelta(minutes=duration_min)
            next_val_end_dt = val_end_dt + timedelta(minutes=duration_min)

        # Use a slightly different INSERT for the "no prior records" case
        # where prev dates may be NULL literals.
        insert_sql_first = (
            f"INSERT INTO `{dataset_id}.{execution_stat_table}` "
            "(Object_ID, Validation_Period, Validation_frequency, Run_Type, "
            "Prev_Val_St_Dt, Prev_Val_End_Dt, Curr_Val_St_Dt, Curr_Val_End_Dt, "
            "Next_Val_St_Dt, Next_Val_End_Dt, Val_Status, Last_Upd_Dt, Run_Id, Job_Id) "
            f"VALUES ({object_id}, '{validation_period}', '{validation_frequency}', "
            f"'Automatic', {prev_val_st_dt}, {prev_val_end_dt}, "
            f"'{val_st_dt}', '{val_end_dt}', '{next_val_st_dt}', '{next_val_end_dt}', "
            f"'Executing', '{last_update_date}', {run_id}, '{job_id}')"
        )
        logger.debug("First-run INSERT: %s", insert_sql_first)
        client.query(insert_sql_first)

    return val_st_dt, val_end_dt, run_id


def _update_execution_status(client, dataset_id, execution_stat_table, status, object_id, run_id=None):
    """Update the Val_Status column in the execution statistics table.

    Args:
        client: ``google.cloud.bigquery.Client`` instance.
        dataset_id (str): BigQuery dataset identifier.
        execution_stat_table (str): Execution status table name.
        status (str): New status value — ``'Success'`` or ``'Failure'``.
        object_id (int | str): Object identifier to filter on.
        run_id (int | None): Run identifier to filter on (optional).
    """
    now = datetime.utcnow()
    run_filter = f" AND Run_Id = {run_id}" if run_id is not None else ""
    update_sql = (
        f'UPDATE `{dataset_id}.{execution_stat_table}` '
        f'SET Val_Status = "{status}", Last_Upd_Dt = "{now}" '
        f'WHERE Object_ID = {object_id}{run_filter} '
        f'AND Val_Status = "Executing"'
    )
    logger.debug("Status update: %s", update_sql)
    client.query(update_sql)


# ---------------------------------------------------------------------------
# Job processors
# ---------------------------------------------------------------------------

def _process_gcs_job(
    cursor,
    database,
    client,
    spark,
    sc,
    dataset_id,
    output_table,
    output_agg_table,
    obj_log_table,
    output_dir,
    execution_stat_table,
    job_id,
):
    """Handle validation for a GCS file trigger.

    Args:
        cursor: Active MySQL cursor.
        database (str): MySQL metadata database name.
        client: BigQuery client.
        spark: Active SparkSession.
        sc: SparkContext.
        dataset_id (str): BigQuery dataset identifier.
        output_table (str): BigQuery detailed error table.
        output_agg_table (str): BigQuery aggregated error table.
        obj_log_table (str): BigQuery object log table.
        output_dir (str): GCS output directory.
        execution_stat_table (str): BigQuery execution status table.
        job_id (str): Job identifier.
    """
    bucket = sys.argv[3]
    filepath = sys.argv[4]
    gcs_dataset_id = sys.argv[5]
    table_id = sys.argv[6]

    threshold_query = (
        "SELECT DISTINCT Failure_Threshold_Value, Criticality, Mapping_ID "
        "FROM data_validation_rule_threshold WHERE Active = 1"
    )
    rule_query = (
        "SELECT a.Object_ID, b.Rule_ID, b.Mapping_ID, a.Object_Name, "
        "Primary_Key, Rule_Logic, Column_Name, participating_table, "
        "a.Object_Extension, Validation_Period, Time_Interval, "
        "Validation_Frequency, c.Rule_Description "
        "FROM data_validation_object_lookup a "
        "JOIN data_validation_rule_mapping b ON a.Object_ID = b.Object_ID "
        "JOIN data_validation_rule c ON b.Rule_ID = c.Rule_ID "
        "WHERE a.Object_Database_Name = %s "
        "AND a.Object_Name = %s "
        "AND a.Active = 1 AND b.Active = 1"
    )

    cursor.execute(rule_query, (gcs_dataset_id, table_id))
    pdf = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "Object_ID", "Rule_ID", "Mapping_ID", "Object_Name", "Primary_Key",
            "Rule_Logic", "Column_Name", "participating_table", "Object_Extension",
            "Validation_Period", "Time_Interval", "Validation_Frequency", "Rule_Description",
        ],
    )
    cursor.execute(threshold_query)

    object_id = pdf["Object_ID"][0]
    run_id = _get_run_id(client, dataset_id, obj_log_table, object_id)

    validation_period = str(pdf["Validation_Period"][0]).lower()
    time_interval = pdf["Time_Interval"][0]
    validation_frequency = pdf["Validation_Frequency"][0]
    val_st_dt, val_end_dt, run_id = calculate_validation_date(
        object_id, time_interval, validation_period, validation_frequency,
        job_id, run_id, client, dataset_id, execution_stat_table,
    )

    criticality_df = pd.DataFrame(
        cursor.fetchall(),
        columns=["Failure_Threshold_Value", "Criticality", "Mapping_ID"],
    )

    if not pdf.empty:
        total_results, err_agg_df, log_df = process_gcs(
            pdf, run_id, criticality_df, job_id, bucket, filepath,
            client.project, gcs_dataset_id, table_id, spark,
            val_st_dt, val_end_dt, sc, cursor, database,
        )
        write_results(total_results, dataset_id, output_table, output_dir, job_id, spark)
        write_results(err_agg_df, dataset_id, output_agg_table, output_dir, job_id, spark)
        write_results(log_df, dataset_id, obj_log_table, output_dir, job_id, spark)
    else:
        logger.warning("No active rule or table entry found in the Metadata table — aborting.")

    _update_execution_status(client, dataset_id, execution_stat_table, "Success", object_id, run_id)


def _process_cloudsql_job(
    cursor,
    database,
    host,
    user,
    pswd,
    client,
    spark,
    sc,
    dataset_id,
    output_table,
    output_agg_table,
    obj_log_table,
    output_dir,
    execution_stat_table,
    job_id,
):
    """Handle validation for a Cloud SQL table.

    Args:
        cursor: Active MySQL cursor.
        database (str): MySQL metadata database name.
        host (str): Cloud SQL host.
        user (str): MySQL username.
        pswd (str): MySQL password.
        client: BigQuery client.
        spark: Active SparkSession.
        sc: SparkContext.
        dataset_id (str): BigQuery dataset identifier.
        output_table (str): BigQuery detailed error table.
        output_agg_table (str): BigQuery aggregated error table.
        obj_log_table (str): BigQuery object log table.
        output_dir (str): GCS output directory.
        execution_stat_table (str): BigQuery execution status table.
        job_id (str): Job identifier.
    """
    object_id = int(sys.argv[2])
    threshold_query = (
        "SELECT DISTINCT Failure_Threshold_Value, Criticality, Mapping_ID "
        "FROM data_validation_rule_threshold WHERE Active = 1"
    )
    rule_query = (
        "SELECT DISTINCT a.Object_ID, participating_table AS Source_Table, "
        "b.Rule_ID, b.Mapping_ID, a.Object_Name, Primary_Key, Rule_Logic, "
        "Column_Name, a.Validation_Period, participating_table, "
        "b.Source_Table_Date_Col, Validation_Frequency, Time_Interval, "
        "Object_Database_Name, c.Rule_Description "
        "FROM data_validation_object_lookup a "
        "JOIN data_validation_rule_mapping b ON a.Object_ID = b.Object_ID "
        "JOIN data_validation_rule c ON b.Rule_ID = c.Rule_ID "
        "WHERE a.Object_ID = %s "
        "AND b.Test_Type = 'Custodial' AND a.Active = 1 AND b.Active = 1"
    )

    cursor.execute(rule_query, (object_id,))
    pdf = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "Object_ID", "source_table", "Rule_ID", "Mapping_ID", "Object_Name",
            "Primary_Key", "Rule_Logic", "Column_Name", "Validation_Period",
            "participating_table", "Source_Table_Date_Col", "Validation_Frequency",
            "Time_Interval", "Object_Database_Name", "Rule_Description",
        ],
    )
    cursor.execute(threshold_query)

    db_name_actual = pdf["Object_Database_Name"][0]
    db_name_arg = sys.argv[4]
    logger.info("Actual Object_Database_Name: %s", db_name_actual)

    if db_name_actual != db_name_arg:
        raise ValueError(
            f"Object_Database_Name mismatch: metadata='{db_name_actual}' "
            f"vs argument='{db_name_arg}'"
        )

    run_id = _get_run_id(client, dataset_id, obj_log_table, object_id)

    criticality_df = pd.DataFrame(
        cursor.fetchall(),
        columns=["Failure_Threshold_Value", "Criticality", "Mapping_ID", "Object_Name", "Rule_Description"],
    )

    if not pdf.empty:
        validation_period = str(pdf["Validation_Period"][0]).lower()
        time_interval = pdf["Time_Interval"][0]
        validation_frequency = pdf["Validation_Frequency"][0]
        val_st_dt, val_end_dt, run_id = calculate_validation_date(
            object_id, time_interval, validation_period, validation_frequency,
            job_id, run_id, client, dataset_id, execution_stat_table,
        )

        total_results, err_agg_df, log_df = process_cloudsql(
            pdf, job_id, criticality_df, host, dataset_id, user, pswd,
            client.project, spark, run_id, val_st_dt, val_end_dt, cursor, sc, database,
        )
        write_results(total_results, dataset_id, output_table, output_dir, job_id, spark)
        write_results(err_agg_df, dataset_id, output_agg_table, output_dir, job_id, spark)
        write_results(log_df, dataset_id, obj_log_table, output_dir, job_id, spark)
    else:
        logger.warning("No active rule or table entry found in the Metadata table — aborting.")

    _update_execution_status(client, dataset_id, execution_stat_table, "Success", object_id)


def _process_bigquery_job(
    cursor,
    database,
    client,
    spark,
    sc,
    dataset_id,
    output_table,
    output_agg_table,
    obj_log_table,
    output_dir,
    execution_stat_table,
    job_id,
):
    """Handle validation for a BigQuery table (business + custodial rules).

    Args:
        cursor: Active MySQL cursor.
        database (str): MySQL metadata database name.
        client: BigQuery client.
        spark: Active SparkSession.
        sc: SparkContext.
        dataset_id (str): BigQuery dataset identifier.
        output_table (str): BigQuery detailed error table.
        output_agg_table (str): BigQuery aggregated error table.
        obj_log_table (str): BigQuery object log table.
        output_dir (str): GCS output directory.
        execution_stat_table (str): BigQuery execution status table.
        job_id (str): Job identifier.
    """
    object_id = int(sys.argv[2])
    threshold_query = (
        "SELECT DISTINCT Failure_Threshold_Value, Criticality, Mapping_ID "
        "FROM data_validation_rule_threshold WHERE Active = 1"
    )

    # ---- Business rules ----
    business_query = (
        "SELECT DISTINCT a.Object_ID, Object_Name AS source_table, b.Rule_ID, "
        "b.Mapping_ID, a.Object_Name, Primary_Key, Rule_Logic, Column_Name, "
        "a.Validation_Period, participating_table, Time_Interval, "
        "Source_Table_Date_Col, Validation_Frequency, Object_Database_Name, "
        "c.Rule_Description "
        "FROM data_validation_object_lookup a "
        "JOIN data_validation_rule_mapping b ON a.Object_ID = b.Object_ID "
        "JOIN data_validation_rule c ON b.Rule_ID = c.Rule_ID "
        "JOIN data_validation_rule_threshold d ON b.Mapping_ID = d.Mapping_ID "
        "WHERE a.Object_ID = %s "
        "AND b.Test_Type = 'Business' AND a.Active = 1 AND b.Active = 1"
    )

    cursor.execute(business_query, (object_id,))
    df_business = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "Object_ID", "source_table", "Rule_ID", "Mapping_ID", "Object_Name",
            "Primary_Key", "Rule_Logic", "Column_Name", "Validation_Period",
            "participating_table", "Time_Interval", "Source_Table_Date_Col",
            "Validation_Frequency", "Object_Database_Name", "Rule_Description",
        ],
    )

    db_name_actual = df_business["Object_Database_Name"][0]
    db_name_arg = sys.argv[4]
    logger.info("Actual Object_Database_Name: %s", db_name_actual)

    if db_name_actual != db_name_arg:
        raise ValueError(
            f"Object_Database_Name mismatch: metadata='{db_name_actual}' "
            f"vs argument='{db_name_arg}'"
        )

    run_id = _get_run_id(client, dataset_id, obj_log_table, object_id)
    cursor.execute(threshold_query)
    criticality_df = pd.DataFrame(
        cursor.fetchall(),
        columns=["Failure_Threshold_Value", "Criticality", "Mapping_ID"],
    )

    # ---- Custodial rules ----
    custodial_query = (
        "SELECT DISTINCT a.Object_ID, Object_Name AS source_table, b.Rule_ID, "
        "b.Mapping_ID, a.Object_Name, Primary_Key, Rule_Logic, Column_Name, "
        "a.Validation_Period, participating_table, Time_Interval, "
        "Source_Table_Date_Col, Validation_Frequency, Object_Database_Name, "
        "c.Rule_Description "
        "FROM data_validation_object_lookup a "
        "JOIN data_validation_rule_mapping b ON a.Object_ID = b.Object_ID "
        "JOIN data_validation_rule c ON b.Rule_ID = c.Rule_ID "
        "JOIN data_validation_rule_threshold d ON b.Mapping_ID = d.Mapping_ID "
        "WHERE a.Object_ID = %s "
        "AND b.Test_Type = 'Custodial' AND b.Active = 1"
    )
    cursor.execute(custodial_query, (object_id,))
    df_custodial = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "Object_ID", "source_table", "Rule_ID", "Mapping_ID", "Object_Name",
            "Primary_Key", "Rule_Logic", "Column_Name", "Validation_Period",
            "participating_table", "Time_Interval", "Source_Table_Date_Col",
            "Validation_Frequency", "Object_Database_Name", "Rule_Description",
        ],
    )

    if df_business.empty and df_custodial.empty:
        logger.warning("No active rule or table entry found in the Metadata table — aborting.")
        return

    full_output_table = f"{dataset_id}.{output_table}"

    if not df_business.empty:
        validation_period = str(df_business["Validation_Period"][0]).lower()
        time_interval = df_business["Time_Interval"][0]
        validation_frequency = df_business["Validation_Frequency"][0]
        val_st_dt, val_end_dt, run_id = calculate_validation_date(
            object_id, time_interval, validation_period, validation_frequency,
            job_id, run_id, client, dataset_id, execution_stat_table,
        )
        biz_results, biz_err_agg, biz_log = val_exe_tbl(
            df_business, criticality_df, run_id, client, spark,
            client.project, sc, val_st_dt, val_end_dt, full_output_table, cursor,
        )
        write_results(biz_results, dataset_id, output_table, output_dir, job_id, spark)
        write_results(biz_err_agg, dataset_id, output_agg_table, output_dir, job_id, spark)
        write_results(biz_log, dataset_id, obj_log_table, output_dir, job_id, spark)
        notification_alert(biz_err_agg, cursor, database)

    if not df_custodial.empty:
        validation_period = str(df_custodial["Validation_Period"][0]).lower()
        time_interval = df_custodial["Time_Interval"][0]
        validation_frequency = df_custodial["Validation_Frequency"][0]
        val_st_dt, val_end_dt, run_id = calculate_validation_date(
            object_id, time_interval, validation_period, validation_frequency,
            job_id, run_id, client, dataset_id, execution_stat_table,
        )
        cust_results, cust_err_agg, cust_log = process_bigquery(
            df_custodial, criticality_df, run_id, spark, client.project,
            sc, val_st_dt, val_end_dt, cursor, database,
        )
        write_results(cust_results, dataset_id, output_table, output_dir, job_id, spark)
        write_results(cust_err_agg, dataset_id, output_agg_table, output_dir, job_id, spark)
        write_results(cust_log, dataset_id, obj_log_table, output_dir, job_id, spark)
        notification_alert(cust_err_agg, cursor, database)

    _update_execution_status(client, dataset_id, execution_stat_table, "Success", object_id, run_id)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    """Orchestrate the data validation pipeline.

    Reads CLI arguments, loads configuration, connects to MySQL, initialises
    Spark and BigQuery, then delegates to the appropriate job processor based
    on ``job_type``.

    CLI args:
        sys.argv[1]: job_type  — ``'gcs'``, ``'cloudsql'``, or ``'bigquery'``.
        sys.argv[2]: object_id — used for cloudsql/bigquery jobs.
        sys.argv[3]: job_id   — for gcs jobs this is the GCS bucket.
        sys.argv[4]+: additional args depending on job_type.
    """
    job_type = str(sys.argv[1])
    job_id = sys.argv[3]

    # Download and parse the configuration file from GCS.
    conf_file_gcs = os.environ["CONF_FILE_GCS"]
    subprocess.check_call(
        ["gsutil", "cp", conf_file_gcs, _CONF_FILE_PATH]
    )
    config = _load_config(_CONF_FILE_PATH)

    project = config["project"]
    table_name = config["table_name"]
    dataset_id = config["dataset_id"]
    output_table = config["output_table"]
    output_dir = config["output_dir"]
    output_agg_table = config["output_agg_table"]
    obj_log_table = config["obj_log_table"]
    user = config["user"]
    pswd = os.environ.get("DB_PASSWORD", config.get("pswd"))
    host = config["hostip"]
    database = config["database"]
    execution_stat_table = config["execution_stat_table"]

    logger.info("Project: %s", project)
    logger.info("TABLE_NAME: %s", table_name)
    logger.info("DATASET_ID: %s", dataset_id)

    bq_client = bigquery.Client(project)

    # Initialise Spark.
    sc = SparkContext.getOrCreate()
    spark = (
        SparkSession.builder
        .master("yarn")
        .appName("data validation")
        .getOrCreate()
    )

    bucket = spark.sparkContext._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
    spark.conf.set("temporaryGcsBucket", bucket)

    conn = get_connection(host, database, user, pswd)
    cursor = conn.cursor()

    processor_kwargs = dict(
        cursor=cursor,
        database=database,
        client=bq_client,
        spark=spark,
        sc=sc,
        dataset_id=dataset_id,
        output_table=output_table,
        output_agg_table=output_agg_table,
        obj_log_table=obj_log_table,
        output_dir=output_dir,
        execution_stat_table=execution_stat_table,
        job_id=job_id,
    )

    if job_type == "gcs":
        try:
            _process_gcs_job(**processor_kwargs)
        except Exception as exc:
            # GCS jobs have no Object_ID CLI argument (only bucket/filepath/dataset/table_id),
            # so there is no reliable Object_ID to update the execution status row for here.
            logger.error("Unable to process GCS file(s): %s", exc)

    elif job_type == "cloudsql":
        try:
            _process_cloudsql_job(
                **processor_kwargs,
                host=host,
                user=user,
                pswd=pswd,
            )
        except Exception as exc:
            logger.error("Unable to process Cloud SQL table: %s", exc)
            _update_execution_status(
                bq_client, dataset_id, execution_stat_table,
                "Failure", sys.argv[2],
            )

    elif job_type == "bigquery":
        try:
            _process_bigquery_job(**processor_kwargs)
        except Exception as exc:
            logger.error("Unable to process BigQuery table: %s", exc)
            _update_execution_status(
                bq_client, dataset_id, execution_stat_table,
                "Failure", sys.argv[2],
            )

    else:
        logger.error("Unknown job_type '%s'. Expected: gcs | cloudsql | bigquery.", job_type)
        sys.exit(1)


if __name__ == "__main__":
    main()