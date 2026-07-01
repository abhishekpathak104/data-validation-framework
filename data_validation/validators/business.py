"""
data_validation/validators/business.py
--------------------------------------
Executes business-rule validation by pushing SQL rule-logic down to BigQuery
and collecting failure counts and criticality scores.
"""

import logging
import time
from datetime import datetime

import mysql.connector
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.functions import lit

from data_validation.connectors.source_extract import load_table_cnt

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------
_ERR_AGG_SQL = (
    "SELECT '{last_update_date}' AS Last_Update_Date, "
    "{failure_percent} AS Failure_Percent, "
    "{failure_count} AS Failure_Count, "
    "'{column_name}' AS Column_Name, "
    "{mapping_id} AS Mapping_ID, "
    "{rule_id} AS Rule_ID, "
    "{object_id} AS Object_ID, "
    "CAST('{period}' AS date) AS Period, "
    "{run_id} AS Run_ID, "
    "{criticality} AS Criticality, "
    "'{object_name}' AS Object_Name, "
    '\"{rule_description}\" AS Rule_Description'
)

_OBJ_LOG_SQL = (
    "SELECT {object_log_id} AS Object_Log_ID, "
    "CAST('{period}' AS date) AS Period, "
    "{object_id} AS Object_ID, "
    "'{object_name}' AS Object_Name, "
    "'{last_update_date}' AS Last_Update_Date"
)

_INSERT_ERROR_SQL = (
    "INSERT INTO `{error_table}` "
    "( Run_ID, Period, Object_ID, Rule_ID, Mapping_ID, Column_Name, "
    "Primary_Key_Column_s_, Last_Update_Date, Primary_Key_Value_s_, Actual_Value ) "
    "({query})"
)

_COUNT_ERROR_SQL = (
    "SELECT COUNT(*) AS Failure_count FROM `{error_table}` "
    "WHERE Object_ID = {object_id} AND Rule_ID = {rule_id} "
    "AND Run_ID = {run_id} "
    "AND CAST(Last_Update_Date AS date) = CURRENT_DATE() "
    "GROUP BY Run_ID, Mapping_ID"
)

# ---------------------------------------------------------------------------
# Result schema definitions
# ---------------------------------------------------------------------------
_RESULT_SCHEMA = StructType([
    StructField("Primary_Key_Value_s_", StringType(), True),
    StructField("Actual_Value", StringType(), True),
    StructField("Run_ID", IntegerType(), True),
    StructField("Period", DateType(), True),
    StructField("Object_ID", IntegerType(), True),
    StructField("Rule_ID", IntegerType(), True),
    StructField("Mapping_ID", IntegerType(), True),
    StructField("Column_Name", StringType(), True),
    StructField("Primary_Key_Column_s_", StringType(), True),
    StructField("Last_Update_Date", TimestampType(), True),
])

_ERR_AGG_SCHEMA = StructType([
    StructField("Last_Update_Date", TimestampType(), True),
    StructField("Failure_Percent", FloatType(), True),
    StructField("Failure_Count", IntegerType(), True),
    StructField("Column_Name", StringType(), True),
    StructField("Mapping_ID", IntegerType(), True),
    StructField("Rule_ID", IntegerType(), True),
    StructField("Object_ID", IntegerType(), True),
    StructField("Period", DateType(), True),
    StructField("Run_ID", IntegerType(), True),
    StructField("Criticality", IntegerType(), True),
    StructField("Object_Name", StringType(), True),
    StructField("Rule_Description", StringType(), True),
])

_LOG_SCHEMA = StructType([
    StructField("Object_Log_ID", IntegerType(), True),
    StructField("Period", DateType(), True),
    StructField("Object_ID", IntegerType(), True),
    StructField("Object_Name", StringType(), True),
    StructField("Last_Update_Date", TimestampType(), True),
])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def val_exe_tbl(
    pdf,
    criticality_df,
    run_id,
    client,
    spark,
    project,
    sc,
    val_st_dt,
    val_end_dt,
    output_table,
    cursor,
):
    """Execute business-rule validation for each rule row in ``pdf``.

    For every rule mapping in the input DataFrame, pushes the SQL rule logic
    down to BigQuery, counts failures, determines criticality, and accumulates
    results into three Spark DataFrames.

    Args:
        pdf (pd.DataFrame): Pandas DataFrame of rule mappings.  Expected
            columns: ``Rule_Logic``, ``Object_ID``, ``Rule_ID``,
            ``Mapping_ID``, ``Column_Name``, ``Primary_Key``,
            ``Object_Database_Name``, ``Object_Name``,
            ``Validation_Period``, ``Source_Table_Date_Col``,
            ``Rule_Description``.
        criticality_df (pd.DataFrame): DataFrame mapping
            ``Failure_Threshold_Value`` → ``Criticality`` → ``Mapping_ID``.
        run_id (int): Current run identifier.
        client: ``google.cloud.bigquery.Client`` instance.
        spark: Active ``SparkSession``.
        project (str): GCP project identifier (unused; kept for API compat).
        sc: Spark context (``SparkContext``).
        val_st_dt: Validation window start datetime.
        val_end_dt: Validation window end datetime.
        output_table (str): Fully-qualified BigQuery table name for error rows.
        cursor: Active MySQL cursor for metadata queries.

    Returns:
        tuple: ``(result_df, err_agg_df, log_df)`` — three Spark DataFrames
        containing detailed results, aggregated error stats, and object log
        entries respectively.
    """
    sql_context = SQLContext(spark.sparkContext)

    result_df = sql_context.createDataFrame(sc.emptyRDD(), _RESULT_SCHEMA)
    err_agg_df = sql_context.createDataFrame(sc.emptyRDD(), _ERR_AGG_SCHEMA)
    log_df = sql_context.createDataFrame(sc.emptyRDD(), _LOG_SCHEMA)

    for idx in pdf.index:
        try:
            sql = str(pdf["Rule_Logic"][idx])
            today = datetime.utcnow()
            period = today.date()
            rule_description = pdf["Rule_Description"][idx]
            object_id = int(pdf["Object_ID"][idx])
            rule_id = int(pdf["Rule_ID"][idx])
            mapping_id = int(pdf["Mapping_ID"][idx])
            column_name = str(pdf["Column_Name"][idx])
            primary_key_col = str(pdf["Primary_Key"][idx])
            last_update_date = today
            src_tbl_name = ".".join(
                (pdf["Object_Database_Name"][idx], pdf["Object_Name"][idx])
            )
            object_name = pdf["Object_Name"][idx]
            validation_period = pdf["Validation_Period"][idx]
            source_table_date_col = pdf["Source_Table_Date_Col"][idx]

            logger.debug("val_st_dt=%s  val_end_dt=%s", val_st_dt, val_end_dt)

            src_tbl_cnt = load_table_cnt(
                src_tbl_name,
                client,
                validation_period,
                spark,
                source_table_date_col,
                val_st_dt,
                val_end_dt,
            )
            failure_count = _validate_push_down(
                sql[:-1],
                primary_key_col,
                column_name,
                run_id,
                period,
                object_id,
                rule_id,
                mapping_id,
                last_update_date,
                val_st_dt,
                val_end_dt,
                output_table,
                client,
                spark,
            )
            criticality = "null"
            failure_percent = 0.0

            logger.debug("src_tbl_cnt=%s", src_tbl_cnt)

            if src_tbl_cnt > 0 and failure_count > 0:
                failure_percent = failure_count / src_tbl_cnt
                logger.debug(
                    "failure_percent=%.4f  src_tbl_cnt=%s  failure_count=%s",
                    failure_percent,
                    src_tbl_cnt,
                    failure_count,
                )
                for cri_idx in criticality_df.index:
                    if failure_percent >= criticality_df["Failure_Threshold_Value"][cri_idx]:
                        criticality = int(criticality_df["Criticality"][cri_idx])

            if failure_count == 0 and src_tbl_cnt > 0:
                logger.info(
                    "New records arrived but no validation errors found "
                    "for rule_id=%s.", rule_id
                )

            err_sql = _ERR_AGG_SQL.format(
                last_update_date=last_update_date,
                failure_percent=failure_percent,
                failure_count=failure_count,
                column_name=column_name,
                mapping_id=mapping_id,
                rule_id=rule_id,
                object_id=object_id,
                period=period,
                run_id=run_id,
                criticality=criticality,
                object_name=object_name,
                rule_description=rule_description,
            )
            err_agg_df = err_agg_df.union(spark.sql(err_sql))

            log_sql = _OBJ_LOG_SQL.format(
                object_log_id=run_id,
                period=period,
                object_id=object_id,
                object_name=object_name,
                last_update_date=last_update_date,
            )
            log_df = log_df.union(spark.sql(log_sql))

            logger.info("Criticality for rule_id=%s: %s", rule_id, criticality)

        except mysql.connector.Error as exc:
            logger.error(
                "MySQL error while validating source_table=%s: %s",
                pdf["source_table"][idx],
                exc,
            )

    log_df = log_df.dropDuplicates()
    log_df = log_df.withColumn("val_st_dt", lit(val_st_dt)).withColumn(
        "val_end_dt", lit(val_end_dt)
    )
    return result_df, err_agg_df, log_df


def _validate_push_down(
    sql,
    primary_key,
    column_name,
    run_id,
    period,
    object_id,
    rule_id,
    mapping_id,
    last_update_date,
    val_st_dt,
    val_end_dt,
    error_table,
    client,
    spark,
):
    """Push rule-logic SQL to BigQuery and return the failure row count.

    Constructs a SELECT statement that wraps the rule SQL, inserts the
    matching rows into the error table in BigQuery, then queries back the
    count of inserted rows.

    Args:
        sql (str): Rule logic SQL string (sub-query body).
        primary_key (str): Comma-separated primary key column names.
        column_name (str): Comma-separated validated column names.
        run_id (int): Current run identifier.
        period (datetime.date): Validation period date.
        object_id (int): Object identifier.
        rule_id (int): Rule identifier.
        mapping_id (int): Mapping identifier.
        last_update_date (datetime): Timestamp of this validation run.
        val_st_dt: Validation window start.
        val_end_dt: Validation window end.
        error_table (str): Fully-qualified BigQuery error table name.
        client: ``google.cloud.bigquery.Client`` instance.
        spark: Active ``SparkSession``.

    Returns:
        int: Number of failure rows inserted into the error table.
    """
    primary_key_list = primary_key.split(", ")
    column_name_list = column_name.split(", ")

    # Build primary key concat fragment.
    pk_concat_parts = "".join(
        f" CAST({pk} AS STRING), ', ' ," for pk in primary_key_list
    )
    pk_label = ", ".join(f"'{pk}'" for pk in primary_key_list)

    # Build column value concat fragment.
    col_concat_parts = "".join(
        f" CAST({cn} AS STRING), ',' ," for cn in column_name_list
    )

    select_stmt = (
        "SELECT "
        "{run_id} AS Run_ID, "
        "CAST('{period}' AS date) AS Period, "
        "{object_id} AS Object_ID, "
        "{rule_id} AS Rule_ID, "
        "{mapping_id} AS Mapping_ID, "
        "CAST('{column_name}' AS STRING) AS Column_Name, "
        "CONCAT({pk_label}) AS Primary_Key_Column_s_, "
        "CAST('{last_update_date}' AS TIMESTAMP) AS Last_Update_Date, "
        "CONCAT({pk_concat}) AS Primary_Key_Value_s_, "
        "CONCAT({col_concat}) AS Actual_Value "
        "FROM ({sql})"
    ).format(
        run_id=run_id,
        period=period,
        object_id=object_id,
        rule_id=rule_id,
        mapping_id=mapping_id,
        column_name=column_name,
        pk_label=pk_label,
        last_update_date=last_update_date,
        pk_concat=pk_concat_parts[:-7],   # strip trailing ",', ' ,"
        col_concat=col_concat_parts[:-7],
        sql=sql,
    )

    insert_query = _INSERT_ERROR_SQL.format(error_table=error_table, query=select_stmt)
    insert_query = insert_query.format(val_st_dt=val_st_dt, val_end_dt=val_end_dt)
    logger.debug("Insert query: %s", insert_query)

    bq_job = client.query(insert_query)

    # Wait for the insert job to complete before counting.
    failure_count = 0
    if bq_job.done():
        cnt_query = _COUNT_ERROR_SQL.format(
            error_table=error_table,
            object_id=object_id,
            rule_id=rule_id,
            run_id=run_id,
        )
        logger.debug("Count query: %s", cnt_query)
        for row in client.query(cnt_query):
            failure_count = row["Failure_count"]

    logger.debug("Failure count for rule_id=%s: %s", rule_id, failure_count)
    return failure_count