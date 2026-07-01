"""
data_validation/connectors/source_extract.py
--------------------------------------------
Utility functions for loading data from BigQuery and Cloud SQL, and for
writing Spark DataFrame results back to BigQuery via GCS staging.
"""

import logging
import subprocess
from datetime import datetime

from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


def write_results(results, output_dataset, output_table, output_dir, job_id, spark):
    """Write a Spark DataFrame of validation results to BigQuery.

    Exports the DataFrame as newline-delimited JSON to a GCS staging
    directory, loads it into BigQuery via ``bq load``, then deletes the
    staging files.

    Args:
        results: Spark DataFrame containing the rows to write.  When empty
            (``head(1)`` returns nothing) the function is a no-op.
        output_dataset (str): BigQuery dataset name to write results to.
        output_table (str): BigQuery table name to write results to.
        output_dir (str): GCS bucket/path prefix for staging JSON files
            (e.g. ``'my-bucket/tmp'``).
        job_id (str): Unique job identifier appended to the staging path so
            that concurrent jobs do not collide.
        spark: Active ``SparkSession`` instance.
    """
    if not results:
        logger.warning("write_results called with no results object for job_id=%s.", job_id)
        return

    if len(results.head(1)) == 0:
        logger.debug("No records to write for job_id=%s — skipping.", job_id)
        return

    output_directory = f"gs://{output_dir}/{job_id}"
    output_files = output_directory + "/part-*"

    results.write.format("json").save(output_directory)
    subprocess.check_call(
        (
            f"bq load --source_format NEWLINE_DELIMITED_JSON "
            f"{output_dataset}.{output_table} {output_files}"
        ).split()
    )

    # Clean up GCS staging files.
    output_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_directory)
    output_path.getFileSystem(
        spark.sparkContext._jsc.hadoopConfiguration()
    ).delete(output_path, True)

    logger.info(
        "Wrote results to %s.%s.", output_dataset, output_table
    )


def load_table_cnt(
    bq_table_name,
    client,
    validation_period,
    spark,
    filter_col,
    val_st_dt,
    val_end_dt,
):
    """Return the row count for a BigQuery table, optionally filtered by date.

    Args:
        bq_table_name (str): Fully-qualified BigQuery table name
            (e.g. ``'project.dataset.table'``).
        client: ``google.cloud.bigquery.Client`` instance.
        validation_period (str): ``'full table'`` for a full scan; any other
            value triggers a date-range filter using ``filter_col``.
        spark: Active ``SparkSession`` (unused but kept for API consistency).
        filter_col (str): Column name used for date-range filtering.
        val_st_dt: Start of the validation window (inclusive).
        val_end_dt: End of the validation window (inclusive).

    Returns:
        int: Row count matching the query criteria.
    """
    if validation_period == "full table":
        count_query = f"SELECT COUNT(*) AS cnt FROM {bq_table_name}"
    else:
        count_query = (
            f"SELECT COUNT(*) AS cnt FROM {bq_table_name} "
            f"WHERE {filter_col} BETWEEN '{val_st_dt}' AND '{val_end_dt}'"
        )

    logger.debug("Row count query: %s", count_query)
    query_job = client.query(count_query)

    cnt = 0
    for row in query_job:
        cnt = row["cnt"]
    return cnt


def load_table(
    bq_table_name,
    spark_view_name,
    validation_period,
    spark,
    source_table_date_col,
    val_st_dt,
    val_end_dt,
):
    """Load a BigQuery table into a Spark temporary view.

    Reads the specified BigQuery table (optionally filtered by date range)
    and registers it as a Spark SQL temporary view so that downstream
    validation queries can reference it by ``spark_view_name``.

    Args:
        bq_table_name (str): Fully-qualified BigQuery table name.
        spark_view_name (str): Name of the temporary Spark view to create.
        validation_period (str): ``'full table'`` for an unfiltered read;
            any other value applies a date-range filter.
        spark: Active ``SparkSession`` instance.
        source_table_date_col (str): Column name used for date filtering.
        val_st_dt: Start of the validation window (inclusive).
        val_end_dt: End of the validation window (inclusive).
    """
    if validation_period == "full table":
        df = spark.read.format("bigquery").option("table", bq_table_name).load()
    else:
        df = (
            spark.read.format("bigquery")
            .option("table", bq_table_name)
            .option("filter", f"{source_table_date_col} >= '{val_st_dt}'")
            .option("filter", f"{source_table_date_col} <= '{val_end_dt}'")
            .load()
        )

    df.createOrReplaceTempView(spark_view_name)


def load_data_from_cloudsql(
    table,
    dataset_id,
    host,
    user,
    pswd,
    validation_period,
    source_table_date_col,
    val_st_dt,
    val_end_dt,
    spark,
):
    """Load a Cloud SQL (MySQL) table into a Spark DataFrame via JDBC.

    Args:
        table (str): Source table name in Cloud SQL.
        dataset_id (str): MySQL database/schema name.
        host (str): Cloud SQL instance IP or hostname.
        user (str): MySQL user name.
        pswd (str): MySQL password.
        validation_period (str): ``'full table'`` for a full read; any other
            value applies a date-range filter.
        source_table_date_col (str): Column name used for date filtering.
        val_st_dt: Start of the validation window (inclusive).
        val_end_dt: End of the validation window (inclusive).
        spark: Active ``SparkSession`` instance.

    Returns:
        Spark DataFrame containing the loaded rows.
    """
    jdbc_url = f"jdbc:mysql://{host}:3306/{dataset_id}"

    if validation_period == "full table":
        pushdown_query = (
            f"(SELECT * FROM {table} WHERE {source_table_date_col}) AS alias_tbl"
        )
    else:
        pushdown_query = (
            f"(SELECT * FROM {table} "
            f"WHERE {source_table_date_col} "
            f"BETWEEN '{val_st_dt}' AND '{val_end_dt}') AS alias_tbl"
        )

    logger.debug("JDBC pushdown query: %s", pushdown_query)

    jdbc_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", pushdown_query)
        .option("user", user)
        .option("password", pswd)
        .load()
    )
    return jdbc_df