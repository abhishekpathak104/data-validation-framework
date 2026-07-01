"""
data_validation/validators/custodial.py
---------------------------------------
Runs custodial (schema/data-quality) validations against BigQuery tables,
Cloud SQL (MySQL) tables, and GCS files (CSV, JSON, XML).

The module exposes three top-level processors:
    - :func:`process_bigquery`
    - :func:`process_cloudsql`
    - :func:`process_gcs`

Each processor iterates over a pandas DataFrame of rule mappings, loads the
relevant data source, runs the appropriate validation tests, and returns three
Spark DataFrames: detailed results, aggregated error stats, and an object log.
"""

import csv
import json
import logging
import os
from collections import OrderedDict
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, storage
from pyspark.sql import SQLContext
from pyspark.sql.functions import (
    collect_list,
    col,
    concat,
    count,
    explode,
    length,
    lit,
    size,
    substring,
    substring_index,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_validation.connectors.source_extract import load_data_from_cloudsql, load_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL Templates
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

# ---------------------------------------------------------------------------
# Shared Spark schema definitions
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

_VALIDATE_RESULT_SCHEMA = StructType([
    StructField("Primary_Key_Value_s_", StringType(), True),
    StructField("Actual_Value", StringType(), True),
])


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_validation_schema(
    project,
    dataset_id,
    table,
    cursor,
    rule_id,
    database,
    ref_schema_loc="bigquery",
):
    """Load and merge BigQuery column schema with MySQL validation rules.

    When ``ref_schema_loc`` is ``'bigquery'``, the function queries the
    BigQuery ``INFORMATION_SCHEMA.COLUMNS`` view to obtain data-type
    information and merges it with rule logic fetched from the MySQL metadata
    tables.  When ``ref_schema_loc`` is ``None``, only the MySQL metadata
    tables are consulted.

    Args:
        project (str): GCP project identifier.
        dataset_id (str): BigQuery dataset (or MySQL database) name.
        table (str): Table name to load the schema for.
        cursor: Active MySQL database cursor.
        rule_id (int): Rule identifier used to filter metadata rows.
        database (str): MySQL database/schema name for metadata tables.
        ref_schema_loc (str | None): ``'bigquery'`` (default) to combine BQ
            schema with MySQL rules; ``None`` to use MySQL metadata only.

    Returns:
        OrderedDict | None: Column-keyed ordered dictionary of validation
        constraints, or ``None`` on error.
    """

    def _load_permitted_values(validation_constraints):
        """Expand a permitted-values constraint to a flat list.

        Accepts either a plain list of values or a dict describing a BigQuery
        lookup (with keys ``project``, ``dataset_id``, ``table_name``,
        ``column_name``).

        Args:
            validation_constraints (list | dict): Raw constraint value from
                the rule JSON.

        Returns:
            list: Flat list of permitted (or forbidden) values.
        """
        values = []
        if isinstance(validation_constraints, list):
            values.extend(validation_constraints)
        elif isinstance(validation_constraints, dict):
            bq_client = bigquery.Client(project)
            src_col = validation_constraints["column_name"]
            lookup_query = (
                "SELECT DISTINCT {col} FROM `{proj}.{ds}.{tbl}`".format(
                    col=src_col,
                    proj=validation_constraints["project"],
                    ds=validation_constraints["dataset_id"],
                    tbl=validation_constraints["table_name"],
                )
            )
            results = bq_client.query(lookup_query).result()
            values.extend([row[0] for row in results])
        return values

    try:
        logger.debug("ref_schema_loc=%s", ref_schema_loc)
        val_schema = OrderedDict()

        if ref_schema_loc == "bigquery":
            bq_client = bigquery.Client(project)

            mysql_query = f"""
                SELECT DISTINCT
                    t3.Test_Type AS type,
                    t4.Rule_Logic,
                    t4.Rule_Description AS test_type,
                    t2.object_name AS object_name,
                    t3.Column_Name
                FROM {database}.data_validation_object_lookup t2
                JOIN {database}.data_validation_rule_mapping t3
                    ON t2.Object_Id = t3.Object_Id
                INNER JOIN {database}.data_validation_rule t4
                    ON t3.Rule_Id = t4.Rule_Id
                WHERE t2.object_name = '{table}'
                  AND t4.Rule_ID = {rule_id}
            """

            bq_query = (
                "SELECT DISTINCT "
                "t1.ordinal_position, t1.table_name, "
                "t1.column_name AS column, t1.data_type, t1.is_nullable "
                f"FROM `{dataset_id}.INFORMATION_SCHEMA.COLUMNS` AS t1 "
                f"WHERE table_name = '{table}'"
            )

            df_info_schema = bq_client.query(bq_query).to_dataframe()
            cursor.execute(mysql_query)
            df_lookup = pd.DataFrame(
                cursor.fetchall(),
                columns=["type", "Rule_Logic", "test_type", "object_name", "Column_Name"],
            )

            merged = pd.merge(
                df_info_schema,
                df_lookup,
                left_on="column",
                right_on="Column_Name",
                how="inner",
            )
            df = merged[
                ["ordinal_position", "table_name", "column", "data_type",
                 "is_nullable", "type", "Rule_Logic", "test_type"]
            ]

            for _, row in df.iterrows():
                out_dict = {
                    "ordinal_position": row["ordinal_position"],
                    "data_type": row["data_type"],
                    "is_nullable": row["is_nullable"],
                }
                if row["Rule_Logic"]:
                    vc = json.loads(
                        row["Rule_Logic"].replace("'", '"').replace("True", "true")
                    )
                    for key, value in vc.items():
                        if key in ("allowed", "forbidden"):
                            out_dict[key] = _load_permitted_values(value)
                        else:
                            out_dict[key] = value
                val_schema[row["column"]] = out_dict

        elif ref_schema_loc is None:
            query = f"""
                SELECT DISTINCT
                    t2.object_name AS table_name,
                    t3.column_name,
                    t4.Rule_Logic
                FROM {database}.data_validation_object_lookup t2
                LEFT JOIN {database}.data_validation_rule_mapping t3
                    ON t2.Object_Id = t3.Object_Id
                INNER JOIN {database}.data_validation_rule t4
                    ON t3.Rule_Id = t4.Rule_Id
                WHERE Object_Database_Name = '{dataset_id}'
                  AND t2.object_name = '{table}'
                  AND t4.Rule_ID = {rule_id}
            """
            logger.debug("MySQL metadata query: %s", query)
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(),
                columns=["table_name", "column_name", "Rule_Logic"],
            )

            for _, row in df.iterrows():
                out_dict = {}
                if row["Rule_Logic"]:
                    vc = json.loads(
                        row["Rule_Logic"].replace("'", '"').replace("True", "true")
                    )
                    for key, value in vc.items():
                        if key in ("allowed", "forbidden"):
                            out_dict[key] = _load_permitted_values(value)
                        else:
                            out_dict[key] = value
                val_schema[row["column_name"]] = out_dict

        logger.debug("Validation schema loaded: %s", val_schema)
        return val_schema

    except Exception as exc:
        logger.error("Unable to generate validation schema: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Individual validation tests
# ---------------------------------------------------------------------------

def test_distinct(col_name, df, primary_key_column, spark):
    """Test whether ``col_name`` contains duplicate values.

    Args:
        col_name (str): Column to check for duplicates.
        df: Spark DataFrame containing the data.
        primary_key_column (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame of failing rows, or ``None`` on error.
    """
    try:
        df.createOrReplaceTempView("tmpView")
        pk_list = primary_key_column.split(",")
        pk_concat = "".join(f" CAST({pk} AS STRING), ',' ," for pk in pk_list)
        select_sql = (
            f"SELECT {col_name}, CONCAT({pk_concat[:-7]}) AS Primary_Key_Value "
            "FROM tmpView"
        )
        df2 = spark.sql(select_sql)
        df2 = df2.filter(col(col_name).isNotNull())
        df3 = (
            df2.groupBy(df2.columns)
            .agg(collect_list("Primary_Key_Value").alias("Primary_Key_Values"))
            .where(size(col("Primary_Key_Values")) > 1)
        )
        df3 = df3.withColumn("Primary_Key_Values", explode("Primary_Key_Values"))
        return prep_df(df3, "business", col_name, "distinct", "Primary_Key_Values", spark)
    except Exception as exc:
        logger.error("Unable to run distinct test: %s", exc)
        return None


def test_range(col_name, val, test, df, primary_key, spark):
    """Test whether ``col_name`` violates a min/max (value, length, or date) bound.

    Args:
        col_name (str): Column to test.
        val: Threshold value (numeric, string, or ``[value, format]`` list for
            date tests).
        test (str): Test type — one of ``'min'``, ``'max'``, ``'min_length'``,
            ``'max_length'``, ``'min_date'``, ``'max_date'`` (and common
            aliases).
        df: Spark DataFrame containing the data.
        primary_key (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame of failing rows, or ``None`` on error.
    """
    try:
        df2 = df.filter(col(col_name).isNotNull())
        test_lower = test.lower()

        if test_lower == "min":
            df2 = df2.filter(df[col_name] < val)
        elif test_lower == "max":
            df2 = df2.filter(df[col_name] > val)
        elif test_lower in ("min_length", "minlength"):
            df2 = df2.filter(length(col_name) < val)
        elif test_lower in ("max_length", "maxlength"):
            df2 = df2.filter(length(col_name) > val)
        elif test_lower in ("min_date", "mindate", "min-date"):
            if isinstance(val, list):
                df2 = df2.filter(
                    to_timestamp(lit(df[col_name]), format=val[1]) < to_timestamp(lit(val[0]))
                )
            else:
                df2 = df2.filter(
                    to_timestamp(lit(df[col_name])) < to_timestamp(lit(val))
                )
        elif test_lower in ("max_date", "maxdate", "max-date"):
            if isinstance(val, list):
                df2 = df2.filter(
                    to_timestamp(lit(df[col_name]), format=val[1]) > to_timestamp(lit(val[0]))
                )
            else:
                df2 = df2.filter(
                    to_timestamp(lit(df[col_name])) > to_timestamp(lit(val))
                )

        return prep_df(df2, "business", col_name, test, primary_key, spark)
    except Exception as exc:
        logger.error("Unable to run %s test: %s", test, exc)
        return None


def test_membership(col_name, allowed, df, primary_key, spark):
    """Test whether values in ``col_name`` are within the allowed set.

    Args:
        col_name (str): Column to test.
        allowed (list): List of permitted values.
        df: Spark DataFrame containing the data.
        primary_key (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame of failing rows, or ``None`` on error.
    """
    try:
        df2 = df.filter(col(col_name).isNotNull())
        df2 = df2.where(~col(col_name).isin(allowed))
        return prep_df(df2, "business", col_name, "allowed", primary_key, spark)
    except Exception as exc:
        logger.error("Unable to run membership test: %s", exc)
        return None


def test_exclusion(col_name, forbidden, df, primary_key, spark):
    """Test whether values in ``col_name`` appear in the forbidden set.

    Args:
        col_name (str): Column to test.
        forbidden (list): List of forbidden values.
        df: Spark DataFrame containing the data.
        primary_key (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame of failing rows, or ``None`` on error.
    """
    try:
        df2 = df.filter(col(col_name).isNotNull())
        df2 = df2.where(col(col_name).isin(forbidden))
        return prep_df(df2, "business", col_name, "forbidden", primary_key, spark)
    except Exception as exc:
        logger.error("Unable to run exclusion test: %s", exc)
        return None


def test_type(val_schema, df, spark, primary_key):
    """Test that the inferred Spark column types match the schema-specified types.

    Due to imperfect Spark type inference and type-system mismatches between
    CloudSQL, BigQuery, and Spark, an ``acceptable_types`` mapping provides
    reasonable flexibility to reduce spurious errors.

    Args:
        val_schema (OrderedDict): Column-keyed validation schema (from
            :func:`load_validation_schema`).
        df: Spark DataFrame to type-check.
        spark: Active ``SparkSession``.
        primary_key (str): Comma-separated primary key column names.

    Returns:
        Spark DataFrame of type-mismatch rows, or ``None`` on error.
    """
    acceptable_types = {
        "TIMESTAMP": ["timestamp", "date", "string"],
        "DATE": ["timestamp", "date", "string"],
        "TIME": ["timestamp", "date", "string"],
        "DATETIME": ["timestamp", "date", "string"],
        "STRING": [
            "timestamp", "date", "boolean", "float", "int", "tinyint",
            "smallint", "bigint", "double", "decimal(10,0)", "string",
        ],
        "BYTES": ["string"],
        "BOOL": ["boolean", "string"],
        "FLOAT64": [
            "float", "int", "tinyint", "smallint", "bigint",
            "double", "decimal(10,0)", "string",
        ],
        "INT64": ["int", "tinyint", "smallint", "bigint", "string"],
        "NUMERIC": [
            "float", "int", "tinyint", "smallint", "bigint",
            "double", "decimal(10,0)", "string",
        ],
        "ARRAY": ["string"],
        "STRUCT": ["string"],
        "GEOGRAPHY": ["string"],
    }
    try:
        dtypes = df.dtypes
        errors = []
        for i, (col_name, inferred_type) in enumerate(dtypes):
            try:
                expected_type = val_schema[col_name]["data_type"]
                if inferred_type not in acceptable_types[expected_type]:
                    errors.append([
                        col_name,
                        (
                            f"{col_name} detected as {inferred_type} type. "
                            f"Schema specifies {expected_type} type."
                        ),
                    ])
            except (KeyError, TypeError):
                pass

        results = spark.createDataFrame(
            errors, schema="column STRING, validation_errors STRING"
        )
        return prep_df(results, "custodial", "column", "type", "validation_errors", spark)
    except Exception as exc:
        logger.error("Unable to run type test: %s", exc)
        return None


def test_nulls(col_name, df, primary_key, spark):
    """Test if any null values exist in ``col_name``.

    Args:
        col_name (str): Column to check for nulls.
        df: Spark DataFrame containing the data.
        primary_key (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame of null rows, or ``None`` on error.
    """
    try:
        df2 = df.filter(col(col_name).isNull())
        df2 = df2.withColumn(
            "validation_errors", concat(lit(col_name), lit(" cannot be null"))
        )
        return prep_df(df2, "custodial", col_name, "nullability", primary_key, spark)
    except Exception as exc:
        logger.error("Unable to run nullability test: %s", exc)
        return None


def test_regex(col_name, regex, df, primary_key, spark):
    """Test if values in ``col_name`` match ``regex``.

    Args:
        col_name (str): Column to test.
        regex (str): Regular expression pattern that values must match.
        df: Spark DataFrame containing the data.
        primary_key (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame of non-matching rows, or ``None`` on error.
    """
    try:
        df2 = df.filter(col(col_name).isNotNull())
        df2 = df2.filter(~df[col_name].rlike(regex))
        return prep_df(df2, "business", col_name, f"regex: {regex}", primary_key, spark)
    except Exception as exc:
        logger.error("Unable to run regex test: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Validation orchestrator
# ---------------------------------------------------------------------------

def validate(val_schema, dataset_id, table_id, df, spark, errors, primary_key, sc):
    """Validate a Spark DataFrame against a schema of rules.

    Iterates over each column and its associated tests in ``val_schema`` and
    dispatches to the appropriate test function.  Supported test keys:
    ``data_type``, ``is_nullable``, ``distinct``, ``regex``, ``min``/``max``
    (and length/date variants), ``allowed``, ``forbidden``,
    ``custom_sql``, ``custom_spark``.

    .. warning::
        The ``custom_spark`` test type executes arbitrary Python code strings
        stored in the rule-logic metadata via ``exec()``.  It is disabled by
        default; set the ``ALLOW_CUSTOM_SPARK_EXEC=1`` environment variable to
        enable it, and only do so if the MySQL metadata database is a trusted,
        access-controlled system (anyone who can write a rule row can run
        arbitrary code on the Spark cluster). Prefer ``custom_sql`` where possible.

    Args:
        val_schema (OrderedDict): Column-keyed validation schema.
        dataset_id (str): Dataset identifier (for context).
        table_id (str): Table identifier (for context).
        df: Spark DataFrame to validate.
        spark: Active ``SparkSession``.
        errors (list): Pre-computed global errors (currently unused by callers
            but kept for API compatibility).
        primary_key (str): Comma-separated primary key column names.
        sc: ``SparkContext`` instance.

    Returns:
        Spark DataFrame of all validation failures found.
    """
    sql_context = SQLContext(spark.sparkContext)
    results = sql_context.createDataFrame(sc.emptyRDD(), _VALIDATE_RESULT_SCHEMA)

    if val_schema and "data_type" in list(val_schema.values())[0]:
        result = test_type(val_schema, df, spark, primary_key)
        if result is not None and len(result.head(1)) != 0:
            results = results.union(result)

    for col_name, tests in val_schema.items():
        for test_key, test_val in tests.items():
            result = None

            if test_key.lower() == "custom_spark":
                if os.environ.get("ALLOW_CUSTOM_SPARK_EXEC") != "1":
                    logger.error(
                        "custom_spark rule for column=%s skipped: set "
                        "ALLOW_CUSTOM_SPARK_EXEC=1 to enable exec()-based rules "
                        "(only do so with a trusted rule-metadata database).",
                        col_name,
                    )
                    continue
                # WARNING: exec() executes arbitrary code. Ensure rule_logic
                # metadata is sourced only from trusted, access-controlled
                # systems.
                func_body = test_val
                if func_body.startswith("def"):
                    func_body = func_body[func_body.find(":") + 1:]
                namespace = {}
                exec(f"def custom_func(df):{func_body}", namespace)  # noqa: S102
                result = prep_df(
                    namespace["custom_func"](df),
                    "business",
                    col_name,
                    test_val,
                    primary_key,
                    spark,
                )

            elif test_key.lower() == "custom_sql":
                df.createOrReplaceTempView("dfSQL")
                result = prep_df(
                    spark.sql(test_val),
                    "business",
                    col_name,
                    test_val,
                    primary_key,
                    spark,
                )

            elif test_key.lower() == "distinct":
                result = test_distinct(col_name, df, primary_key, spark)

            elif test_key.lower() == "is_nullable":
                if test_val == "NO":
                    result = test_nulls(col_name, df, primary_key, spark)

            elif test_key.lower() == "regex":
                result = test_regex(col_name, test_val, df, primary_key, spark)

            elif "min" in test_key or "max" in test_key:
                result = test_range(col_name, test_val, test_key, df, primary_key, spark)

            elif test_key.lower() == "allowed":
                result = test_membership(col_name, test_val, df, primary_key, spark)

            elif test_key.lower() == "forbidden":
                result = test_exclusion(col_name, test_val, df, primary_key, spark)

            if result is not None and len(result.head(1)) != 0:
                results = results.union(result)

    return results


# ---------------------------------------------------------------------------
# Result DataFrame preparation
# ---------------------------------------------------------------------------

def prep_df(df, validation_type, column, test_type, primary_key, spark):
    """Standardise a results DataFrame for writing to BigQuery.

    Selects the primary key value(s) and the actual column value(s) from
    ``df``, concatenates them into single string columns, and returns a
    two-column DataFrame ready for union with the master results set.

    Args:
        df: Spark DataFrame of validation failure rows.
        validation_type (str): ``'business'`` or ``'custodial'`` (unused in
            the select but kept for traceability).
        column (str): Comma-separated column name(s) that were validated.
        test_type (str): Description of the test (e.g. ``'regex'``, ``'min'``).
        primary_key (str): Comma-separated primary key column names.
        spark: Active ``SparkSession``.

    Returns:
        Spark DataFrame with columns ``Primary_Key_Value_s_`` and
        ``Actual_Value``, or ``None`` on error.
    """
    try:
        df.createOrReplaceTempView("tmp_view")
        pk_list = primary_key.split(", ")
        col_list = column.split(", ")

        pk_concat = "".join(f" CAST({pk} AS STRING), ', ' ," for pk in pk_list)
        col_concat = "".join(f" CAST({cn} AS STRING), ', ' ," for cn in col_list)

        select_sql = (
            f"SELECT CONCAT({pk_concat[:-7]}) AS Primary_Key_Value_s_, "
            f"CONCAT({col_concat[:-7]}) AS Actual_Value "
            "FROM tmp_view"
        )
        logger.debug("prep_df SQL: %s", select_sql)
        return spark.sql(select_sql)
    except Exception as exc:
        logger.error("prep_df failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Source processors
# ---------------------------------------------------------------------------

def process_bigquery(
    pdf,
    criticality_df,
    run_id,
    spark,
    project,
    sc,
    val_st_dt,
    val_end_dt,
    cursor,
    database,
):
    """Run custodial validations against BigQuery tables.

    Args:
        pdf (pd.DataFrame): Rule-mapping DataFrame.  Must include columns:
            ``participating_table``, ``Source_Table_Date_Col``,
            ``Object_Database_Name``, ``Object_Name``, ``Validation_Period``,
            ``Object_ID``, ``Rule_ID``, ``Mapping_ID``, ``Column_Name``,
            ``Primary_Key``, ``Rule_Description``.
        criticality_df (pd.DataFrame): Criticality threshold DataFrame.
        run_id (int): Current run identifier.
        spark: Active ``SparkSession``.
        project (str): GCP project identifier.
        sc: ``SparkContext`` instance.
        val_st_dt: Validation window start datetime.
        val_end_dt: Validation window end datetime.
        cursor: Active MySQL cursor.
        database (str): MySQL metadata database name.

    Returns:
        tuple: ``(total_results, err_agg_df, log_df)`` — Spark DataFrames.
    """
    sql_context = SQLContext(spark.sparkContext)
    total_results = sql_context.createDataFrame(sc.emptyRDD(), _RESULT_SCHEMA)
    err_agg_df = sql_context.createDataFrame(sc.emptyRDD(), _ERR_AGG_SCHEMA)
    log_df = sql_context.createDataFrame(sc.emptyRDD(), _LOG_SCHEMA)

    # Build a dict of {participating_table: validation_period} and load each
    # table as a Spark temp view.
    table_period_map = {}
    source_date_col_map = {}
    src_tbl_name = None

    for idx in pdf.index:
        for tbl in pdf["participating_table"][idx].split(","):
            table_period_map[tbl] = "full table"
            source_date_col_map[tbl] = pdf["Source_Table_Date_Col"][idx]
        src_tbl_name = ".".join(
            (pdf["Object_Database_Name"][idx], pdf["Object_Name"][idx])
        )

    if src_tbl_name:
        table_period_map[src_tbl_name] = pdf["Validation_Period"][idx]

    logger.debug("Table-period map: %s", table_period_map)
    logger.debug("Source date col map: %s", source_date_col_map)

    for participating_table, validation_period in table_period_map.items():
        spark_view_name = participating_table.replace(".", "_")
        load_table(
            participating_table,
            spark_view_name,
            validation_period,
            spark,
            source_date_col_map.get(participating_table, ""),
            val_st_dt,
            val_end_dt,
        )

    today = datetime.utcnow()

    for idx in pdf.index:
        try:
            table_id = src_tbl_name.replace(".", "_") if src_tbl_name else ""
            bq_table_id = str(pdf["Object_Name"][idx])
            bq_dataset_id = str(pdf["Object_Database_Name"][idx])
            df = spark.sql(f"SELECT * FROM {table_id}")
            df_cnt = df.count()
            period = today.date()
            object_id = int(pdf["Object_ID"][idx])
            rule_id = int(pdf["Rule_ID"][idx])
            mapping_id = int(pdf["Mapping_ID"][idx])
            column_name = str(pdf["Column_Name"][idx])
            primary_key_col = str(pdf["Primary_Key"][idx])
            last_update_date = today
            object_name = pdf["Object_Name"][idx]
            rule_description = pdf["Rule_Description"][idx]

            val_schema = load_validation_schema(
                project, bq_dataset_id, bq_table_id, cursor, rule_id, database
            )
            if not val_schema:
                continue

            results = validate(
                val_schema, bq_dataset_id, table_id, df, spark, [], primary_key_col, sc
            )
            results = (
                results
                .withColumn("Run_ID", lit(run_id))
                .withColumn("Period", lit(period))
                .withColumn("Object_ID", lit(object_id))
                .withColumn("Rule_ID", lit(rule_id))
                .withColumn("Mapping_ID", lit(mapping_id))
                .withColumn("Column_Name", lit(column_name))
                .withColumn("Primary_Key_Column_s_", lit(primary_key_col))
                .withColumn("Last_Update_Date", lit(last_update_date))
            )

            failure_count = results.count()
            failure_percent = (failure_count / df_cnt) if df_cnt > 0 and failure_count > 0 else 0.0
            criticality = "null"

            cri_df = (
                criticality_df[criticality_df.Mapping_ID == mapping_id]
                .dropna()
                .sort_values(by="Failure_Threshold_Value", ascending=True)
            )
            for cri_idx in cri_df.index:
                if failure_percent >= cri_df["Failure_Threshold_Value"][cri_idx]:
                    criticality = int(cri_df["Criticality"][cri_idx])

            total_results = total_results.union(
                results
                .withColumn("Actual_Value", results.Actual_Value.cast("string"))
                .withColumn("Primary_Key_Value_s_", results.Primary_Key_Value_s_.cast("string"))
            )

            err_agg_df = err_agg_df.union(spark.sql(_ERR_AGG_SQL.format(
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
            )))

            log_df = log_df.union(spark.sql(_OBJ_LOG_SQL.format(
                object_log_id=run_id,
                period=period,
                object_id=object_id,
                object_name=object_name,
                last_update_date=last_update_date,
            )))

        except Exception as exc:
            logger.error(
                "Error during BigQuery validation for source_table=%s: %s",
                pdf.get("source_table", {}).get(idx, "unknown"),
                exc,
            )

    log_df = log_df.dropDuplicates()
    log_df = log_df.withColumn("val_st_dt", lit(val_st_dt)).withColumn(
        "val_end_dt", lit(val_end_dt)
    )
    return total_results, err_agg_df, log_df


def process_gcs(
    pdf,
    run_id,
    criticality_df,
    job_id,
    bucket,
    filepath,
    project,
    dataset_id,
    table_id,
    spark,
    val_st_dt,
    val_end_dt,
    sc,
    cursor,
    database,
):
    """Run custodial validations against a file stored in GCS.

    Supports CSV, JSON, and XML file formats.

    Args:
        pdf (pd.DataFrame): Rule-mapping DataFrame.
        run_id (int): Current run identifier.
        criticality_df (pd.DataFrame): Criticality threshold DataFrame.
        job_id (str): Job identifier (unused; kept for API compatibility).
        bucket (str): GCS bucket name.
        filepath (str): GCS object path within the bucket.
        project (str): GCP project identifier.
        dataset_id (str): Dataset identifier for schema lookups.
        table_id (str): Table identifier for schema lookups.
        spark: Active ``SparkSession``.
        val_st_dt: Validation window start datetime.
        val_end_dt: Validation window end datetime.
        sc: ``SparkContext`` instance.
        cursor: Active MySQL cursor.
        database (str): MySQL metadata database name.

    Returns:
        tuple: ``(total_results, err_agg_df, log_df)`` — Spark DataFrames.
    """
    sql_context = SQLContext(spark.sparkContext)
    total_results = sql_context.createDataFrame(sc.emptyRDD(), _RESULT_SCHEMA)
    err_agg_df = sql_context.createDataFrame(sc.emptyRDD(), _ERR_AGG_SCHEMA)
    log_df = sql_context.createDataFrame(sc.emptyRDD(), _LOG_SCHEMA)

    local_filename = f"./{filepath[filepath.rfind('/') + 1:]}"

    for idx in pdf.index:
        object_extension = pdf["Object_Extension"][idx]
        download_from_bucket(project, bucket, filepath, local_filename)
        rule_id = int(pdf["Rule_ID"][idx])
        object_name = pdf["Object_Name"][idx]
        rule_description = pdf["Rule_Description"][idx]

        val_schema = load_validation_schema(
            project, dataset_id, table_id, cursor, rule_id, database, None
        )
        logger.debug("Validation schema: %s", val_schema)
        if not val_schema:
            continue

        df = None
        if object_extension == "csv":
            try:
                with open(local_filename, "r", encoding="utf-8") as f:
                    sample = f.readline() + f.readline() + f.readline()
                has_header = csv.Sniffer().has_header(sample)
                os.remove(local_filename)
            except Exception as exc:
                logger.warning("Could not sniff CSV header: %s", exc)
                has_header = True

            if has_header:
                df = spark.read.csv(
                    f"gs://{bucket}/{filepath}",
                    inferSchema=True,
                    header=True,
                    enforceSchema=False,
                    escape='"',
                    quote='"',
                )
            else:
                col_names = list(val_schema.keys())
                df_raw = spark.read.csv(
                    f"gs://{bucket}/{filepath}",
                    inferSchema=True,
                    header=False,
                    enforceSchema=False,
                    escape='"',
                    quote='"',
                )
                df = df_raw.toDF(*col_names)

        elif object_extension == "json":
            df = spark.read.json(local_filename)

        elif object_extension == "xml":
            df = spark.read.format("com.databricks.spark.xml").load(local_filename)

        if df is None:
            logger.warning("Unsupported object_extension=%s — skipping.", object_extension)
            continue

        schema_col_count = len(list(val_schema.keys()))
        if len(df.schema) != schema_col_count:
            logger.warning(
                "Column count mismatch for object_id=%s: "
                "data has %d cols, schema expects %d cols.",
                pdf["Object_ID"][idx],
                len(df.schema),
                schema_col_count,
            )

        df_cnt = df.count()
        today = datetime.utcnow()
        primary_key_col = str(pdf["Primary_Key"][idx])
        period = today.date()
        object_id = int(pdf["Object_ID"][idx])
        rule_id = int(pdf["Rule_ID"][idx])
        mapping_id = int(pdf["Mapping_ID"][idx])
        column_name = str(pdf["Column_Name"][idx])
        object_name = pdf["Object_Name"][idx]
        last_update_date = today

        results = validate(val_schema, dataset_id, table_id, df, spark, [], primary_key_col, sc)
        failure_count = results.count()

        results = (
            results
            .withColumn("Run_ID", lit(run_id))
            .withColumn("Period", lit(period))
            .withColumn("Object_ID", lit(object_id))
            .withColumn("Rule_ID", lit(rule_id))
            .withColumn("Mapping_ID", lit(mapping_id))
            .withColumn("Column_Name", lit(column_name))
            .withColumn("Primary_Key_Column_s_", lit(primary_key_col))
            .withColumn("Last_Update_Date", lit(last_update_date))
            .withColumn("Actual_Value", col("Actual_Value").cast("string"))
            .withColumn("Primary_Key_Value_s_", col("Primary_Key_Value_s_").cast("string"))
        )

        criticality = "null"
        failure_percent = 0.0

        if failure_count > 0 and df_cnt > 0:
            failure_percent = failure_count / df_cnt
            cri_df = (
                criticality_df[criticality_df.Mapping_ID == mapping_id]
                .dropna()
                .sort_values(by="Failure_Threshold_Value", ascending=True)
            )
            for cri_idx in cri_df.index:
                if failure_percent >= cri_df["Failure_Threshold_Value"][cri_idx]:
                    criticality = int(cri_df["Criticality"][cri_idx])
        elif failure_count == 0 and df_cnt > 0:
            logger.info(
                "New records arrived but no validation errors for rule_id=%s.", rule_id
            )

        if df_cnt > 0:
            err_agg_df = err_agg_df.union(spark.sql(_ERR_AGG_SQL.format(
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
            )))

        log_df = log_df.union(spark.sql(_OBJ_LOG_SQL.format(
            object_log_id=run_id,
            period=period,
            object_id=object_id,
            object_name=object_name,
            last_update_date=last_update_date,
        )))
        total_results = total_results.union(results)

    log_df = log_df.dropDuplicates()
    log_df = log_df.withColumn("val_st_dt", lit(val_st_dt)).withColumn(
        "val_end_dt", lit(val_end_dt)
    )
    return total_results, err_agg_df, log_df


def process_cloudsql(
    pdf,
    job_id,
    criticality_df,
    host,
    dataset_id,
    user,
    password,
    project,
    spark,
    run_id,
    val_st_dt,
    val_end_dt,
    cursor,
    sc,
    database,
):
    """Run custodial validations against Cloud SQL (MySQL) tables.

    Args:
        pdf (pd.DataFrame): Rule-mapping DataFrame.
        job_id (str): Job identifier (unused; kept for API compatibility).
        criticality_df (pd.DataFrame): Criticality threshold DataFrame.
        host (str): Cloud SQL host IP or hostname.
        dataset_id (str): Default dataset/schema identifier.
        user (str): MySQL username.
        password (str): MySQL password.
        project (str): GCP project identifier.
        spark: Active ``SparkSession``.
        run_id (int): Current run identifier.
        val_st_dt: Validation window start datetime.
        val_end_dt: Validation window end datetime.
        cursor: Active MySQL cursor.
        sc: ``SparkContext`` instance.
        database (str): MySQL metadata database name.

    Returns:
        tuple: ``(total_results, err_agg_df, log_df)`` — Spark DataFrames.
    """
    sql_context = SQLContext(spark.sparkContext)
    total_results = sql_context.createDataFrame(sc.emptyRDD(), _RESULT_SCHEMA)
    err_agg_df = sql_context.createDataFrame(sc.emptyRDD(), _ERR_AGG_SCHEMA)
    log_df = sql_context.createDataFrame(sc.emptyRDD(), _LOG_SCHEMA)

    for idx in pdf.index:
        rule_description = pdf["Rule_Description"][idx]
        current_dataset_id = str(pdf["Object_Database_Name"][idx])
        table_id = str(pdf["Object_Name"][idx])
        source_table_date_col = str(pdf["Source_Table_Date_Col"][idx])
        validation_period = pdf["Validation_Period"][idx]
        rule_id = int(pdf["Rule_ID"][idx])

        val_schema = load_validation_schema(
            project, current_dataset_id, table_id, cursor, rule_id, database, None
        )
        logger.debug("Validation schema: %s", val_schema)
        if not val_schema:
            continue

        df = load_data_from_cloudsql(
            str(pdf["source_table"][idx]),
            current_dataset_id,
            host,
            user,
            password,
            validation_period,
            source_table_date_col,
            val_st_dt,
            val_end_dt,
            spark,
        )
        df_cnt = df.count()
        logger.debug("df_cnt=%s", df_cnt)

        today = datetime.utcnow()
        primary_key_col = str(pdf["Primary_Key"][idx])
        period = today.date()
        object_id = int(pdf["Object_ID"][idx])
        rule_id = int(pdf["Rule_ID"][idx])
        mapping_id = int(pdf["Mapping_ID"][idx])
        column_name = str(pdf["Column_Name"][idx])
        object_name = pdf["Object_Name"][idx]
        last_update_date = today

        results = validate(
            val_schema, current_dataset_id, table_id, df, spark, [], primary_key_col, sc
        )
        failure_count = results.count()

        results = (
            results
            .withColumn("Run_ID", lit(run_id))
            .withColumn("Period", lit(period))
            .withColumn("Object_ID", lit(object_id))
            .withColumn("Rule_ID", lit(rule_id))
            .withColumn("Mapping_ID", lit(mapping_id))
            .withColumn("Column_Name", lit(column_name))
            .withColumn("Primary_Key_Column_s_", lit(primary_key_col))
            .withColumn("Last_Update_Date", lit(last_update_date))
            .withColumn("Actual_Value", col("Actual_Value").cast("string"))
            .withColumn("Primary_Key_Value_s_", col("Primary_Key_Value_s_").cast("string"))
        )

        criticality = "null"
        failure_percent = 0.0

        if df_cnt > 0 and failure_count > 0:
            failure_percent = failure_count / df_cnt

        cri_df = (
            criticality_df[criticality_df.Mapping_ID == mapping_id]
            .dropna()
            .sort_values(by="Failure_Threshold_Value", ascending=True)
        )
        for cri_idx in criticality_df.index:
            if failure_percent >= criticality_df["Failure_Threshold_Value"][cri_idx]:
                criticality = int(criticality_df["Criticality"][cri_idx])

        err_agg_df = err_agg_df.union(spark.sql(_ERR_AGG_SQL.format(
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
        )))

        log_df = log_df.union(spark.sql(_OBJ_LOG_SQL.format(
            object_log_id=run_id,
            period=period,
            object_id=object_id,
            object_name=object_name,
            last_update_date=last_update_date,
        )))
        total_results = total_results.union(results)

    log_df = log_df.dropDuplicates()
    log_df = log_df.withColumn("val_st_dt", lit(val_st_dt)).withColumn(
        "val_end_dt", lit(val_end_dt)
    )
    return total_results, err_agg_df, log_df


# ---------------------------------------------------------------------------
# GCS download helper
# ---------------------------------------------------------------------------

def download_from_bucket(project, bucket, remote_path, local_path):
    """Download a file from GCS to the local filesystem.

    Args:
        project (str): GCP project identifier.
        bucket (str): GCS bucket name.
        remote_path (str): Object path within the bucket.
        local_path (str): Local filesystem path to write the file to.
    """
    gcs_client = storage.Client(project)
    bucket_obj = gcs_client.get_bucket(bucket)
    logger.debug("Downloading gs://%s/%s → %s", bucket, remote_path, local_path)
    blob = bucket_obj.blob(remote_path)
    blob.download_to_filename(local_path)