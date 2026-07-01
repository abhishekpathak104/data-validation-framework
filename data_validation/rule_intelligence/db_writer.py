"""
data_validation/rule_intelligence/db_writer.py
--------------------------------------------------
Writes a human-reviewed batch of rules into the MySQL metadata tables:
data_validation_object_lookup -> data_validation_rule ->
data_validation_rule_mapping -> data_validation_rule_threshold (in that
order, respecting foreign keys).

Duplicates the tiny MySQL connection helper from ``data_validation.main``
rather than importing it, so this subpackage has zero import-time dependency
on pyspark. Uses ``%s`` parameterized queries throughout, matching the
SQL-injection hardening already applied to ``data_validation/main.py``.
"""

import json
import logging
from dataclasses import dataclass, field

import mysql.connector

from data_validation.rule_intelligence.schema import ReviewedBatch, ReviewedRule

logger = logging.getLogger(__name__)

# Rule types where any failure at all should be treated as high severity by default.
_HARD_CONSTRAINT_TYPES = {"is_nullable", "distinct", "data_type"}


def get_connection(host: str, database: str, user: str, password: str):
    """Open a MySQL connection. Identical pattern to data_validation.main.get_connection.

    Args:
        host (str): MySQL host IP or hostname.
        database (str): MySQL database/schema name.
        user (str): MySQL username.
        password (str): MySQL password.

    Returns:
        mysql.connector.connection.MySQLConnection: An open connection.

    Raises:
        mysql.connector.Error: If the connection attempt fails.
    """
    return mysql.connector.connect(
        host=host, database=database, user=user, password=password
    )


@dataclass
class ApplyReport:
    object_id: int
    created_rule_ids: list[int] = field(default_factory=list)
    created_mapping_ids: list[int] = field(default_factory=list)
    skipped_duplicates: list[str] = field(default_factory=list)


def _rule_logic_payload(rule: ReviewedRule) -> str:
    """Render a rule's Rule_Logic column content (JSON dict or raw SQL)."""
    if rule.test_type.value == "Business":
        return rule.business_sql
    merged = {c.rule_type.value: c.value for c in (rule.custodial_constraints or [])}
    return json.dumps(merged, sort_keys=True)


def get_or_create_object(
    cursor,
    object_name: str,
    object_database_name: str,
    primary_key: str,
    object_extension: str | None = None,
    validation_period: str = "full table",
) -> int:
    """Look up (or create) a data_validation_object_lookup row.

    If the object already exists, its stored Primary_Key is never overwritten
    — a mismatch is logged as a warning instead, since silently changing it
    could break other active rule mappings on that object.

    Args:
        cursor: Active MySQL cursor.
        object_name (str): Object/table/file name.
        object_database_name (str): Dataset, schema, or bucket path.
        primary_key (str): Comma-separated primary key column name(s).
        object_extension (str | None): File extension for GCS objects.
        validation_period (str): One of 'full table', 'daily', 'monthly', 'yearly'.

    Returns:
        int: The Object_ID (existing or newly created).
    """
    cursor.execute(
        "SELECT Object_ID, Primary_Key FROM data_validation_object_lookup "
        "WHERE Object_Name = %s AND Object_Database_Name = %s LIMIT 1",
        (object_name, object_database_name),
    )
    row = cursor.fetchone()
    if row is not None:
        object_id, existing_primary_key = row
        if primary_key and existing_primary_key and primary_key != existing_primary_key:
            logger.warning(
                "Object '%s' (%s) already exists with Primary_Key='%s'; ignoring "
                "proposed Primary_Key='%s' from the review file.",
                object_name,
                object_database_name,
                existing_primary_key,
                primary_key,
            )
        return object_id

    cursor.execute(
        "INSERT INTO data_validation_object_lookup "
        "(Object_Name, Object_Database_Name, Object_Extension, Primary_Key, "
        "Validation_Period, Active) VALUES (%s, %s, %s, %s, %s, 1)",
        (object_name, object_database_name, object_extension, primary_key, validation_period),
    )
    return cursor.lastrowid


def insert_rule(cursor, rule_description: str, rule_logic: str, test_type: str) -> int:
    """Insert a new data_validation_rule row.

    Args:
        cursor: Active MySQL cursor.
        rule_description (str): Human-readable rule description.
        rule_logic (str): JSON constraint dict (custodial) or raw SQL (business).
        test_type (str): 'Custodial' or 'Business'.

    Returns:
        int: The new Rule_ID.
    """
    cursor.execute(
        "INSERT INTO data_validation_rule (Rule_Description, Rule_Logic, Test_Type) "
        "VALUES (%s, %s, %s)",
        (rule_description, rule_logic, test_type),
    )
    return cursor.lastrowid


def find_existing_mapping(
    cursor, object_id: int, column_name: str, rule_logic: str, test_type: str
) -> int | None:
    """Check whether an equivalent rule mapping already exists for this object.

    Used to avoid creating duplicate data_validation_rule_mapping rows when a
    review file is applied more than once.

    Args:
        cursor: Active MySQL cursor.
        object_id (int): Object identifier.
        column_name (str): Comma-separated target column name(s).
        rule_logic (str): Rendered Rule_Logic content (JSON or SQL) to match.
        test_type (str): 'Custodial' or 'Business'.

    Returns:
        int | None: The existing Mapping_ID, or None if no match is found.
    """
    cursor.execute(
        "SELECT m.Mapping_ID FROM data_validation_rule_mapping m "
        "JOIN data_validation_rule r ON m.Rule_ID = r.Rule_ID "
        "WHERE m.Object_ID = %s AND m.Column_Name = %s "
        "AND r.Rule_Logic = %s AND r.Test_Type = %s LIMIT 1",
        (object_id, column_name, rule_logic, test_type),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def insert_rule_mapping(
    cursor,
    object_id: int,
    rule_id: int,
    column_name: str,
    test_type: str,
) -> int:
    """Insert a new data_validation_rule_mapping row.

    Args:
        cursor: Active MySQL cursor.
        object_id (int): Object identifier.
        rule_id (int): Rule identifier.
        column_name (str): Comma-separated target column name(s).
        test_type (str): 'Custodial' or 'Business'.

    Returns:
        int: The new Mapping_ID.
    """
    cursor.execute(
        "INSERT INTO data_validation_rule_mapping "
        "(Object_ID, Rule_ID, Column_Name, Test_Type, Active) VALUES (%s, %s, %s, %s, 1)",
        (object_id, rule_id, column_name, test_type),
    )
    return cursor.lastrowid


def insert_default_thresholds(cursor, mapping_id: int, rule: ReviewedRule) -> None:
    """Insert a sane default criticality threshold for a new rule mapping.

    Hard constraints (is_nullable/distinct/data_type) default to any failure
    being High severity; everything else defaults to a 1% failure rate being
    Medium severity. These are starting points only — humans can add
    additional tiers directly in MySQL afterwards.

    Args:
        cursor: Active MySQL cursor.
        mapping_id (int): The rule mapping to attach a threshold to.
        rule (ReviewedRule): The rule, used to pick hard-vs-soft defaults.
    """
    is_hard = rule.test_type.value == "Custodial" and any(
        c.rule_type.value in _HARD_CONSTRAINT_TYPES for c in (rule.custodial_constraints or [])
    )
    failure_threshold, criticality = (0.0, 3) if is_hard else (0.01, 2)
    cursor.execute(
        "INSERT INTO data_validation_rule_threshold "
        "(Mapping_ID, Failure_Threshold_Value, Criticality, Active) VALUES (%s, %s, %s, 1)",
        (mapping_id, failure_threshold, criticality),
    )


def apply_reviewed_batch(
    conn,
    reviewed: ReviewedBatch,
    skip_thresholds: bool = False,
    force: bool = False,
) -> ApplyReport:
    """Write a human-reviewed batch of rules into the MySQL metadata tables.

    Runs as a single transaction: commits only if every included rule is
    written successfully, rolls back and re-raises on any error.

    Args:
        conn: Open MySQL connection (``autocommit`` will be disabled).
        reviewed (ReviewedBatch): The human-reviewed rules to apply.
        skip_thresholds (bool): If True, do not insert default threshold rows.
        force (bool): If True, insert rule mappings even if an equivalent one
            already exists (bypasses duplicate detection).

    Returns:
        ApplyReport: Summary of what was created/skipped.
    """
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        object_id = get_or_create_object(
            cursor,
            reviewed.object.object_name,
            reviewed.object.object_database_name,
            reviewed.object.primary_key,
        )
        report = ApplyReport(object_id=object_id)

        for rule in reviewed.rules:
            if not rule.include:
                continue

            rule_logic = _rule_logic_payload(rule)

            if not force:
                existing_mapping_id = find_existing_mapping(
                    cursor, object_id, rule.column_name, rule_logic, rule.test_type.value
                )
                if existing_mapping_id is not None:
                    logger.info(
                        "Skipping rule '%s' (column=%s): equivalent mapping %s already exists.",
                        rule.rule_id_temp,
                        rule.column_name,
                        existing_mapping_id,
                    )
                    report.skipped_duplicates.append(rule.rule_id_temp)
                    continue

            rule_id = insert_rule(cursor, rule.rule_description, rule_logic, rule.test_type.value)
            mapping_id = insert_rule_mapping(
                cursor, object_id, rule_id, rule.column_name, rule.test_type.value
            )
            if not skip_thresholds:
                insert_default_thresholds(cursor, mapping_id, rule)

            report.created_rule_ids.append(rule_id)
            report.created_mapping_ids.append(mapping_id)

        conn.commit()
        return report

    except Exception:
        conn.rollback()
        raise
