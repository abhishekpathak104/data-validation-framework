"""
data_validation/rule_intelligence/review_yaml.py
----------------------------------------------------
Reads and writes the human-reviewed YAML rule file. Uses plain PyYAML (no
comment round-tripping) — the LLM's rationale for each rule is a normal YAML
field, not a comment, so nothing is lost by reserializing/reparsing it.
"""

import yaml

from data_validation.rule_intelligence.schema import (
    CandidateRule,
    ReviewedBatch,
    RuleBatch,
)


def _rule_logic_mapping(rule: CandidateRule) -> dict:
    """Merge a rule's custodial_constraints into a single Rule_Logic-shaped dict."""
    merged = {}
    for constraint in rule.custodial_constraints or []:
        merged[constraint.rule_type.value] = constraint.value
    return merged


def _rule_to_yaml_dict(rule: CandidateRule, include: bool = True) -> dict:
    entry = {
        "rule_id_temp": rule.rule_id_temp,
        "include": include,
        "test_type": rule.test_type.value,
        "column_name": rule.column_name,
        "rule_description": rule.rule_description,
        "confidence": rule.confidence,
        "rationale": rule.rationale,
    }
    if rule.primary_key_suggestion:
        entry["primary_key_suggestion"] = rule.primary_key_suggestion
    if rule.test_type.value == "Business":
        entry["rule_logic_sql"] = rule.business_sql
    else:
        entry["rule_logic"] = _rule_logic_mapping(rule)
    return entry


def write_review_yaml(
    batch: RuleBatch,
    out_path: str,
    object_name: str,
    object_database_name: str,
    primary_key: str | None = None,
) -> None:
    """Write a RuleBatch to a human-editable YAML review file.

    Args:
        batch (RuleBatch): The LLM-generated candidate rules.
        out_path (str): Path to write the YAML review file to.
        object_name (str): Object name to record (CLI arg overrides the LLM's
            ``object_name_suggestion`` when resolving this upstream).
        object_database_name (str): Dataset/schema/bucket name for the object.
        primary_key (str | None): Primary key column(s); falls back to the
            first rule's ``primary_key_suggestion`` if not given.
    """
    if primary_key is None:
        primary_key = next(
            (r.primary_key_suggestion for r in batch.rules if r.primary_key_suggestion),
            "",
        )

    document = {
        "object": {
            "object_name": object_name,
            "object_database_name": object_database_name,
            "primary_key": primary_key,
        },
        "rules": [_rule_to_yaml_dict(rule) for rule in batch.rules],
    }

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(
            "# Review each rule below: edit `include`, `rule_description`, `rule_logic`,\n"
            "# or delete a rule entirely. Rules with `include: false` are skipped by\n"
            "# `dv-rules-apply`.\n"
        )
        yaml.safe_dump(document, f, sort_keys=False, default_flow_style=False, width=100)


def read_review_yaml(path: str) -> ReviewedBatch:
    """Read and re-validate a human-reviewed YAML rule file.

    Re-validating through the same Pydantic models used for the LLM's
    structured output means a human typo in ``rule_logic`` (e.g. a
    ``custom_spark`` key) fails loudly here, before any database write.

    Args:
        path (str): Path to the reviewed YAML file.

    Returns:
        ReviewedBatch: The validated, reviewed batch.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    rules = []
    for entry in raw.get("rules", []):
        entry = dict(entry)
        test_type = entry.pop("test_type")
        rule_kwargs = {
            "rule_id_temp": entry["rule_id_temp"],
            "include": entry.get("include", True),
            "test_type": test_type,
            "column_name": entry["column_name"],
            "rule_description": entry["rule_description"],
            "confidence": entry["confidence"],
            "rationale": entry.get("rationale", ""),
            "primary_key_suggestion": entry.get("primary_key_suggestion"),
        }
        if test_type == "Business":
            rule_kwargs["business_sql"] = entry.get("rule_logic_sql")
        else:
            rule_logic = entry.get("rule_logic") or {}
            rule_kwargs["custodial_constraints"] = [
                {"rule_type": key, "value": value} for key, value in rule_logic.items()
            ]
        rules.append(rule_kwargs)

    return ReviewedBatch(object=raw["object"], rules=rules)
