"""
data_validation/rule_intelligence/cli.py
---------------------------------------------
Two CLI entry points, run manually by a human:

    dv-rules-generate  - profile sample data (+ optional contract doc), call
                         Gemini, write a YAML file of candidate rules.
    dv-rules-apply     - read a human-reviewed YAML file and write the
                         approved rules into the MySQL metadata tables.
"""

import argparse
import logging
import os
import sys

from tabulate import tabulate

from data_validation.rule_intelligence import db_writer
from data_validation.rule_intelligence.config import load_db_config, resolve_db_password
from data_validation.rule_intelligence.contract import load_contract
from data_validation.rule_intelligence.llm import DEFAULT_MODEL, build_llm, generate_candidate_rules
from data_validation.rule_intelligence.profiling import profile_file
from data_validation.rule_intelligence.review_yaml import read_review_yaml, write_review_yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _build_generate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dv-rules-generate",
        description="Profile sample data and/or a data contract, and ask Gemini to "
        "propose candidate validation rules for human review.",
    )
    parser.add_argument("--sample-file", required=True, help="Path to a CSV/JSON sample data file.")
    parser.add_argument("--contract", default=None, help="Path to a freeform data-contract document.")
    parser.add_argument("--object-name", required=True, help="Object/table/file name to register.")
    parser.add_argument(
        "--object-database-name", required=True, help="Dataset, schema, or bucket path for the object."
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Gemini model name.")
    parser.add_argument(
        "--out", default="review_rules.yaml", help="Output path for the review YAML file."
    )
    parser.add_argument(
        "--max-sample-values", type=int, default=5, help="Max distinct sample values per column."
    )
    return parser


def generate_main(argv: list[str] | None = None) -> int:
    """CLI entry point: profile sample data + contract, generate candidate rules.

    Args:
        argv (list[str] | None): Argument list (defaults to ``sys.argv[1:]``).

    Returns:
        int: Process exit code.
    """
    args = _build_generate_parser().parse_args(argv)

    profile = profile_file(args.sample_file, max_sample_values=args.max_sample_values)
    contract_text = load_contract(args.contract)

    llm = build_llm(model=args.model)
    batch = generate_candidate_rules(
        profile, contract_text, object_name_hint=args.object_name, llm=llm
    )

    write_review_yaml(
        batch,
        out_path=args.out,
        object_name=args.object_name,
        object_database_name=args.object_database_name,
    )

    avg_confidence = (
        sum(r.confidence for r in batch.rules) / len(batch.rules) if batch.rules else 0.0
    )
    print(f"Proposed {len(batch.rules)} candidate rule(s), avg confidence {avg_confidence:.2f}.")
    print(f"Review file written to: {args.out}")
    print("Edit `include`/`rule_logic`/`rule_description` as needed, then run dv-rules-apply.")
    return 0


def _build_apply_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dv-rules-apply",
        description="Write a human-reviewed batch of rules into the MySQL metadata tables.",
    )
    parser.add_argument("--review-file", required=True, help="Path to the reviewed YAML file.")
    parser.add_argument("--conf", default=None, help="Path to a data_validation.conf-style config file.")
    parser.add_argument("--host", default=None, help="MySQL host (overrides --conf).")
    parser.add_argument("--user", default=None, help="MySQL user (overrides --conf).")
    parser.add_argument("--database", default=None, help="MySQL database (overrides --conf).")
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and print planned inserts without writing."
    )
    parser.add_argument(
        "--skip-thresholds", action="store_true", help="Do not insert default threshold rows."
    )
    parser.add_argument(
        "--force", action="store_true", help="Insert rule mappings even if an equivalent one exists."
    )
    return parser


def apply_main(argv: list[str] | None = None) -> int:
    """CLI entry point: apply a human-reviewed YAML rule file to MySQL.

    Args:
        argv (list[str] | None): Argument list (defaults to ``sys.argv[1:]``).

    Returns:
        int: Process exit code.
    """
    args = _build_apply_parser().parse_args(argv)
    reviewed = read_review_yaml(args.review_file)

    included = [r for r in reviewed.rules if r.include]
    print(f"Object: {reviewed.object.object_name} ({reviewed.object.object_database_name})")
    print(
        tabulate(
            [
                [r.rule_id_temp, r.test_type.value, r.column_name, r.rule_description]
                for r in included
            ],
            headers=["id", "type", "column(s)", "description"],
        )
    )
    print(f"{len(included)} of {len(reviewed.rules)} rule(s) marked include: true.")

    if args.dry_run:
        print("Dry run — no database connection opened, nothing written.")
        return 0

    host = args.host
    user = args.user
    database = args.database
    password = None

    if args.conf:
        config = load_db_config(args.conf)
        host = host or config.get("hostip")
        user = user or config.get("user")
        database = database or config.get("database")
        password = resolve_db_password(config)
    else:
        password = os.environ.get("DB_PASSWORD")

    if not all([host, user, database, password]):
        logger.error(
            "Missing MySQL connection details. Provide --conf, or all of "
            "--host/--user/--database plus a DB_PASSWORD env var."
        )
        return 1

    conn = db_writer.get_connection(host, database, user, password)
    try:
        report = db_writer.apply_reviewed_batch(
            conn, reviewed, skip_thresholds=args.skip_thresholds, force=args.force
        )
    finally:
        conn.close()

    print(f"Object_ID: {report.object_id}")
    print(f"Created {len(report.created_rule_ids)} rule(s), {len(report.created_mapping_ids)} mapping(s).")
    if report.skipped_duplicates:
        print(f"Skipped {len(report.skipped_duplicates)} duplicate(s): {report.skipped_duplicates}")
    return 0


if __name__ == "__main__":
    sys.exit(generate_main())
