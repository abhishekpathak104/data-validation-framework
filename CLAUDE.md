# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A GCP-native data validation framework that runs as a PySpark job on Dataproc. It reads validation
rules from a MySQL metadata database, applies them to data in BigQuery / Cloud SQL / GCS, and writes
failures (detailed rows + aggregated stats + execution log) to BigQuery, alerting via SendGrid email
when failure rates cross configured criticality thresholds.

There is no local dev/test/CI setup — `pyspark`, `mysql-connector-python`, and `google-cloud-*` are
runtime dependencies of a Dataproc cluster. `tests/` is currently just a placeholder package with no
actual tests. There is no linter or formatter configured (no ruff/flake8/pylint config, no Makefile).

## Commands

```bash
# Install package + deps locally (for editing/import resolution only — Spark itself comes from Dataproc)
pip install -e .

# Package the framework for Dataproc job submission
zip -r data_validation.zip data_validation/
gsutil cp data_validation.zip gs://<your-bucket>/validation_framework/scripts/data_validation.zip
gsutil cp data_validation/main.py gs://<your-bucket>/validation_framework/scripts/main.py

# Submit a Dataproc job directly (job_type: gcs | cloudsql | bigquery — see arg reference in README)
gcloud dataproc jobs submit pyspark \
    gs://<your-bucket>/validation_framework/scripts/main.py \
    --cluster=data-validation --region=<region> \
    --py-files=gs://<your-bucket>/validation_framework/scripts/data_validation.zip \
    --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar \
    -- <job_type> <object_id> <job_id> [<extra_args>...]

# Deploy the GCS-trigger Cloud Function
gcloud functions deploy hello_gcs \
    --runtime python310 --trigger-resource <bucket> \
    --trigger-event google.storage.object.finalize \
    --source cloud_functions/gcs_trigger/ --entry-point hello_gcs \
    --set-env-vars GCP_PROJECT=<gcp_project_id>

# Bootstrap the MySQL metadata schema (run in order)
mysql -h $HOST -u root -p < sql/ddl/00_create_database.sql
mysql -h $HOST -u root -p Data_Validation < sql/ddl/01_create_tables.sql
mysql -h $HOST -u root -p Data_Validation < sql/dml/01_seed_rules.sql   # + 02..05, optional sample data

# AI-assisted rule generation (local CLI, optional — see rule_intelligence architecture note below)
pip install -e ".[rule-intelligence]"
export GOOGLE_API_KEY=<gemini_api_key>
dv-rules-generate --sample-file samples/accounts.csv --object-name account \
    --object-database-name salesforce --out review/account_rules.yaml
dv-rules-apply --review-file review/account_rules.yaml --conf config/data_validation.conf
```

There is no single-test-runner command since no tests exist yet. If you add tests, they should be
runnable with plain `pytest tests/` and should mock/avoid needing a live Spark cluster, MySQL, or
BigQuery connection (per the README's contributing guidance).

## Architecture

```
GCS file drop → Cloud Function (cloud_functions/gcs_trigger/main.py)
                     → submits a Dataproc PySpark job
                          → data_validation/main.py (orchestrator)
                               ├─ connectors/source_extract.py   (load BQ / Cloud SQL / GCS; write results)
                               ├─ validators/business.py         (SQL push-down rule engine)
                               ├─ validators/custodial.py        (schema / data-quality tests)
                               └─ notifications/mailer.py        (SendGrid alerts)
                                    → BigQuery: detailed_errors, aggregated_errors, object_log, execution_status
```

**Job types** (first CLI arg to `main.py`, dispatched in `main()`):
- `gcs` — validates a single uploaded file (CSV/JSON/XML), triggered by the Cloud Function.
- `cloudsql` — validates a Cloud SQL (MySQL) table; custodial rules only.
- `bigquery` — validates a BigQuery table; runs both business and custodial rules.

Each job type has its own `_process_*_job` function in `main.py` that: queries the MySQL metadata
tables for active rule mappings for the object, computes the validation date window via
`calculate_validation_date` (tracks prev/curr/next windows and `Val_Status` in the
`validation_execution` BigQuery table to support incremental/delta validation), dispatches to the
relevant validator module, and writes the three result DataFrames via
`connectors.source_extract.write_results` (Spark DF → GCS JSON staging → `bq load` → cleanup).

**Metadata schema** (MySQL, see `sql/ddl/01_create_tables.sql`) — 8 tables in dependency order:
`data_validation_rule` → `data_validation_object_lookup` → `data_validation_rule_mapping` →
`data_validation_rule_threshold`, plus a parallel notification chain:
`data_validation_notification_message_handler` + `_distribution_list` → `data_validation_notification`
→ `data_validation_object_notification`. A rule's `Rule_Logic` column holds either a JSON constraint
dict (custodial, e.g. `{"is_nullable": "NO", "regex": "..."}`) or a raw SQL string (business rules,
pushed straight to BigQuery and counted).

**Two validator modules, two different execution strategies:**
- `validators/business.py` — treats `Rule_Logic` as a SQL fragment, wraps it, inserts matching rows
  directly into the BigQuery error table (`_validate_push_down`), then counts them back. No data ever
  passes through Spark for business rules — it's pure BQ push-down.
- `validators/custodial.py` — loads data into Spark, merges MySQL rule metadata with BigQuery
  `INFORMATION_SCHEMA.COLUMNS` (`load_validation_schema`), then runs per-column test functions
  (`test_nulls`, `test_distinct`, `test_range`, `test_membership`, `test_exclusion`, `test_regex`,
  `test_type`) dispatched from `validate()`. Supports a `custom_sql` test key (arbitrary Spark SQL)
  and a `custom_spark` test key that `exec()`s arbitrary Python from `Rule_Logic` — treat any code
  touching this path as executing untrusted input if rule metadata isn't tightly access-controlled.

**Criticality/thresholds**: failure_percent = failures / total rows is compared against
`data_validation_rule_threshold.Failure_Threshold_Value` rows (sorted ascending) for the rule's
`Mapping_ID`; the highest tier whose threshold is exceeded wins. `notifications/mailer.py` then looks
up the notification handler text + distribution list for that `(Object_ID, Criticality)` pair and
emails a CSV-attached summary via SendGrid SMTP if `SENDGRID_API_KEY` is set.

**Configuration**: copy `config/data_validation.conf.example` to `config/data_validation.conf`
(git-ignored), fill in real values, and upload it to GCS. The job downloads it at startup from the
path in the `CONF_FILE_GCS` env var. The `DB_PASSWORD` env var overrides the `pswd` key at runtime —
prefer that (or Secret Manager) over putting a real password in the conf file.

**`custom_spark` rules are disabled by default** (`validators/custodial.py`) because they `exec()`
arbitrary Python sourced from the `Rule_Logic` column. Set `ALLOW_CUSTOM_SPARK_EXEC=1` only if the
MySQL metadata database is trusted/access-controlled; prefer `custom_sql` otherwise.

**`rule_intelligence/` — LLM-assisted rule authoring** is a separate, optional, locally-run CLI tool
(installed via the `rule-intelligence` extra) with zero import-time dependency on pyspark/BigQuery —
it never runs on Dataproc. Two commands: `dv-rules-generate` profiles a sample data file (pandas) and
an optional freeform data-contract document, sends both to Gemini via `langchain-google-genai`
(Google AI Studio `GOOGLE_API_KEY`, not Vertex AI) using `.with_structured_output()` against the
Pydantic schema in `rule_intelligence/schema.py`, and writes a YAML file of candidate rules for a
human to review by hand (toggle `include`, edit `rule_logic`). `dv-rules-apply` then reads the
reviewed YAML and writes into the metadata tables in FK order — `object_lookup` → `rule` →
`rule_mapping` → `rule_threshold` — inside one transaction, using the same `%s`-parameterized-query
convention as `main.py`, with duplicate-mapping detection so re-running `apply` is safe. The
`CustodialRuleType` enum in `schema.py` has no `custom_spark` member, so the LLM cannot emit an
exec()-based rule under any circumstances, including prompt injection via a contract document.
