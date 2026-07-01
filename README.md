# Data Validation Framework

A GCP-native data validation framework built on **Apache Spark (Dataproc)** and **BigQuery**. It enforces configurable business and custodial rules against data stored in BigQuery tables, Cloud SQL (MySQL), and GCS files, then reports failures with criticality scores and email notifications.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Environment Variables](#environment-variables)
- [Database Setup](#database-setup)
- [Deployment](#deployment)
- [Running a Validation Job](#running-a-validation-job)
- [Validation Rule Types](#validation-rule-types)
- [Output Tables](#output-tables)
- [Notification Setup](#notification-setup)
- [AI-Assisted Rule Generation](#ai-assisted-rule-generation)
- [Contributing](#contributing)

---

## Overview

The framework reads validation rules from a MySQL metadata database, applies them to source data using Spark, and writes three categories of output to BigQuery:

| Output | Description |
|---|---|
| **Detailed errors** | One row per failing record, with primary key and actual value |
| **Aggregated errors** | Failure count, failure %, and criticality per rule per run |
| **Object log** | Execution log entry per validated object per run |

Failures above a configured threshold trigger email alerts via SendGrid.

---

## Architecture

```
                     ┌─────────────────────────────────────────┐
                     │         GCS File Drop (event)            │
                     └───────────────┬─────────────────────────┘
                                     │ triggers
                                     ▼
                     ┌─────────────────────────────────────────┐
                     │   Cloud Function  (gcs_trigger/main.py)  │
                     └───────────────┬─────────────────────────┘
                                     │ submits Dataproc PySpark job
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        Dataproc Cluster                                │
│                                                                        │
│  data_validation/main.py  (orchestrator)                               │
│       │                                                                │
│       ├── connectors/source_extract.py   ← load from BQ / SQL / GCS   │
│       ├── validators/business.py         ← SQL push-down rule engine   │
│       ├── validators/custodial.py        ← schema / data-quality tests │
│       └── notifications/mailer.py        ← SendGrid email alerts       │
│                                                                        │
└───────────────────────────┬────────────────────────────────────────────┘
                            │ writes results
                            ▼
              ┌─────────────────────────┐
              │         BigQuery        │
              │  • detailed_errors      │
              │  • aggregated_errors    │
              │  • object_log           │
              │  • execution_status     │
              └─────────────────────────┘
```

**Supported data sources**

| Source | Job type arg | Trigger |
|---|---|---|
| GCS file (CSV / JSON / XML) | `gcs` | Cloud Function on file upload |
| Cloud SQL (MySQL) | `cloudsql` | Manual / scheduled |
| BigQuery table | `bigquery` | Manual / scheduled |

---

## Project Structure

```
data-validation-framework/
│
├── README.md
├── requirements.txt                          # Dataproc runtime dependencies
├── pyproject.toml                            # Package metadata
│
├── config/
│   └── data_validation.conf                  # Framework configuration (key=value)
│
├── data_validation/                          # Main Python package
│   ├── __init__.py
│   ├── main.py                               # Spark entry point & job orchestrator
│   │
│   ├── connectors/
│   │   ├── __init__.py
│   │   └── source_extract.py                 # Data loaders: BigQuery, Cloud SQL, GCS
│   │
│   ├── validators/
│   │   ├── __init__.py
│   │   ├── business.py                       # SQL push-down business rule engine
│   │   └── custodial.py                      # Schema / data-quality validation tests
│   │
│   └── notifications/
│       ├── __init__.py
│       └── mailer.py                         # SendGrid email alert dispatcher
│
├── cloud_functions/
│   └── gcs_trigger/
│       ├── main.py                           # Cloud Function entry point
│       └── requirements.txt                  # CF-specific deps (no PySpark)
│
├── sql/
│   ├── ddl/
│   │   ├── 00_create_database.sql            # Create Data_Validation database
│   │   └── 01_create_tables.sql              # All 8 metadata tables (dependency order)
│   └── dml/
│       ├── 01_seed_rules.sql                 # Sample validation rules (custodial + business)
│       ├── 02_seed_objects.sql               # Sample source object registrations
│       ├── 03_seed_mappings.sql              # Sample rule-to-object mappings
│       ├── 04_seed_thresholds.sql            # Criticality thresholds per mapping
│       └── 05_seed_notifications.sql         # Email notification config
│
└── tests/
    └── __init__.py                           # Placeholder for unit tests
```

---

## Database Setup

All validation metadata is stored in a MySQL database (Cloud SQL).  The `sql/`
folder contains the complete DDL and sample DML to bootstrap the schema.

### Folder layout

```
sql/
├── ddl/
│   ├── 00_create_database.sql   # Create the Data_Validation database
│   └── 01_create_tables.sql     # All 8 metadata tables in dependency order
└── dml/
    ├── 01_seed_rules.sql        # Sample validation rules (all rule types)
    ├── 02_seed_objects.sql      # Sample source object registrations
    ├── 03_seed_mappings.sql     # Rule → object mappings
    ├── 04_seed_thresholds.sql   # Criticality thresholds per mapping
    └── 05_seed_notifications.sql# Email alert configuration
```

### Metadata tables

| Table | Purpose |
|---|---|
| `data_validation_rule` | Validation rule definitions (logic + type) |
| `data_validation_object_lookup` | Registry of tables / files to validate |
| `data_validation_rule_mapping` | Links rules to objects with column detail |
| `data_validation_rule_threshold` | Failure-rate → criticality tier mappings |
| `data_validation_notification_message_handler` | Alert message templates |
| `data_validation_notification_distribution_list` | Recipient email addresses |
| `data_validation_notification` | Pairs a handler template with a distribution list |
| `data_validation_object_notification` | Links objects to notifications with a criticality gate |

### Run order

```bash
HOST=<cloud_sql_ip>
USER=root
DB=Data_Validation

# 1. Create database
mysql -h $HOST -u $USER -p < sql/ddl/00_create_database.sql

# 2. Create all tables
mysql -h $HOST -u $USER -p $DB < sql/ddl/01_create_tables.sql

# 3. (Optional) Load sample data
mysql -h $HOST -u $USER -p $DB < sql/dml/01_seed_rules.sql
mysql -h $HOST -u $USER -p $DB < sql/dml/02_seed_objects.sql
mysql -h $HOST -u $USER -p $DB < sql/dml/03_seed_mappings.sql
mysql -h $HOST -u $USER -p $DB < sql/dml/04_seed_thresholds.sql
mysql -h $HOST -u $USER -p $DB < sql/dml/05_seed_notifications.sql
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.8+ |
| Apache Spark (Dataproc) | 2.x / 3.x |
| Google Cloud SDK (`gcloud`, `gsutil`, `bq`) | Latest |
| GCP services | BigQuery, Cloud Storage, Dataproc, Cloud Functions |
| MySQL (Cloud SQL) | 5.7+ — stores validation metadata |

---

## Configuration

Copy `config/data_validation.conf.example` to `config/data_validation.conf` (git-ignored) and fill in
real values. The framework downloads this file from GCS at runtime — path given via the
`CONF_FILE_GCS` env var. Each line uses `key=value` format.

```ini
# MySQL metadata database
user=<db_user>
pswd=<db_password>         # ⚠ Move to DB_PASSWORD env var in production
hostip=<cloud_sql_ip>
hport=3306
database=Data_Validation

# GCP project
project=<gcp_project_id>

# BigQuery dataset
dataset_id=<bq_dataset>
table_name=data_validation_rule

# Output tables (within dataset_id)
output_table=data_validation_detailed_error_result
output_agg_table=data_validation_aggregated_error_result
obj_log_table=data_validation_object_log
execution_stat_table=validation_execution

# GCS staging directory (bucket name only, no gs://)
output_dir=<gcs_bucket_name>
```

Upload to GCS before running:

```bash
gsutil cp config/data_validation.conf \
    gs://<your-bucket>/validation_framework/config/data_validation.conf
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `CONF_FILE_GCS` | Yes | GCS path to the uploaded `.conf` file, e.g. `gs://<bucket>/validation_framework/config/data_validation.conf` |
| `SENDGRID_API_KEY` | Yes (for email alerts) | SendGrid API key for SMTP auth |
| `NOTIFICATION_SENDER` | No | Sender email address (default: `noreply@example.com`) |
| `DB_PASSWORD` | Recommended | Overrides `pswd` in the conf file |
| `ALLOW_CUSTOM_SPARK_EXEC` | No | Set to `1` to enable the `custom_spark` rule type, which `exec()`s Python code stored in the rule metadata. Disabled by default — only enable with a trusted, access-controlled metadata database |
| `GCP_PROJECT` | Cloud Function only | GCP project ID |
| `SCRIPTS_BUCKET` | Cloud Function only | GCS bucket/prefix holding `main.py` and `data_validation.zip` |
| `VALIDATION_DATASETS` | Cloud Function only | Comma-separated BigQuery datasets to scan for validated tables |
| `OUTPUT_DATASET` / `OUTPUT_TABLE` / `OUTPUT_DIR` | Cloud Function only | Where the submitted Dataproc job writes its output |
| `DATAPROC_CLUSTER` | Cloud Function only | Name of the Dataproc cluster to submit jobs to |

---

## Deployment

### 1 — Package the Python source

The Dataproc job receives the `data_validation` package as a zip via `python_file_uris`.

```bash
# From the repo root
zip -r data_validation.zip data_validation/

# Upload package and entry point
gsutil cp data_validation.zip \
    gs://<your-bucket>/validation_framework/scripts/data_validation.zip

gsutil cp data_validation/main.py \
    gs://<your-bucket>/validation_framework/scripts/main.py
```

### 2 — Deploy the Cloud Function

```bash
gcloud functions deploy hello_gcs \
    --runtime python310 \
    --trigger-resource <your-gcs-bucket> \
    --trigger-event google.storage.object.finalize \
    --source cloud_functions/gcs_trigger/ \
    --entry-point hello_gcs \
    --set-env-vars GCP_PROJECT=<gcp_project_id>,SCRIPTS_BUCKET=<your-bucket>/validation_framework/scripts,VALIDATION_DATASETS=<dataset1,dataset2>,OUTPUT_DATASET=<output_dataset>,OUTPUT_TABLE=<output_table>,OUTPUT_DIR=<gcs_bucket>/tmp,DATAPROC_CLUSTER=<cluster_name>
```

---

## Running a Validation Job

Jobs can also be submitted directly to Dataproc without the Cloud Function:

```bash
gcloud dataproc jobs submit pyspark \
    gs://<your-bucket>/validation_framework/scripts/main.py \
    --cluster=data-validation \
    --region=<region> \
    --py-files=gs://<your-bucket>/validation_framework/scripts/data_validation.zip \
    --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar \
    -- <job_type> <object_id> <job_id> [<extra_args>...]
```

### Job type argument reference

| `job_type` | `sys.argv[2]` | `sys.argv[3]` | `sys.argv[4]` | `sys.argv[5]` | `sys.argv[6]` |
|---|---|---|---|---|---|
| `gcs` | — | GCS bucket | GCS file path | BigQuery dataset | Table ID |
| `cloudsql` | Object ID | Job ID | — | DB name | — |
| `bigquery` | Object ID | Job ID | — | DB name | — |

**Examples**

```bash
# Validate a GCS CSV file
-- gcs my-job-001 my-bucket data/accounts.csv salesforce account

# Validate a BigQuery table
-- bigquery 42 my-job-002 my-job-002 salesforce
```

---

## Validation Rule Types

Rules are stored in the MySQL metadata database and referenced by `Rule_Logic` (JSON).

### Custodial rules (schema / data-quality)

| Test key | Description |
|---|---|
| `is_nullable: "NO"` | Fails if any null values found in the column |
| `distinct: true` | Fails if duplicate values found |
| `regex: "<pattern>"` | Fails if value does not match the regex |
| `allowed: [...]` | Fails if value is not in the permitted list |
| `forbidden: [...]` | Fails if value is in the forbidden list |
| `min: <val>` | Fails if value is less than the minimum |
| `max: <val>` | Fails if value exceeds the maximum |
| `min_length: <n>` | Fails if string length is below minimum |
| `max_length: <n>` | Fails if string length exceeds maximum |
| `min_date: "<date>"` | Fails if date is earlier than the minimum |
| `max_date: "<date>"` | Fails if date is later than the maximum |
| `data_type: "<BQ_type>"` | Fails if inferred Spark type is incompatible |
| `custom_sql: "<SQL>"` | Executes a custom SQL query against a Spark view |
| `custom_spark: "<code>"` | ⚠ Executes arbitrary Spark code — use with caution |

### Business rules

Business rules use complete SQL statements stored in `Rule_Logic`. The framework pushes them directly to BigQuery and counts the resulting failure rows.

---

## Output Tables

### `data_validation_detailed_error_result`

| Column | Type | Description |
|---|---|---|
| `Primary_Key_Value_s_` | STRING | Concatenated primary key value(s) |
| `Actual_Value` | STRING | Concatenated actual column value(s) |
| `Run_ID` | INT | Run identifier |
| `Period` | DATE | Validation period date |
| `Object_ID` | INT | Validated object identifier |
| `Rule_ID` | INT | Rule that was violated |
| `Mapping_ID` | INT | Rule-to-object mapping identifier |
| `Column_Name` | STRING | Column(s) validated |
| `Primary_Key_Column_s_` | STRING | Primary key column name(s) |
| `Last_Update_Date` | TIMESTAMP | Row write timestamp |

### `data_validation_aggregated_error_result`

| Column | Type | Description |
|---|---|---|
| `Failure_Count` | INT | Number of failing rows |
| `Failure_Percent` | FLOAT | Failure rate (failures / total rows) |
| `Criticality` | INT | Severity level derived from threshold config |
| `Object_ID`, `Rule_ID`, `Mapping_ID` | INT | Identifiers |
| `Last_Update_Date` | TIMESTAMP | Row write timestamp |

### `validation_execution` (execution status)

| `Val_Status` | Meaning |
|---|---|
| `Executing` | Job is currently running |
| `Success` | Job completed without errors |
| `Failure` | Job encountered an unrecoverable error |

---

## Notification Setup

Email alerts are sent when the failure rate for a rule exceeds its configured criticality threshold.

1. Set the `SENDGRID_API_KEY` environment variable on the Dataproc cluster.
2. Configure distribution lists in the MySQL metadata tables:
   - `data_validation_notification`
   - `data_validation_notification_distribution_list`
   - `data_validation_notification_message_handler`
   - `data_validation_object_notification`

---

## AI-Assisted Rule Generation

`data_validation/rule_intelligence/` is an optional, local CLI tool (never run on Dataproc) that uses
Gemini via LangChain to propose candidate validation rules from a sample data file and/or a freeform
data-contract document, subject to human review before anything is written to the metadata database.

Install the extra dependencies:

```bash
pip install -e ".[rule-intelligence]"
export GOOGLE_API_KEY=<your Gemini API key from https://aistudio.google.com/apikey>
```

**1. Generate candidate rules** from a sample CSV/JSON file and (optionally) a data-contract document:

```bash
dv-rules-generate \
    --sample-file samples/accounts.csv \
    --contract contracts/accounts_contract.md \
    --object-name account \
    --object-database-name salesforce \
    --out review/account_rules.yaml
```

This profiles the sample data with pandas, sends the profile (and contract text, if given) to Gemini,
and writes a `review/account_rules.yaml` file — one entry per proposed rule, each with `include: true`,
a `rule_logic` mapping, a confidence score, and a rationale.

**2. Review the YAML by hand.** Toggle `include: false` to reject a rule, edit `rule_description` or
`rule_logic`, or delete entries outright. Nothing is written to the database until step 3.

**3. Apply the reviewed rules** to the MySQL metadata tables:

```bash
dv-rules-apply --review-file review/account_rules.yaml --conf config/data_validation.conf
```

This writes (in FK order) `data_validation_object_lookup` → `data_validation_rule` →
`data_validation_rule_mapping` → `data_validation_rule_threshold`, inside a single transaction, and
skips rule mappings that already exist (re-running `apply` on the same file is safe; pass `--force` to
bypass this). Use `--dry-run` to preview the planned inserts without connecting to the database.

Notes:
- The LLM can only propose rules from the existing `Rule_Logic` vocabulary (`is_nullable`, `distinct`,
  `regex`, `allowed`, `forbidden`, `min`, `max`, `min_length`, `max_length`, `min_date`, `max_date`,
  `data_type`, `custom_sql`, or a business SQL statement). It can never generate a `custom_spark`
  rule — that type is not a valid value in the underlying schema, so it cannot be produced even via a
  maliciously-crafted contract document.
- Business rules (raw SQL) are not syntax-checked against BigQuery by this tool — review them more
  carefully than custodial rules, since errors only surface at actual validation run time.
- This subpackage has no import-time dependency on `pyspark`/BigQuery; it's independent of the
  Dataproc job's runtime requirements.

---

## Contributing

1. Fork the repository and create a feature branch.
2. Follow the existing code style (PEP 8, PEP 257 docstrings, `snake_case`).
3. Add or update tests in `tests/` for any new logic that does not require a live Spark cluster.
4. Open a pull request with a clear description of the change.

> **Security note**: Never commit credentials, API keys, or IP addresses to this repository. Use environment variables or Google Secret Manager for all sensitive values.