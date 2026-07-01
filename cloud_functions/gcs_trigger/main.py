"""
cloud_functions/gcs_trigger/main.py
-----------------------------------
Google Cloud Function triggered by a GCS file upload event.

When a new file lands in the monitored bucket, this function looks up whether
the file's table ID has a validation schema defined in BigQuery, then submits
a Dataproc PySpark job to run the full data-validation pipeline against that
file.

The Dataproc job uses a pre-built zip of the ``data_validation`` package
uploaded to GCS as ``data_validation.zip`` via ``python_file_uris``.  Build
and upload the zip with::

    cd data-validation-framework
    zip -r data_validation.zip data_validation/
    gsutil cp data_validation.zip gs://<your-bucket>/validation_framework/scripts/

Environment variables:
    GCP_PROJECT       - GCP project ID.
    SCRIPTS_BUCKET     - GCS bucket/prefix holding main.py and data_validation.zip
                         (e.g. 'my-bucket/validation_framework/scripts').
    VALIDATION_DATASETS - Comma-separated BigQuery datasets to scan for tables.
    OUTPUT_DATASET     - BigQuery dataset to write validation output to.
    OUTPUT_TABLE       - BigQuery table to write validation output to.
    OUTPUT_DIR         - GCS staging path for job output.
    DATAPROC_CLUSTER   - Name of the Dataproc cluster to submit jobs to.
"""

import logging
import os
from datetime import datetime

from google.cloud import bigquery, dataproc_v1, storage

logger = logging.getLogger(__name__)

_JAR_FILES = ["gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar"]


def _get_tables(project, datasets):
    """List all BigQuery table IDs across the specified datasets.

    Args:
        project (str): GCP project identifier.
        datasets (list[str]): BigQuery dataset names to scan.

    Returns:
        dict: Mapping of ``{table_id: dataset_name}``.
    """
    bq_client = bigquery.Client(project)
    table_map = {}
    for dataset in datasets:
        dataset_ref = bq_client.dataset(dataset)
        for table_ref in bq_client.list_tables(dataset_ref):
            table_map[table_ref.table_id] = dataset
    return table_map


def hello_gcs(event, context):
    """Cloud Function entry point triggered by a GCS object-finalise event.

    Extracts the table ID from the uploaded file name, checks whether a
    validation schema exists for it, and submits a Dataproc PySpark job if
    so.

    Args:
        event (dict): GCS event payload containing at minimum:
            ``'name'`` (object path) and ``'bucket'`` (bucket name).
        context (google.cloud.functions.Context): Cloud Function invocation
            metadata (generation, event ID, timestamp, etc.).

    Returns:
        str: Dataproc job ID of the submitted job, or ``None`` if the table
        has no registered schema.
    """
    # ---- Configuration ----
    project = os.environ["GCP_PROJECT"]
    datasets = os.environ["VALIDATION_DATASETS"].split(",")
    output_dataset = os.environ["OUTPUT_DATASET"]
    output_table = os.environ["OUTPUT_TABLE"]
    output_dir = os.environ["OUTPUT_DIR"]
    scripts_base = f"gs://{os.environ['SCRIPTS_BUCKET']}"
    main_script = f"{scripts_base}/main.py"
    python_files = [f"{scripts_base}/data_validation.zip"]
    cluster_name = os.environ["DATAPROC_CLUSTER"]

    table_map = _get_tables(project, datasets)
    table_id = event["name"][event["name"].rfind("/") + 1: event["name"].rfind(".")]
    logger.info("GCS trigger: table_id=%s", table_id)

    if table_id not in table_map:
        logger.info("No validation schema found for table_id=%s — skipping.", table_id)
        return None

    job_id = f"DATA_VALIDATION_{table_id}_{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    logger.info("Processing file: %s", event["name"])

    dataproc_client = dataproc_v1.JobControllerClient()
    job_details = {
        "placement": {
            "cluster_name": cluster_name,
        },
        "reference": {
            "job_id": job_id,
        },
        "pyspark_job": {
            "main_python_file_uri": main_script,
            "python_file_uris": python_files,
            "jar_file_uris": _JAR_FILES,
            "args": [
                "gcs",
                job_id,
                event["bucket"],
                event["name"],
                table_map[table_id],
                table_id,
            ],
        },
    }

    result = dataproc_client.submit_job(
        project_id=project, region="global", job=job_details
    )
    submitted_job_id = result.reference.job_id
    logger.info("Submitted Dataproc job ID: %s", submitted_job_id)
    return submitted_job_id
