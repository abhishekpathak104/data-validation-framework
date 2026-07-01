"""
data_validation/rule_intelligence
----------------------------------
LLM-assisted, human-in-the-loop rule generation.

This subpackage is a locally-run CLI tool, separate from the Dataproc/PySpark
validation job in ``data_validation.main``. It never imports ``pyspark`` or
``google.cloud.bigquery`` — installing it is optional (``pip install -e
".[rule-intelligence]"``) and independent of the Dataproc job's runtime deps.

Workflow:
    1. ``dv-rules-generate`` profiles a sample data file (and/or reads a
       freeform data-contract document), asks Gemini (via LangChain) to
       propose candidate validation rules, and writes them to a YAML file.
    2. A human reviews/edits the YAML by hand (toggling ``include``, tweaking
       ``rule_logic``, deleting rules).
    3. ``dv-rules-apply`` reads the reviewed YAML and writes the approved
       rules into the MySQL metadata tables used by the rest of the framework.
"""
