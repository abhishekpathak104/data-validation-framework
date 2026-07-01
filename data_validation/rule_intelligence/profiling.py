"""
data_validation/rule_intelligence/profiling.py
------------------------------------------------
Loads a small sample data file (CSV or JSON) and profiles it into a compact,
JSON-serializable summary that is fed to the LLM as context. Only pandas is
used here — no pyspark/BigQuery dependency.
"""

import logging
import re
import warnings

import pandas as pd

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Cap how much raw data we push into the LLM prompt to keep token usage bounded.
_MAX_SAMPLE_STRING_LEN = 80


def load_sample_data(path: str) -> pd.DataFrame:
    """Load a CSV or JSON sample file into a pandas DataFrame based on extension.

    Args:
        path (str): Path to a ``.csv``, ``.json``, or ``.jsonl`` sample file.

    Returns:
        pd.DataFrame: The loaded sample data.

    Raises:
        ValueError: If the file extension is not recognised.
    """
    lower = path.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return pd.read_json(path, lines=True)
    if lower.endswith(".json"):
        try:
            return pd.read_json(path)
        except ValueError:
            logger.debug("Falling back to JSON-lines parsing for %s", path)
            return pd.read_json(path, lines=True)
    raise ValueError(
        f"Unsupported sample data file extension for '{path}'. "
        "Expected .csv, .json, or .jsonl."
    )


def _truncate(value: str) -> str:
    if len(value) > _MAX_SAMPLE_STRING_LEN:
        return value[:_MAX_SAMPLE_STRING_LEN] + "…"
    return value


def _looks_like_email(series: pd.Series) -> bool:
    sample = series.dropna().astype(str).head(20)
    if sample.empty:
        return False
    return bool(sample.map(lambda v: bool(_EMAIL_RE.match(v))).all())


def _looks_like_date(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    sample = series.dropna().astype(str).head(20)
    if sample.empty:
        return False
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed = pd.to_datetime(sample, errors="coerce")
    return bool(parsed.notna().all())


def profile_dataframe(df: pd.DataFrame, max_sample_values: int = 5) -> dict:
    """Profile a DataFrame into a compact, JSON-serializable summary dict.

    Args:
        df (pd.DataFrame): The sample data to profile.
        max_sample_values (int): Maximum number of distinct non-null sample
            values to include per column.

    Returns:
        dict: ``{"row_count": int, "columns": [...], "profile": {col: {...}}}``.
    """
    profile = {}

    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        distinct_values = non_null.unique()

        entry = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_fraction": float(series.isna().mean()) if len(series) else 0.0,
            "distinct_count": int(len(distinct_values)),
            "sample_values": [
                _truncate(str(v)) for v in list(distinct_values[:max_sample_values])
            ],
        }

        if pd.api.types.is_numeric_dtype(series) and not non_null.empty:
            entry["min"] = float(non_null.min())
            entry["max"] = float(non_null.max())

        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            lengths = non_null.astype(str).map(len)
            if not lengths.empty:
                entry["min_length"] = int(lengths.min())
                entry["max_length"] = int(lengths.max())
            entry["looks_like_email"] = _looks_like_email(series)
            entry["looks_like_date"] = _looks_like_date(series)

        profile[str(col)] = entry

    return {
        "row_count": int(len(df)),
        "columns": [str(c) for c in df.columns],
        "profile": profile,
    }


def profile_file(path: str, max_sample_values: int = 5) -> dict:
    """Load and profile a sample data file in one step.

    Args:
        path (str): Path to a ``.csv``, ``.json``, or ``.jsonl`` sample file.
        max_sample_values (int): Maximum number of distinct sample values per column.

    Returns:
        dict: See :func:`profile_dataframe`.
    """
    df = load_sample_data(path)
    return profile_dataframe(df, max_sample_values=max_sample_values)
