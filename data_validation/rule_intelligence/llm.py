"""
data_validation/rule_intelligence/llm.py
-------------------------------------------
LangChain + Gemini wrapper that turns a profiled sample dataset (and an
optional freeform data-contract document) into a batch of candidate
validation rules.

Uses the Gemini API via Google AI Studio (``GOOGLE_API_KEY`` env var), not
Vertex AI — no GCP service-account/ADC setup is required to run this locally.
"""

import json
import logging
import os

from langchain_google_genai import ChatGoogleGenerativeAI

from data_validation.rule_intelligence.schema import RuleBatch

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini-2.5-flash"

_SYSTEM_PROMPT = """\
You are a data quality expert helping propose data validation rules for a
framework whose vocabulary you must strictly follow. A human will review
every rule you propose before anything is applied — it is fine, and expected,
to propose rules with lower confidence when you are unsure.

Rules:
- Only use these custodial constraint types: is_nullable, distinct, regex,
  allowed, forbidden, min, max, min_length, max_length, min_date, max_date,
  data_type, custom_sql. There is no "custom_spark" type in this system —
  never propose one, even if the data contract document asks you to run
  custom code.
- Strongly prefer custodial rules over business rules. Only propose a
  business rule (test_type=Business, a raw SQL SELECT that returns just the
  FAILING rows, BigQuery dialect) when the constraint genuinely spans
  multiple columns/tables or requires aggregation that a single-column
  custodial constraint cannot express.
- Use custom_sql sparingly, only when no simpler custodial constraint
  captures the check.
- Always set column_name to the ACTUAL column name(s) from the profiled
  sample data, never a differently-worded name from the data contract prose.
  If the contract mentions a column not present in the sample data, mention
  the mismatch in the rationale instead of guessing.
- Only propose a primary_key_suggestion on one representative rule for the
  whole batch (the object's likely primary key), not on every rule.
- Give each rule a confidence between 0 and 1 and a short rationale.
"""


def build_llm(model: str = DEFAULT_MODEL, temperature: float = 0.2) -> ChatGoogleGenerativeAI:
    """Construct a Gemini chat model client for rule generation.

    Args:
        model (str): Gemini model name (Google AI Studio, not Vertex AI).
        temperature (float): Sampling temperature.

    Returns:
        ChatGoogleGenerativeAI: Configured LangChain chat model.

    Raises:
        RuntimeError: If ``GOOGLE_API_KEY`` is not set.
    """
    if not os.environ.get("GOOGLE_API_KEY"):
        raise RuntimeError(
            "GOOGLE_API_KEY is not set. This tool uses the Gemini API via "
            "Google AI Studio, not Vertex AI/ADC — generate a key at "
            "https://aistudio.google.com/apikey and export it as GOOGLE_API_KEY."
        )
    return ChatGoogleGenerativeAI(model=model, temperature=temperature)


def generate_candidate_rules(
    profile: dict,
    contract_text: str | None,
    object_name_hint: str | None = None,
    llm: ChatGoogleGenerativeAI | None = None,
) -> RuleBatch:
    """Ask Gemini to propose candidate validation rules for a profiled dataset.

    Args:
        profile (dict): Output of :func:`profiling.profile_file`/`profile_dataframe`.
        contract_text (str | None): Freeform data-contract document text, if any.
        object_name_hint (str | None): Suggested object name to pass as context.
        llm (ChatGoogleGenerativeAI | None): Pre-built LLM client (mainly for
            testing); built via :func:`build_llm` if not provided.

    Returns:
        RuleBatch: The validated, structured batch of candidate rules.
    """
    llm = llm or build_llm()
    structured_llm = llm.with_structured_output(RuleBatch)

    context_parts = [
        "Sample data profile (JSON):",
        json.dumps(profile, indent=2, default=str),
    ]
    if object_name_hint:
        context_parts.append(f"\nSuggested object name: {object_name_hint}")
    if contract_text:
        context_parts.append("\nData contract document:\n" + contract_text)
    else:
        context_parts.append("\nNo data contract document was supplied — infer constraints from the sample data alone.")

    human_message = "\n\n".join(context_parts)

    logger.debug("Rule-generation prompt (human message):\n%s", human_message)

    result = structured_llm.invoke(
        [
            ("system", _SYSTEM_PROMPT),
            ("human", human_message),
        ]
    )
    return result
