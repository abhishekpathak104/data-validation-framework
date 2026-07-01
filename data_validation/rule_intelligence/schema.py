"""
data_validation/rule_intelligence/schema.py
----------------------------------------------
Pydantic models that form the contract between the LLM's structured output,
the human-reviewed YAML file, and the MySQL write step.

Security note: ``CustodialRuleType`` intentionally has no ``custom_spark``
member. That rule type executes arbitrary Python via ``exec()``
(see ``data_validation/validators/custodial.py``) and must never be something
an LLM — or a human copy-pasting its output — can produce here. Because it is
not a valid enum value, Gemini cannot emit it even if a data-contract document
tries to instruct it to (prompt injection), and a hand-edited YAML containing
it will fail validation in :func:`read_review_yaml` before it ever reaches the
database.
"""

from enum import Enum
from typing import Union

from pydantic import BaseModel, Field, field_validator, model_validator


class TestType(str, Enum):
    CUSTODIAL = "Custodial"
    BUSINESS = "Business"


class CustodialRuleType(str, Enum):
    """Mirrors the Rule_Logic JSON vocabulary in validators/custodial.py.

    ``custom_spark`` is deliberately excluded — see module docstring.
    """

    IS_NULLABLE = "is_nullable"
    DISTINCT = "distinct"
    REGEX = "regex"
    ALLOWED = "allowed"
    FORBIDDEN = "forbidden"
    MIN = "min"
    MAX = "max"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    MIN_DATE = "min_date"
    MAX_DATE = "max_date"
    DATA_TYPE = "data_type"
    CUSTOM_SQL = "custom_sql"


class CustodialConstraint(BaseModel):
    """A single Rule_Logic JSON key/value pair, e.g. {"min_length": 2}."""

    rule_type: CustodialRuleType
    value: Union[str, bool, int, float, list[str]] = Field(
        description=(
            "The constraint value. E.g. 'NO' for is_nullable, true for distinct, "
            "a regex string, a list of strings for allowed/forbidden, a number "
            "for min/max/min_length/max_length, a date string for "
            "min_date/max_date, or a Spark SQL SELECT string for custom_sql."
        )
    )

    @field_validator("rule_type")
    @classmethod
    def _reject_exec_types(cls, v):
        # Defense in depth: unreachable given the Enum, but kept explicit.
        if v == "custom_spark":
            raise ValueError("custom_spark is not permitted for LLM-generated rules.")
        return v


class CandidateRule(BaseModel):
    """One proposed validation rule; maps to a single data_validation_rule row."""

    rule_id_temp: str = Field(
        description="Stable local id (e.g. 'r1') for cross-referencing in the review file."
    )
    test_type: TestType
    column_name: str = Field(
        description=(
            "Target column name(s), comma-separated for multi-column rules. "
            "Must match the sample data's actual column names, not any "
            "differently-worded names used in a data-contract document."
        )
    )
    rule_description: str = Field(max_length=500)
    primary_key_suggestion: str | None = Field(
        default=None,
        description=(
            "Suggested comma-separated primary key column(s) for the object. "
            "Only populated on one representative rule per batch."
        ),
    )
    custodial_constraints: list[CustodialConstraint] | None = Field(
        default=None,
        description="Required when test_type == Custodial. One or more constraints "
        "on column_name, merged into a single Rule_Logic JSON object.",
    )
    business_sql: str | None = Field(
        default=None,
        description=(
            "Required when test_type == Business. A raw SQL SELECT statement "
            "(BigQuery dialect) that returns only the FAILING rows."
        ),
    )
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(description="Why this rule was proposed; shown to the human reviewer.")

    @model_validator(mode="after")
    def _require_matching_payload(self):
        if self.test_type == TestType.BUSINESS and not self.business_sql:
            raise ValueError("business_sql is required when test_type == Business")
        if self.test_type == TestType.CUSTODIAL and not self.custodial_constraints:
            raise ValueError("custodial_constraints is required when test_type == Custodial")
        return self


class RuleBatch(BaseModel):
    """Top-level structured-output payload requested from the LLM."""

    object_name_suggestion: str
    object_database_name_suggestion: str | None = None
    rules: list[CandidateRule] = Field(default_factory=list)


class ReviewObjectInfo(BaseModel):
    """Object-level metadata in the human-reviewed YAML file."""

    object_name: str
    object_database_name: str
    primary_key: str


class ReviewedRule(CandidateRule):
    """A CandidateRule plus the human's include/exclude decision."""

    include: bool = True


class ReviewedBatch(BaseModel):
    """The full contents of a human-reviewed YAML file, re-validated on read."""

    object: ReviewObjectInfo
    rules: list[ReviewedRule] = Field(default_factory=list)
