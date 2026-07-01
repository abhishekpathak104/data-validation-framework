"""
data_validation/rule_intelligence/contract.py
------------------------------------------------
Loads a freeform data-contract document (Markdown/plain text) as raw context
for the LLM. No structured parsing is attempted — the LLM is responsible for
interpreting the prose.
"""

import logging

logger = logging.getLogger(__name__)

# Bounds prompt size for very large contract documents.
_MAX_CONTRACT_CHARS = 20_000


def load_contract(path: str | None) -> str | None:
    """Read a data-contract document as plain text.

    Args:
        path (str | None): Path to a ``.md``/``.txt`` (or any plain-text)
            data-contract document. ``None`` if no contract was supplied.

    Returns:
        str | None: The document text (truncated to ``_MAX_CONTRACT_CHARS``
        characters if longer), or ``None`` if ``path`` is ``None``.
    """
    if path is None:
        return None

    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    if len(text) > _MAX_CONTRACT_CHARS:
        logger.warning(
            "Data contract document '%s' truncated from %d to %d characters "
            "to bound LLM prompt size.",
            path,
            len(text),
            _MAX_CONTRACT_CHARS,
        )
        text = text[:_MAX_CONTRACT_CHARS]

    return text
