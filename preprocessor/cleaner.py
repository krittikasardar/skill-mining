"""
preprocessor/cleaner.py
-----------------------
Cleans individual evidence snippets from the raw evidence_index.

v2 updates
----------
- Preserves new evidence types introduced by the GitHub data-gap fixes:
  profile, skill, role, leadership, commit, contribution
- Uses evidence-type-aware minimum lengths so short but useful commit or
  contribution evidence is not accidentally dropped.
- Preserves metadata instead of flattening/removing it.
- Cleans simple text corruption in both content and string metadata fields.
- Handles non-string content safely by serialising it to compact JSON/text.

Operations:
  - Replace Unicode replacement characters (U+FFFD) from encoding corruption
  - Strip HTML tags
  - Remove markdown badge/shield image lines
  - Collapse excessive whitespace and blank lines
  - Drop only genuinely empty/low-signal items
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any

from utils.helpers import get_logger

logger = get_logger(__name__)

# Default threshold for old evidence items.
MIN_CONTENT_CHARS = 20

# Lower thresholds for structured v2 evidence.
# Commit/contribution evidence can be short but still valuable when metadata
# contains repo, date, PR/issue counts, or commit frequency.
MIN_CONTENT_CHARS_BY_TYPE: dict[str, int] = {
    "profile": 10,
    "skill": 15,
    "role": 10,
    "leadership": 10,
    "commit": 5,
    "contribution": 8,
}

KNOWN_EVIDENCE_TYPES = {
    "profile",
    "skill",
    "role",
    "leadership",
    "commit",
    "contribution",
}

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BADGE_RE = re.compile(
    r"!\[.*?\]\(https?://.*?(badge|shield|travis|circleci|codecov|github\.com/.*?/workflows).*?\)",
    re.IGNORECASE,
)
_MULTI_BLANK_RE = re.compile(r"\n{3,}")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")
# U+FFFD surrounded by digits → likely a corrupted dash, e.g. "2022�2025".
_FFFD_BETWEEN_DIGITS = re.compile(r"(\d)�(\d)")


def clean_text(text: Any) -> str:
    """
    Return a cleaned version of a single text value.

    Non-string values are converted safely:
      - dict/list -> compact JSON string
      - None      -> empty string
      - others    -> str(value)
    """
    if text is None:
        return ""

    if isinstance(text, (dict, list)):
        try:
            text = json.dumps(text, ensure_ascii=False, sort_keys=True)
        except TypeError:
            text = str(text)
    elif not isinstance(text, str):
        text = str(text)

    # Fix U+FFFD between digits before stripping it elsewhere.
    text = _FFFD_BETWEEN_DIGITS.sub(r"\1–\2", text)

    # Strip remaining replacement characters.
    text = text.replace("�", "")

    # Remove simple HTML tags.
    text = _HTML_TAG_RE.sub("", text)

    # Remove badge-only noise lines.
    lines = text.splitlines()
    lines = [ln for ln in lines if not _BADGE_RE.search(ln)]

    text = "\n".join(lines)
    text = _MULTI_BLANK_RE.sub("\n\n", text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()


def _clean_metadata(value: Any) -> Any:
    """
    Recursively clean string values inside metadata while preserving structure.

    This keeps important grounding fields such as repo names, commit dates,
    PR counts, issue counts, and language breakdowns available for RAG.
    """
    if isinstance(value, dict):
        return {k: _clean_metadata(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_metadata(v) for v in value]
    if isinstance(value, str):
        return clean_text(value)
    return value


def _min_chars_for_item(item: dict) -> int:
    ev_type = str(item.get("type", "")).lower()
    return MIN_CONTENT_CHARS_BY_TYPE.get(ev_type, MIN_CONTENT_CHARS)


def is_meaningful(text: str, min_chars: int = MIN_CONTENT_CHARS) -> bool:
    """Return True if text has substantive content worth embedding."""
    return bool(text) and len(text) >= min_chars


def clean_evidence_item(item: dict) -> dict | None:
    """
    Clean one evidence item.

    Returns None only if the item has no useful content after cleaning.
    Metadata is preserved and lightly cleaned.
    """
    if not isinstance(item, dict):
        return None

    raw = item.get("content", "")
    cleaned_content = clean_text(raw)

    min_chars = _min_chars_for_item(item)
    if not is_meaningful(cleaned_content, min_chars=min_chars):
        return None

    cleaned_item = deepcopy(item)
    cleaned_item["content"] = cleaned_content

    # Normalise evidence type but do not reject unknown types.
    # This keeps the pipeline forward-compatible with future evidence labels.
    ev_type = cleaned_item.get("type")
    if isinstance(ev_type, str):
        cleaned_item["type"] = ev_type.strip().lower()

    # Preserve metadata and clean only string noise inside it.
    metadata = cleaned_item.get("metadata", {})
    cleaned_item["metadata"] = _clean_metadata(metadata if isinstance(metadata, dict) else {})

    # Ensure commonly expected fields exist.
    cleaned_item.setdefault("evidence_id", "")
    cleaned_item.setdefault("source", "")
    cleaned_item.setdefault("type", "unknown")

    return cleaned_item


def filter_and_clean(evidence_index: list[dict]) -> tuple[list[dict], int]:
    """
    Clean all evidence items, dropping only empty or very low-signal items.

    Returns
    -------
    (cleaned_items, dropped_count)
    """
    cleaned: list[dict] = []
    dropped = 0

    for item in evidence_index or []:
        result = clean_evidence_item(item)
        if result is None:
            dropped += 1
            logger.debug(
                "Dropped evidence item %s (too short or empty after cleaning)",
                item.get("evidence_id") if isinstance(item, dict) else None,
            )
        else:
            cleaned.append(result)

    logger.info("Cleaned %d items, dropped %d", len(cleaned), dropped)
    return cleaned, dropped
