"""
preprocessor/cleaner.py
-----------------------
Cleans individual evidence snippets from the raw evidence_index.

Operations:
  - Replace Unicode replacement characters (U+FFFD) from encoding corruption
  - Strip HTML tags
  - Remove markdown badge/shield image lines
  - Collapse excessive whitespace and blank lines
  - Drop items whose content is empty or below a minimum length after cleaning
"""

import re
from utils.helpers import get_logger

logger = get_logger(__name__)

MIN_CONTENT_CHARS = 20

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BADGE_RE = re.compile(
    r"!\[.*?\]\(https?://.*?(badge|shield|travis|circleci|codecov|github\.com/.*?/workflows).*?\)",
    re.IGNORECASE,
)
_MULTI_BLANK_RE = re.compile(r"\n{3,}")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")
# U+FFFD surrounded by digits → likely a corrupted dash (e.g. "2022–2025")
_FFFD_BETWEEN_DIGITS = re.compile(r"(\d)�(\d)")


def clean_text(text: str) -> str:
    """Return a cleaned version of a single text string."""
    # Fix U+FFFD between digits (corrupted dash) before stripping it elsewhere
    text = _FFFD_BETWEEN_DIGITS.sub(r"\1–\2", text)
    # Strip remaining replacement characters
    text = text.replace("�", "")
    text = _HTML_TAG_RE.sub("", text)
    lines = text.splitlines()
    lines = [ln for ln in lines if not _BADGE_RE.search(ln)]
    text = "\n".join(lines)
    text = _MULTI_BLANK_RE.sub("\n\n", text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()


def is_meaningful(text: str, min_chars: int = MIN_CONTENT_CHARS) -> bool:
    """Return True if text has substantive content worth embedding."""
    return bool(text) and len(text) >= min_chars


def clean_evidence_item(item: dict) -> dict | None:
    """
    Clean the content field of one evidence item.
    Returns None if the item should be dropped after cleaning.
    """
    raw = item.get("content", "")
    cleaned = clean_text(raw)
    if not is_meaningful(cleaned):
        return None
    return {**item, "content": cleaned}


def filter_and_clean(evidence_index: list[dict]) -> tuple[list[dict], int]:
    """
    Clean all evidence items, dropping those that are empty or too short.
    Returns (cleaned_items, dropped_count).
    """
    cleaned, dropped = [], 0
    for item in evidence_index:
        result = clean_evidence_item(item)
        if result is None:
            dropped += 1
            logger.debug(
                "Dropped evidence item %s (too short or empty after cleaning)",
                item.get("evidence_id"),
            )
        else:
            cleaned.append(result)
    logger.info("Cleaned %d items, dropped %d", len(cleaned), dropped)
    return cleaned, dropped
