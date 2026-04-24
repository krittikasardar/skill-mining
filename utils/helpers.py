"""
utils/helpers.py
----------------
Shared helper functions used across the pipeline.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
import config

# ── Logger setup ───────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """Return a consistently configured logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    return logger


# ── Date helpers ───────────────────────────────────────────────────────────

def utcnow_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def iso_to_year(iso: Optional[str]) -> Optional[int]:
    """Extract year from an ISO-8601 timestamp string."""
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).year
    except (ValueError, AttributeError):
        return None


def years_since(iso: Optional[str]) -> float:
    """Return fractional years elapsed since the given ISO timestamp."""
    if not iso:
        return 0.0
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return (now - dt).days / 365.25
    except (ValueError, AttributeError):
        return 0.0


# ── Text helpers ───────────────────────────────────────────────────────────

def truncate(text: Optional[str], max_chars: int) -> str:
    """Safely truncate a string and mark it if truncated."""
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + " … [truncated]"


def extract_readme_excerpt(readme: Optional[str], max_chars: int = 4000) -> str:
    """
    Return a cleaned excerpt from README text.
    Strips badge lines and excessive blank lines.
    """
    if not readme:
        return ""
    # Remove badge/shield lines (common noise in READMEs)
    lines = readme.splitlines()
    cleaned = [
        line for line in lines
        if not re.match(r"^\s*!\[.*?\]\(https?://.*?(badge|shield|travis|circleci|codecov).*?\)", line, re.I)
    ]
    joined = "\n".join(cleaned)
    # Collapse runs of blank lines
    joined = re.sub(r"\n{3,}", "\n\n", joined)
    return truncate(joined, max_chars)


# ── Safe attribute getter ──────────────────────────────────────────────────

def safe_get(obj: Any, attr: str, default: Any = None) -> Any:
    """
    Safely retrieve an attribute from a PyGitHub object.
    Returns `default` on AttributeError or if the value is None.
    """
    try:
        val = getattr(obj, attr, default)
        return val if val is not None else default
    except Exception:
        return default


def datetime_to_iso(dt: Optional[datetime]) -> Optional[str]:
    """Convert a datetime object to an ISO-8601 string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
