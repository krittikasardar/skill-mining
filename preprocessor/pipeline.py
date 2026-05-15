"""
preprocessor/pipeline.py (v2)
-----------------------------
Orchestrates preprocessing for the Skill Mining pipeline.

This module supports two entry points:
  1. transform(raw_doc)  -> used by main.py after collection
  2. preprocess(raw_path) -> used when preprocessing an existing raw JSON file

v2 keeps the richer data-gap fields introduced in the collectors:
  - profile README / pinned repositories / organisations
  - richer aggregate_signals
  - month-level historical_analysis
  - commit and contribution evidence types
  - collaboration and tooling evidence
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import config
from utils.helpers import get_logger, utcnow_iso
from preprocessor.cleaner import filter_and_clean
from preprocessor.chunker import chunk_evidence_index
from preprocessor.historical import build_historical_analysis

logger = get_logger(__name__)

PREPROCESSED_DIR = Path(getattr(config, "DATA_DIR", Path("data"))) / "preprocessed"
PREPROCESSED_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CHUNK_MAX_CHARS = int(getattr(config, "CHUNK_MAX_CHARS", 1500))


# ---------------------------------------------------------------------------
# Public API used by main.py
# ---------------------------------------------------------------------------

def transform(
    raw_doc: dict[str, Any],
    chunk_max_chars: int = DEFAULT_CHUNK_MAX_CHARS,
) -> dict[str, Any]:
    """
    Transform an in-memory raw v2 document into a processed document.

    This is the function used by the updated main.py:
        processed_doc = transform(raw_doc)

    It preserves the important raw sections while also producing cleaned/chunked
    RAG-ready evidence.
    """
    username = _get_username(raw_doc)
    evidence_index = raw_doc.get("evidence_index", []) or []
    repositories = raw_doc.get("repositories", []) or []

    original_count = len(evidence_index)

    # 1. Clean evidence items
    cleaned, dropped = filter_and_clean(evidence_index)

    # 2. Chunk evidence items for retrieval
    chunks = chunk_evidence_index(cleaned, max_chars=chunk_max_chars)

    # 3. Historical analysis
    # Prefer an already-computed block from main.py/schema, but recompute if absent.
    historical = raw_doc.get("historical_analysis")
    if not historical:
        historical = build_historical_analysis(repositories)

    # 4. Preserve aggregate signals and enrich them with selected historical fields
    aggregate_signals = dict(raw_doc.get("aggregate_signals", {}) or {})
    _merge_historical_into_aggregate(aggregate_signals, historical)

    # 5. Output stats
    avg_len = (
        round(sum(len(str(c.get("content", ""))) for c in chunks) / len(chunks))
        if chunks else 0
    )

    evidence_type_counts: dict[str, int] = {}
    for item in cleaned:
        ev_type = item.get("type", "unknown")
        evidence_type_counts[ev_type] = evidence_type_counts.get(ev_type, 0) + 1

    doc = {
        "schema_version": "2.0",
        "username": username,
        "preprocessed_at": utcnow_iso(),
        "source_schema_version": raw_doc.get("schema_version"),

        # Keep high-value non-chunked context for downstream agents.
        "profile": raw_doc.get("profile", {}),
        "aggregate_signals": aggregate_signals,
        "historical_analysis": historical,

        # Keep lightweight repository metadata/evidence summary.
        # Full repositories are preserved too because the project is still small
        # and evidence-grounded agents may need exact repo-level fields.
        "repositories": repositories,

        # Cleaned + chunked retrieval data.
        "evidence_index": cleaned,
        "chunks": chunks,

        "stats": {
            "original_evidence_count": original_count,
            "cleaned_evidence_count": len(cleaned),
            "items_dropped": dropped,
            "chunks_produced": len(chunks),
            "avg_chunk_length_chars": avg_len,
            "evidence_type_counts": evidence_type_counts,
            "repo_count": len(repositories),
        },
        "collection_metadata": raw_doc.get("collection_metadata", {}),
        "preprocessing_metadata": {
            "chunk_max_chars": chunk_max_chars,
            "pipeline_version": "2.0",
            "notes": (
                "Processed output preserves v2 data-gap fields such as commit depth, "
                "collaboration signals, monthly activity patterns, tooling detection, "
                "profile README, pinned repositories, and richer evidence types."
            ),
        },
    }

    return doc


# ---------------------------------------------------------------------------
# File-based API retained for older workflows
# ---------------------------------------------------------------------------

def preprocess(
    raw_path: Path,
    output_dir: Path = PREPROCESSED_DIR,
    chunk_max_chars: int = DEFAULT_CHUNK_MAX_CHARS,
) -> dict[str, Any]:
    """
    Run preprocessing on one raw JSON file and write a preprocessed JSON file.

    Parameters
    ----------
    raw_path:
        Path to a raw JSON file.
    output_dir:
        Directory where the preprocessed output should be written.
    chunk_max_chars:
        Maximum characters per output chunk.
    """
    raw_path = Path(raw_path)
    logger.info("Preprocessing: %s", raw_path)

    raw_doc = json.loads(raw_path.read_text(encoding="utf-8", errors="replace"))
    doc = transform(raw_doc, chunk_max_chars=chunk_max_chars)
    doc["source_file"] = str(raw_path)

    username = doc.get("username") or raw_path.stem
    output_dir.mkdir(parents=True, exist_ok=True)

    # Keep older filename pattern for file-based preprocessing.
    out_path = output_dir / f"{username}_preprocessed.json"
    out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(
        "Wrote %d chunks (%d dropped) -> %s",
        doc.get("stats", {}).get("chunks_produced", 0),
        doc.get("stats", {}).get("items_dropped", 0),
        out_path,
    )
    return doc


def preprocess_all(
    raw_dir: Path = config.RAW_DIR,
    output_dir: Path = PREPROCESSED_DIR,
    chunk_max_chars: int = DEFAULT_CHUNK_MAX_CHARS,
) -> list[dict[str, Any]]:
    """
    Preprocess every non-empty .json file found in raw_dir.
    Returns one processed document per successfully processed file.
    """
    raw_dir = Path(raw_dir)
    files = sorted(f for f in raw_dir.glob("*.json") if f.stat().st_size > 0)

    results: list[dict[str, Any]] = []
    for f in files:
        try:
            results.append(preprocess(f, output_dir=output_dir, chunk_max_chars=chunk_max_chars))
        except Exception as exc:
            logger.error("Failed to preprocess %s: %s", f.name, exc, exc_info=True)
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_username(raw_doc: dict[str, Any]) -> str:
    """Resolve username from collection metadata or profile."""
    return (
        raw_doc.get("collection_metadata", {}).get("username")
        or raw_doc.get("profile", {}).get("login")
        or raw_doc.get("username")
        or "unknown"
    )


def _merge_historical_into_aggregate(
    aggregate_signals: dict[str, Any],
    historical: dict[str, Any],
) -> None:
    """
    Copy selected historical-analysis values into aggregate_signals.

    This helps downstream agents retrieve the most important temporal features
    without having to inspect a separate block.
    """
    if not historical:
        return

    keys_to_copy = [
        "commits_by_year",
        "monthly_commit_heatmap",
        "weekday_vs_weekend_ratio",
        "most_active_month_of_year",
        "inactive_periods",
        "recent_6_month_commit_count",
        "repo_creation_cadence",
        "activity_trend",
        "peak_activity_year",
        "tech_evolution",
    ]

    for key in keys_to_copy:
        if key in historical and key not in aggregate_signals:
            aggregate_signals[key] = historical[key]
