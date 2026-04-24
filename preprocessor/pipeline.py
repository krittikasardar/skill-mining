"""
preprocessor/pipeline.py
------------------------
Orchestrates the full preprocessing pipeline for a single user's raw JSON.

Steps:
  1. Load raw JSON from data/raw/{username}.json
  2. Clean evidence items  (fix encoding, strip noise, drop empties)
  3. Chunk long items into RAG-sized segments
  4. Build historical analysis from repository commit/language data
  5. Write output to data/preprocessed/{username}_preprocessed.json

Output schema (schema_version 1.1):
  {
    "schema_version": "1.1",
    "username": "...",
    "preprocessed_at": "...",
    "source_file": "...",
    "historical_analysis": { commits_by_year, languages_by_year, activity_trend,
                              peak_activity_year, tech_evolution },
    "chunks": [ { chunk_id, evidence_id, chunk_index, total_chunks,
                  type, source, content, metadata }, ... ],
    "stats": { original_evidence_count, items_dropped,
               chunks_produced, avg_chunk_length_chars }
  }
"""

import json
from pathlib import Path

import config
from utils.helpers import get_logger, utcnow_iso
from preprocessor.cleaner import filter_and_clean
from preprocessor.chunker import chunk_evidence_index
from preprocessor.historical import build_historical_analysis

logger = get_logger(__name__)

PREPROCESSED_DIR = config.DATA_DIR / "preprocessed"
PREPROCESSED_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CHUNK_MAX_CHARS = 1500


def preprocess(
    raw_path: Path,
    output_dir: Path = PREPROCESSED_DIR,
    chunk_max_chars: int = DEFAULT_CHUNK_MAX_CHARS,
) -> dict:
    """
    Run the full preprocessing pipeline on one raw JSON file.

    Parameters
    ----------
    raw_path        : path to data/raw/{username}.json
    output_dir      : directory to write the preprocessed output file
    chunk_max_chars : maximum characters per output chunk

    Returns
    -------
    The preprocessed document dict (also written to disk).
    """
    logger.info("Preprocessing: %s", raw_path)
    raw = json.loads(raw_path.read_text(encoding="utf-8", errors="replace"))
    username = (
        raw.get("collection_metadata", {}).get("username") or raw_path.stem
    )
    evidence_index = raw.get("evidence_index", [])
    repositories = raw.get("repositories", [])

    original_count = len(evidence_index)

    # Step 1: Clean
    cleaned, dropped = filter_and_clean(evidence_index)

    # Step 2: Chunk
    chunks = chunk_evidence_index(cleaned, max_chars=chunk_max_chars)

    # Step 3: Historical analysis
    historical = build_historical_analysis(repositories)

    # Step 4: Assemble output
    avg_len = (
        round(sum(len(c["content"]) for c in chunks) / len(chunks))
        if chunks
        else 0
    )
    doc = {
        "schema_version": "1.1",
        "username": username,
        "preprocessed_at": utcnow_iso(),
        "source_file": str(raw_path),
        "historical_analysis": historical,
        "chunks": chunks,
        "stats": {
            "original_evidence_count": original_count,
            "items_dropped": dropped,
            "chunks_produced": len(chunks),
            "avg_chunk_length_chars": avg_len,
        },
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{username}_preprocessed.json"
    out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(
        "Wrote %d chunks (%d dropped) → %s", len(chunks), dropped, out_path
    )
    return doc


def preprocess_all(
    raw_dir: Path = config.RAW_DIR,
    output_dir: Path = PREPROCESSED_DIR,
    chunk_max_chars: int = DEFAULT_CHUNK_MAX_CHARS,
) -> list[dict]:
    """
    Preprocess every non-empty .json file found in raw_dir.
    Returns a list of output dicts (one per successfully processed file).
    """
    files = sorted(f for f in raw_dir.glob("*.json") if f.stat().st_size > 0)
    results = []
    for f in files:
        try:
            doc = preprocess(f, output_dir=output_dir, chunk_max_chars=chunk_max_chars)
            results.append(doc)
        except Exception as exc:
            logger.error("Failed to preprocess %s: %s", f.name, exc)
    return results
