"""
preprocessor/chunker.py
-----------------------
Splits evidence items into RAG-friendly chunks while preserving traceability.

v2 updates
----------
- Keeps metadata for newer evidence types such as commit and contribution.
- Avoids unnecessary splitting for short structured signals.
- Adds type-aware chunk sizes so README/profile evidence can be longer while
  commit/contribution evidence stays compact.
- Adds defensive handling for missing evidence_id/content.
- Adds richer metadata fields useful for retrieval and grounding.
"""

from __future__ import annotations

import re
from typing import Any

from utils.helpers import get_logger

logger = get_logger(__name__)

DEFAULT_MAX_CHARS = 1500
DEFAULT_OVERLAP = 150

# Some evidence types should not be over-split because they are already concise.
TYPE_MAX_CHARS: dict[str, int] = {
    "profile": 1800,
    "skill": 1500,
    "role": 1200,
    "leadership": 1200,
    "commit": 900,
    "contribution": 1000,
}

SUPPORTED_EVIDENCE_TYPES = {
    "profile",
    "skill",
    "role",
    "leadership",
    "commit",
    "contribution",
}


def _split_paragraphs(text: str) -> list[str]:
    """Split text into paragraphs while dropping empty sections."""
    return [p.strip() for p in re.split(r"\n\n+", text) if p.strip()]


def _split_sentences(text: str) -> list[str]:
    """Split text into sentence-like units."""
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def _safe_content(value: Any) -> str:
    """Convert content to a safe string for chunking."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _normalise_overlap(max_chars: int, overlap: int) -> int:
    """
    Keep overlap safe. If overlap is too large, hard splitting can loop badly.
    """
    if max_chars <= 0:
        raise ValueError("max_chars must be greater than 0")
    if overlap < 0:
        return 0
    return min(overlap, max_chars // 3)


def _chunk_max_for_item(item: dict, default_max_chars: int) -> int:
    """
    Pick a chunk size based on evidence type.

    README/profile evidence can be a bit larger.
    Commit/contribution evidence is kept compact and usually remains one chunk.
    """
    ev_type = item.get("type")
    return TYPE_MAX_CHARS.get(ev_type, default_max_chars)


def chunk_text(
    text: str,
    max_chars: int = DEFAULT_MAX_CHARS,
    overlap: int = DEFAULT_OVERLAP,
) -> list[str]:
    """
    Split text into chunks no longer than max_chars.

    Strategy:
      1. Keep short text as one chunk.
      2. Split by paragraphs.
      3. Split oversized paragraphs by sentence.
      4. Hard split as fallback.

    Returns a list of strings. Empty input returns [].
    """
    text = _safe_content(text).strip()
    if not text:
        return []

    overlap = _normalise_overlap(max_chars, overlap)

    if len(text) <= max_chars:
        return [text]

    paragraphs = _split_paragraphs(text)
    chunks: list[str] = []
    current = ""

    for para in paragraphs:
        # Oversized paragraph: flush current buffer, then split this paragraph.
        if len(para) > max_chars:
            if current:
                chunks.append(current.strip())
                current = ""

            sentences = _split_sentences(para)
            sent_buf = ""

            for sent in sentences:
                if len(sent) > max_chars:
                    if sent_buf:
                        chunks.append(sent_buf.strip())
                        sent_buf = ""

                    step = max(1, max_chars - overlap)
                    for i in range(0, len(sent), step):
                        hard_chunk = sent[i : i + max_chars].strip()
                        if hard_chunk:
                            chunks.append(hard_chunk)
                    continue

                candidate = (sent_buf + " " + sent).strip() if sent_buf else sent
                if len(candidate) > max_chars:
                    if sent_buf:
                        chunks.append(sent_buf.strip())

                    tail = sent_buf[-overlap:].strip() if overlap and sent_buf else ""
                    sent_buf = (tail + " " + sent).strip() if tail else sent
                else:
                    sent_buf = candidate

            if sent_buf:
                chunks.append(sent_buf.strip())
            continue

        candidate = (current + "\n\n" + para).strip() if current else para
        if len(candidate) > max_chars:
            if current:
                chunks.append(current.strip())

            tail = current[-overlap:].strip() if overlap and current else ""
            current = (tail + "\n\n" + para).strip() if tail else para
        else:
            current = candidate

    if current:
        chunks.append(current.strip())

    return chunks


def chunk_evidence_item(
    item: dict,
    max_chars: int = DEFAULT_MAX_CHARS,
    overlap: int = DEFAULT_OVERLAP,
) -> list[dict]:
    """
    Produce one or more chunk dictionaries from a single evidence item.

    Each chunk preserves:
      - evidence_id
      - evidence type
      - source
      - original metadata
      - chunk index and total chunk count
    """
    evidence_id = item.get("evidence_id") or "ev_unknown"
    ev_type = item.get("type") or "unknown"
    source = item.get("source") or "unknown"
    metadata = dict(item.get("metadata") or {})

    content = _safe_content(item.get("content", "")).strip()
    if not content:
        return []

    effective_max_chars = _chunk_max_for_item(item, max_chars)

    # Unknown evidence types are still kept, but marked for debugging.
    if ev_type not in SUPPORTED_EVIDENCE_TYPES:
        metadata["unknown_evidence_type"] = ev_type

    parts = chunk_text(
        content,
        max_chars=effective_max_chars,
        overlap=overlap,
    )

    total = len(parts)
    chunks: list[dict] = []

    for i, part in enumerate(parts):
        chunk_metadata = {
            **metadata,
            "chunk_index": i,
            "total_chunks": total,
            "source_evidence_type": ev_type,
            "source_evidence_id": evidence_id,
        }

        # Helpful retrieval flags for the new v2 data gap fields.
        if ev_type in {"commit", "contribution"}:
            chunk_metadata["retrieval_hint"] = "github_contribution_signal"
        elif ev_type == "skill":
            chunk_metadata["retrieval_hint"] = "github_skill_signal"
        elif ev_type == "profile":
            chunk_metadata["retrieval_hint"] = "github_profile_signal"

        chunks.append({
            "chunk_id": f"{evidence_id}_c{i:02d}",
            "evidence_id": evidence_id,
            "chunk_index": i,
            "total_chunks": total,
            "type": ev_type,
            "source": source,
            "content": part,
            "metadata": chunk_metadata,
        })

    return chunks


def chunk_evidence_index(
    evidence_index: list[dict],
    max_chars: int = DEFAULT_MAX_CHARS,
    overlap: int = DEFAULT_OVERLAP,
) -> list[dict]:
    """
    Process the full evidence index, expanding each item into one or more chunks.

    Returns a flat list of chunks ready for embedding.
    """
    all_chunks: list[dict] = []
    multi_chunk_count = 0
    skipped_count = 0

    for item in evidence_index:
        chunks = chunk_evidence_item(
            item,
            max_chars=max_chars,
            overlap=overlap,
        )
        if not chunks:
            skipped_count += 1
            continue
        if len(chunks) > 1:
            multi_chunk_count += 1
        all_chunks.extend(chunks)

    logger.info(
        "Chunked %d evidence items → %d chunks (%d items split, %d skipped)",
        len(evidence_index),
        len(all_chunks),
        multi_chunk_count,
        skipped_count,
    )
    return all_chunks
