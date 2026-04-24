"""
preprocessor/chunker.py
-----------------------
Splits long evidence items into overlapping text chunks suitable for embedding
with text-embedding-ada-002 (8191 token limit, ~4 chars/token).

Target chunk size is 1500 chars (~375 tokens), giving plenty of headroom while
keeping each chunk semantically focused.

Split strategy (applied in priority order):
  1. Content ≤ max_chars  → single chunk, no splitting
  2. Split on paragraph boundaries (\\n\\n)
  3. Paragraphs still > max_chars → split on sentence boundaries
  4. Hard split as last resort

Each output chunk carries the original evidence_id, type, source, and metadata,
plus chunk_index and total_chunks for traceability by downstream retrieval.
"""

import re
from utils.helpers import get_logger

logger = get_logger(__name__)

DEFAULT_MAX_CHARS = 1500
DEFAULT_OVERLAP = 150


def _split_paragraphs(text: str) -> list[str]:
    return [p.strip() for p in re.split(r"\n\n+", text) if p.strip()]


def _split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def chunk_text(
    text: str,
    max_chars: int = DEFAULT_MAX_CHARS,
    overlap: int = DEFAULT_OVERLAP,
) -> list[str]:
    """
    Split text into overlapping chunks no longer than max_chars.
    Returns a list of strings (always at least one element).
    """
    if len(text) <= max_chars:
        return [text]

    paragraphs = _split_paragraphs(text)
    chunks: list[str] = []
    current = ""

    for para in paragraphs:
        if len(para) > max_chars:
            if current:
                chunks.append(current.strip())
                current = ""
            # Break oversized paragraph by sentences
            sentences = _split_sentences(para)
            sent_buf = ""
            for sent in sentences:
                if len(sent_buf) + len(sent) + 1 > max_chars:
                    if sent_buf:
                        chunks.append(sent_buf.strip())
                    # Hard-split sentences that are themselves too long
                    if len(sent) > max_chars:
                        for i in range(0, len(sent), max_chars - overlap):
                            chunks.append(sent[i : i + max_chars])
                        sent_buf = ""
                    else:
                        sent_buf = sent
                else:
                    sent_buf = (sent_buf + " " + sent).strip() if sent_buf else sent
            if sent_buf:
                chunks.append(sent_buf.strip())
            continue

        if len(current) + len(para) + 2 > max_chars:
            if current:
                chunks.append(current.strip())
            # Carry the tail of the previous chunk for overlap continuity
            tail = current[-overlap:].strip() if overlap and current else ""
            current = (tail + "\n\n" + para).strip() if tail else para
        else:
            current = (current + "\n\n" + para).strip() if current else para

    if current:
        chunks.append(current.strip())

    return chunks if chunks else [text[:max_chars]]


def chunk_evidence_item(
    item: dict,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> list[dict]:
    """
    Produce one or more chunk dicts from a single evidence item.
    Short items become a single chunk; long items are split with overlap.
    """
    content = item.get("content", "")
    parts = chunk_text(content, max_chars=max_chars)
    total = len(parts)
    chunks = []
    for i, part in enumerate(parts):
        chunks.append({
            "chunk_id": f"{item['evidence_id']}_c{i:02d}",
            "evidence_id": item["evidence_id"],
            "chunk_index": i,
            "total_chunks": total,
            "type": item.get("type"),
            "source": item.get("source"),
            "content": part,
            "metadata": {
                **item.get("metadata", {}),
                "chunk_index": i,
                "total_chunks": total,
            },
        })
    return chunks


def chunk_evidence_index(
    evidence_index: list[dict],
    max_chars: int = DEFAULT_MAX_CHARS,
) -> list[dict]:
    """
    Process the full evidence index, expanding each item into one or more chunks.
    Returns the flat list of all chunks ready for embedding.
    """
    all_chunks: list[dict] = []
    multi_chunk_count = 0
    for item in evidence_index:
        chunks = chunk_evidence_item(item, max_chars=max_chars)
        if len(chunks) > 1:
            multi_chunk_count += 1
        all_chunks.extend(chunks)
    logger.info(
        "Chunked %d evidence items → %d chunks (%d items were split)",
        len(evidence_index),
        len(all_chunks),
        multi_chunk_count,
    )
    return all_chunks
