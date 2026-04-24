# Skill Mining – GitHub Profile Data Collector & Preprocessor

A Python pipeline that collects public GitHub profile data, cleans and chunks
it into RAG-ready evidence, and produces structured JSON output for downstream
skill/role/leadership inference by LLM agents.

---

## Project structure

```
skill_mining/
├── config.py                        # All configuration (reads from .env)
├── github_client.py                 # Authenticated client, retry, rate-limit helpers
├── main.py                          # CLI entry point (typer)
├── collectors/
│   ├── profile_collector.py         # User profile metadata
│   └── repo_collector.py            # Repositories, commits, READMEs, scoring
├── transformers_local/
│   └── schema_builder.py            # Assembles final JSON schema
├── preprocessor/
│   ├── cleaner.py                   # Fix encoding, strip noise, drop empty items
│   ├── chunker.py                   # Split long evidence into ≤1500-char chunks
│   ├── historical.py                # Temporal analysis (commits/languages by year)
│   └── pipeline.py                  # Orchestrates clean → chunk → historical analysis
├── utils/
│   └── helpers.py                   # Shared utility functions
├── data/
│   ├── raw/                         # <username>.json (full evidence document)
│   ├── processed/                   # <username>_summary.json, master_summary.csv
│   └── preprocessed/                # <username>_preprocessed.json (RAG-ready chunks)
├── logs/                            # run_<timestamp>.json
├── usernames.txt                    # 10 individual GitHub profiles for experiments
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Clone / create the project

```bash
cd skill_mining
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure your GitHub token

```bash
cp .env.example .env
# Edit .env and set GITHUB_TOKEN=ghp_your_token_here

Never commit your `.env` file. Copy `.env.example` to `.env` and fill in your own GitHub Personal Access Token. The `.env` file is listed in `.gitignore` and will not be pushed to GitHub.
Generate one at: https://github.com/settings/tokens
```

A **classic personal access token** with **public repo read** scope is
sufficient. No write permissions are required.

Generate one at: https://github.com/settings/tokens

### 3. Verify setup

```bash
python main.py rate-limit
```

---

## Usage

### Collect a single profile

```bash
python main.py collect --username torvalds
```

### Collect multiple profiles from a file

```bash
python main.py collect --file usernames.txt
```

### Also generate Markdown summaries for manual inspection

```bash
python main.py collect --file usernames.txt --markdown
```

### Enable disk caching (avoids re-fetching during development)

```bash
ENABLE_CACHE=true python main.py collect --file usernames.txt
```

---

## Preprocessing

After collection, run the preprocessor to clean, chunk, and analyse the raw data.
This produces the `data/preprocessed/` files consumed by the retrieval (ChromaDB) step.

### Preprocess a single user

```bash
python main.py preprocess --username karpathy
```

### Preprocess all collected users at once

```bash
python main.py preprocess --all
```

### Adjust chunk size (default 1500 characters)

```bash
python main.py preprocess --all --chunk-size 1000
```

The preprocessor runs three steps internally:

| Step | Module | What it does |
|------|--------|--------------|
| Clean | `preprocessor/cleaner.py` | Fixes encoding corruption, strips HTML and badge lines, drops items < 20 chars |
| Chunk | `preprocessor/chunker.py` | Splits long README evidence into ≤1500-char overlapping segments |
| Historical analysis | `preprocessor/historical.py` | Derives commit trends, language evolution, and activity signals per year |

---

## Output files

| File | Description |
|------|-------------|
| `data/raw/<username>.json` | Full evidence document (all repos, commits, READMEs) |
| `data/processed/<username>_summary.json` | Lightweight summary (top 10 repos, aggregates) |
| `data/processed/<username>_summary.md` | Human-readable Markdown (with `--markdown`) |
| `data/processed/master_summary.csv` | One row per user, key signals |
| `data/preprocessed/<username>_preprocessed.json` | RAG-ready chunks + historical analysis |
| `logs/run_<timestamp>.json` | Run metadata, status, timestamps |

---

## Preprocessed JSON schema (`schema_version: 1.1`)

Produced by `python main.py preprocess`. This is the input file for the retrieval module.

```
{
  "schema_version": "1.1",
  "username": "karpathy",
  "preprocessed_at": "<ISO timestamp>",
  "source_file": "data/raw/karpathy.json",
  "historical_analysis": {
    "commits_by_year":     { "2014": 69, "2015": 127, ... },
    "languages_by_year":   { "2015": [{"language": "Lua", "bytes": ..., "pct": ...}], ... },
    "activity_trend":      "growing" | "stable" | "declining",
    "peak_activity_year":  2015,
    "tech_evolution": [
      { "period": "early (2012-2017)", "dominant_languages": ["Lua", "C", "C++"] },
      { "period": "recent (2021-2026)", "dominant_languages": ["Python", "Cuda"] },
      { "new_languages": ["Python", "Cuda"], "dropped_languages": ["Lua", "C++"] }
    ]
  },
  "chunks": [
    {
      "chunk_id":     "ev_0013_c00",
      "evidence_id":  "ev_0013",
      "chunk_index":  0,
      "total_chunks": 4,
      "type":         "skill" | "role" | "leadership" | "profile",
      "source":       "repo:<owner>/<repo>" | "profile:<login>",
      "content":      "...",
      "metadata":     { "repo": "...", "field": "...", "stars": ..., ... }
    }
  ],
  "stats": {
    "original_evidence_count": 559,
    "items_dropped":            2,
    "chunks_produced":          660,
    "avg_chunk_length_chars":   314
  }
}
```

### Chunk types

| Type | Content describes | Key metadata fields |
|------|-------------------|---------------------|
| `skill` | Technologies, README text, languages, commit messages, topics | `repo`, `field`, `language`, `stars`, `topics` |
| `role` | Whether user owns or forked a repo | `repo`, `is_owner`, `forked_from`, `stars`, `forks` |
| `leadership` | Star count, sustained contributions, multi-year activity | `repo`, `stars`, `forks` |
| `profile` | Bio or company affiliation | `field`, `login` |

### Loading chunks for embedding (Sushma's retrieval step)

```python
import json

doc    = json.load(open("data/preprocessed/karpathy_preprocessed.json", encoding="utf-8"))
texts  = [c["content"]   for c in doc["chunks"]]
ids    = [c["chunk_id"]  for c in doc["chunks"]]
metas  = [c["metadata"]  for c in doc["chunks"]]
```

---

## Output JSON schema

```
{
  "schema_version": "1.0",
  "profile": { ... },           // user metadata
  "repositories": [ ... ],      // per-repo evidence records, sorted by relevance
  "aggregate_signals": { ... }, // cross-repo computed features
  "evidence_index": [ ... ],    // flat list of citable snippets (for RAG chunking)
  "collection_metadata": { ... }
}
```

### Repository record structure

```json
{
  "repository_metadata": { ... },
  "relevance_score": 0.73,
  "skill_evidence": {
    "primary_language": "Python",
    "language_breakdown": { ... },
    "topics": [ ... ],
    "readme_keywords": [ ... ],
    "commit_years_covered": [2018, 2019, 2021, 2023]
  },
  "role_evidence": {
    "is_owner": true,
    "is_fork": false,
    "stars_received": 142,
    "forks_received": 31
  },
  "leadership_evidence": {
    "signals": ["Owner of repo with 142 stars", ...],
    "commit_year_span": { "from": 2018, "to": 2023 }
  },
  "raw_text_evidence": {
    "readme_excerpt": "...",
    "commit_samples": [ ... ],
    "description": "..."
  }
}
```

---

## Configuration reference (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_TOKEN` | *(required)* | GitHub PAT |
| `MAX_REPOS_PER_USER` | 200 | Max repos to fetch per user |
| `COMMITS_PER_REPO` | 30 | Commit samples per repo (spread across history) |
| `README_MAX_CHARS` | 4000 | Max README characters to store |
| `ENABLE_CACHE` | false | Cache API responses to disk |
| `SCORE_THRESHOLD` | 0.0 | Min relevance score (0 = keep all) |
| `LOG_LEVEL` | INFO | Logging verbosity |

---

## Design notes

### Historical coverage
- All repositories (up to `MAX_REPOS_PER_USER`) are retained - none are discarded.
- Commit samples are spread across the full repository lifetime, not just recent commits.
- An `age_bonus` scoring component boosts repos created >3 years ago to surface historical work.
- `aggregate_signals.historical_activity_summary` lists repos that are ≥4 years old.

### Evidence grounding
- Every repository produces structured `skill_evidence`, `role_evidence`, and `leadership_evidence` dicts.
- `evidence_index` provides a flat, citable list of evidence snippets ready for RAG chunking/embedding.
- README excerpts, commit messages, language stats, and topics are preserved as raw text.

### Relevance scoring
Transparent, configurable weighted formula (see `config.SCORING_WEIGHTS`):
- `stars` × 0.25
- `forks` × 0.20
- `size` × 0.10
- `is_owner` × 0.20
- `recency` × 0.15
- `age_bonus` × 0.10

All weights are adjustable in `.env` / `config.py`.
