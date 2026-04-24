# Skill Mining – GitHub Profile Data Collector

A production-grade Python pipeline that collects public GitHub profile data
and produces evidence-preserving JSON output for downstream RAG-based
skill/role/leadership inference.

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
├── transformers/
│   └── schema_builder.py            # Assembles final JSON schema
├── utils/
│   └── helpers.py                   # Shared utility functions
├── data/
│   ├── raw/                         # <username>.json (full evidence document)
│   └── processed/                   # <username>_summary.json, master_summary.csv
├── logs/                            # run_<timestamp>.json
├── usernames.txt                    # Sample list of 10 GitHub usernames
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

## Output files

| File | Description |
|------|-------------|
| `data/raw/<username>.json` | Full evidence document (all repos, commits, READMEs) |
| `data/processed/<username>_summary.json` | Lightweight summary (top 10 repos, aggregates) |
| `data/processed/<username>_summary.md` | Human-readable Markdown (with `--markdown`) |
| `data/processed/master_summary.csv` | One row per user, key signals |
| `logs/run_<timestamp>.json` | Run metadata, status, timestamps |

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
