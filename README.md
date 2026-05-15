# Skill Mining – GitHub Profile Data Collector & Preprocessor

A Python pipeline that collects public GitHub profile data, cleans and chunks
it into RAG-ready evidence, and produces structured JSON output for downstream
skill/role/leadership inference by LLM agents.

---

## Project structure

```text
skill_mining/
├── config.py                         # Central configuration and feature flags
├── github_client.py                  # GitHub client, retry, rate-limit helpers
├── main.py                           # CLI entry point
├── collectors/
│   ├── profile_collector.py          # Profile metadata, profile README, pinned repos, orgs
│   └── repo_collector.py             # Repositories, languages, commits, PRs/issues, tooling
├── transformers_local/
│   └── schema_builder.py             # Builds final raw JSON schema and evidence index
├── preprocessor/
│   ├── cleaner.py                    # Cleans evidence text and preserves metadata
│   ├── chunker.py                    # Chunks evidence for embedding/RAG
│   ├── historical.py                 # Yearly/monthly activity and tech evolution analysis
│   └── pipeline.py                   # Preprocessing orchestration
├── utils/
│   └── helpers.py                    # Shared utility functions
├── data/
│   ├── raw/                          # <username>_raw_v2.json
│   ├── processed/                    # <username>_processed_v2.json
│   └── preprocessed/                 # Optional file-based preprocessing output
├── logs/                             # Runtime logs
├── usernames.txt                     # Optional list of GitHub usernames
│   ├── profile_collector.py         # User profile metadata
│   └── repo_collector.py            # Repositories, commits, READMEs, scoring
├── data/
│   ├── agents/                      # AI agent pipeline for profile analysis
│   │   ├── agents.py                # LangChain agents (skill, role, summarizer)
│   │   ├── pipeline.py              # LangGraph orchestration
│   │   └── tools.py                 # GitHub API utilities
│   ├── raw/                         # <username>.json (full evidence document)
│   ├── processed/                   # <username>_summary.json, master_summary.csv
│   └── preprocessed/                # <username>_preprocessed.json (RAG-ready chunks)
├── transformers_local/
│   └── schema_builder.py            # Assembles final JSON schema
├── preprocessor/
│   ├── cleaner.py                   # Fix encoding, strip noise, drop empty items
│   ├── chunker.py                   # Split long evidence into ≤1500-char chunks
│   ├── historical.py                # Temporal analysis (commits/languages by year)
│   └── pipeline.py                  # Orchestrates clean → chunk → historical analysis
├── utils/
│   └── helpers.py                   # Shared utility functions
├── logs/                            # run_<timestamp>.json
├── usernames.txt                    # Individual GitHub profiles for experiments
├── requirements.txt
├── .env.example
└── README.md
```

> If your repository uses `transformers/` instead of `transformers_local/`, keep the folder name consistent with your imports. The updated `main.py` supports flexible schema-builder imports.

---

## Setup

### 1. Create and activate a virtual environment

```bash
cd skill_mining
python -m venv .venv
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Configure your API keys

```bash
cp .env.example .env
# Edit .env and set:
# GITHUB_TOKEN=ghp_your_github_token_here
# GROQ_API_KEY=your_groq_api_key_here

Never commit your `.env` file. Copy `.env.example` to `.env` and fill in your own API keys. The `.env` file is listed in `.gitignore` and will not be pushed to GitHub.
```

#### GitHub Token
Generate a **classic personal access token** with **public repo read** scope at: https://github.com/settings/tokens

#### Groq API Key
Get your API key for AI agent analysis at: https://console.groq.com/keys

### 3. Verify setup

```bash
python main.py rate-limit
```

A classic GitHub Personal Access Token with public read access is enough for public profile/repository collection. Do not commit your `.env` file.

The pipeline can run without a token, but the GitHub API rate limit will be much lower and some GraphQL-based features such as pinned repositories may not work reliably.

---

## Usage

### Collect usernames from `usernames.txt`

```bash
python main.py
```

### Collect one or more specific profiles

```bash
python main.py torvalds octocat
```

The updated `main.py` writes both raw and processed v2 outputs.

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

## AI Agent Analysis

Analyze GitHub profiles using AI agents to extract skills, roles, and professional summaries.

### Analyze a single profile

```bash
python main.py analyze --username karpathy
```

### What the agents do

The AI agent pipeline consists of three specialized agents:

| Agent | Purpose | Output |
|-------|---------|--------|
| **Skill Extractor** | Analyzes repositories and languages | Technical skills with confidence scores and evidence |
| **Role Analyzer** | Examines ownership patterns | Developer role (creator, contributor, maintainer, learner) |
| **Summarizer** | Combines all data | Professional profile summary |

### Sample output

```
DEVELOPER PROFILE

SKILLS:
1. Python
   Confidence: 0.9
   Justification: Used in majority of repositories
   Evidence: Repository descriptions and language statistics

ROLE:
Maintainer - Created 9 out of 10 repositories with high community engagement

SUMMARY:
Professional summary highlighting expertise, experience, and achievements...
```

---

## Output files

| File | Description |
|---|---|
| `data/raw/<username>_raw_v2.json` | Full evidence-preserving document with profile, repositories, aggregate signals, evidence index, and historical analysis |
| `data/processed/<username>_processed_v2.json` | Cleaned and chunked output for downstream RAG/agent use |
| `data/preprocessed/<username>_preprocessed.json` | Optional output when using file-based preprocessing directly |
| `logs/` | Runtime logs |

---

## Raw v2 JSON schema
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
  "schema_version": "2.0",
  "profile": {},
  "repositories": [],
  "aggregate_signals": {},
  "historical_analysis": {},
  "evidence_index": [],
  "collection_metadata": {}
}
```

---

## Profile fields

The profile collector keeps the original GitHub profile metadata and adds richer v2 context:

```json
{
  "login": "...",
  "name": "...",
  "bio": "...",
  "company": "...",
  "location": "...",
  "followers": 0,
  "following": 0,
  "public_repos": 0,
  "hireable": null,
  "years_on_github": 0,
  "followers_to_following_ratio": 0.0,
  "profile_readme": "...",
  "profile_readme_content": "...",
  "pinned_repositories": [],
  "organisations": [],
  "organisations_member_of": [],
  "is_sponsored": false,
  "sponsoring_count": 0
}
```

---

## Repository record structure

Each repository contains metadata, skill evidence, role evidence, leadership evidence, and raw text evidence.

```json
{
  "repository_metadata": {
    "name": "...",
    "full_name": "...",
    "description": "...",
    "language": "Python",
    "topics": [],
    "stargazers_count": 0,
    "forks_count": 0,
    "created_at": "...",
    "pushed_at": "...",
    "has_ci_cd": false,
    "has_tests": false,
    "has_contributing_guide": false,
    "has_code_of_conduct": false,
    "dependency_count": 0,
    "releases_count": 0,
    "latest_release_tag": null,
    "latest_release_date": null,
    "contributor_count": 0
  },
  "relevance_score": 0.73,
  "skill_evidence": {
    "primary_language": "Python",
    "languages_used": [],
    "language_breakdown": {},
    "topics": [],
    "readme_keywords": [],
    "commit_count_sampled": 30,
    "total_commit_count": 120,
    "total_commits_to_repo": 120,
    "commit_frequency_per_year": {},
    "avg_commits_per_active_month": 4.2,
    "last_commit_date": "...",
    "total_lines_added": 0,
    "total_lines_deleted": 0,
    "commit_size_distribution": {
      "small": 0,
      "medium": 0,
      "large": 0
    },
    "commit_message_quality_score": 0.0,
    "contribution_gap_months": 0,
    "frameworks_detected": [],
    "devops_tools_detected": [],
    "testing_frameworks_detected": [],
    "ci_platforms_detected": [],
    "cloud_platforms_detected": [],
    "db_technologies_detected": [],
    "api_patterns_detected": [],
    "documentation_quality_score": 0
  },
  "role_evidence": {
    "is_owner": true,
    "is_fork": false,
    "forked_from": null,
    "stars_received": 0,
    "forks_received": 0,
    "issues_opened_count": 0,
    "closed_issues_count": 0,
    "pr_open_count": 0,
    "pr_closed_count": 0,
    "pr_merged_count": 0,
    "pr_merge_rate_pct": null,
    "avg_issue_close_time_days": null,
    "contributor_count": 0
  },
  "leadership_evidence": {
    "signals": [],
    "commit_year_span": {}
  },
  "raw_text_evidence": {
    "readme_excerpt": "...",
    "commit_samples": [],
    "description": "..."
  }
}
```

---

## Aggregate signals

The v2 aggregate layer includes both classic profile-wide summaries and richer data-gap fields:

```json
{
  "top_languages": [],
  "top_topics": [],
  "total_stars_received": 0,
  "total_forks_received": 0,
  "total_repos_collected": 0,
  "owned_repo_count": 0,
  "forked_repo_count": 0,
  "active_years": [],
  "first_active_year": null,
  "last_active_year": null,
  "activity_span_years": 0,
  "monthly_commit_heatmap": {},
  "recent_6_month_commit_count": 0,
  "total_commits_all_repos": 0,
  "total_lines_added_all_repos": 0,
  "total_lines_deleted_all_repos": 0,
  "total_prs_opened": 0,
  "total_prs_closed": 0,
  "total_prs_merged": 0,
  "total_issues_opened": 0,
  "total_issues_closed": 0,
  "external_contributions_count": 0,
  "longest_contribution_streak_months": 0,
  "most_active_year": null,
  "inactive_periods": [],
  "collection_completeness_pct": null,
  "repos_with_ci_cd": 0,
  "repos_with_tests": 0,
  "top_frameworks_detected": [],
  "top_devops_tools_detected": [],
  "top_cloud_platforms_detected": [],
  "top_database_technologies_detected": []
}
```

---

## Historical analysis

The historical preprocessor now supports both year-level and month-level signals:

```json
{
  "commits_by_year": {},
  "languages_by_year": {},
  "activity_trend": "stable",
  "peak_activity_year": null,
  "tech_evolution": [],
  "monthly_commit_heatmap": {},
  "weekday_vs_weekend_ratio": null,
  "most_active_month_of_year": null,
  "inactive_periods": [],
  "recent_6_month_commit_count": 0,
  "repo_creation_cadence": {}
}
```

---

## Evidence index and chunks

The evidence index is a flat list of citable evidence snippets. It supports these evidence types:

```text
profile
skill
role
leadership
commit
contribution
```

After preprocessing, evidence items are cleaned and split into chunks while preserving:

```json
{
  "chunk_id": "ev_0001_c00",
  "evidence_id": "ev_0001",
  "chunk_index": 0,
  "total_chunks": 1,
  "type": "skill",
  "source": "repo:owner/name",
  "content": "...",
  "metadata": {}
}
```

---

## Configuration reference

Important `.env` variables:

| Variable | Default | Description |
|---|---:|---|
| `GITHUB_TOKEN` | empty | GitHub token. Strongly recommended for higher rate limits and GraphQL features |
| `MAX_REPOS_PER_USER` | `200` | Maximum public repositories to collect per user |
| `COMMITS_PER_REPO` | `30` | Number of representative commit samples stored per repo |
| `MAX_COMMITS_TO_SCAN_PER_REPO` | `500` | Maximum commits scanned per repo for activity statistics |
| `README_MAX_CHARS` | `4000` | README excerpt length |
| `ENABLE_CACHE` | `false` | Enable disk cache |
| `CACHE_TTL_SECONDS` | `86400` | Cache lifetime |
| `SCORE_THRESHOLD` | `0.0` | Minimum relevance score; `0.0` keeps all collected repos |
| `LOG_LEVEL` | `INFO` | Logging level |
| `ENABLE_COLLABORATION_COLLECTION` | `true` | Collect PR/issue stats |
| `ENABLE_RELEASE_COLLECTION` | `true` | Collect release info |
| `ENABLE_ENGINEERING_MATURITY_COLLECTION` | `true` | Collect CI/test/docs/dependency signals |
| `ENABLE_TOOLING_DETECTION` | `true` | Detect frameworks, DevOps, cloud, DB, API signals |
| `ENABLE_PROFILE_README` | `true` | Collect profile README |
| `ENABLE_PINNED_REPOSITORIES` | `true` | Collect pinned repos through GraphQL |
| `ENABLE_ORGANISATION_COLLECTION` | `true` | Collect public org memberships |
| `ENABLE_SPONSORSHIP_COLLECTION` | `true` | Collect sponsorship signals |
| `ENABLE_HISTORICAL_MONTHLY_ANALYSIS` | `true` | Produce month-level historical analysis |
| `INACTIVE_GAP_THRESHOLD_MONTHS` | `3` | Minimum inactive gap length to report |
| `RECENT_ACTIVITY_MONTHS` | `6` | Recent activity window |

---

## Design notes

### Historical coverage

- All public repositories are retained up to `MAX_REPOS_PER_USER`.
- Commit samples are spread across repository history.
- Total commit count is collected separately from sampled commit messages.
- Monthly heatmaps and inactive periods help avoid relying only on recent activity.

### Evidence grounding

- Every important claim should be traceable to profile, repository, commit, contribution, or leadership evidence.
- The `evidence_index` is designed for downstream chunking and embeddings.
- Metadata is preserved so RAG outputs can cite repository names, commit dates, PR/issue counts, and detected tools.

### Collaboration and role signals

The pipeline captures ownership, forks, PR counts, merge rate, issue closure signals, contributor count, stars, forks, releases, and engineering maturity signals.

### Relevance scoring

Repository ranking uses a transparent weighted formula from `config.SCORING_WEIGHTS`:

| Signal | Default weight |
|---|---:|
| Stars | 0.25 |
| Forks | 0.20 |
| Size | 0.10 |
| Ownership | 0.20 |
| Recency | 0.15 |
| Age bonus | 0.10 |

---

## Recommended run checklist

Before running:

```bash
python -m py_compile config.py github_client.py main.py collectors/profile_collector.py collectors/repo_collector.py preprocessor/*.py utils/helpers.py
```

Then run:

```bash
python main.py <github_username>
```

Check outputs:

```text
data/raw/<username>_raw_v2.json
data/processed/<username>_processed_v2.json
```
