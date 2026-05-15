"""
config.py
---------
Central configuration for the Skill Mining GitHub collector.
Reads from environment variables / .env file.

v2 additions
------------
Adds safe feature flags and API limits for the richer collector pipeline:
- deeper commit scanning
- collaboration collection through PRs/issues
- releases, CI/CD, tests, dependencies
- profile README, pinned repos, organisations, sponsorship signals
- historical/month-level activity analysis
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the project root
load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Small parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _env_bool(name: str, default: bool = False) -> bool:
    """Read a boolean value from environment variables."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    """Read an integer value from environment variables with safe fallback."""
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    """Read a float value from environment variables with safe fallback."""
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("OUTPUT_DIR", BASE_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"
CACHE_DIR = BASE_DIR / ".cache"

for _d in (RAW_DIR, PROCESSED_DIR, LOGS_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# GitHub API
# ─────────────────────────────────────────────────────────────────────────────
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

# Maximum repositories to collect per user.
# Set high to capture historical repos; relevance scoring will rank them.
MAX_REPOS_PER_USER: int = _env_int("MAX_REPOS_PER_USER", 200)

# How many commits to keep as readable examples per repository.
# These are NOT the same as the scan limit. They are stored as raw text evidence.
COMMITS_PER_REPO: int = _env_int("COMMITS_PER_REPO", 30)

# Maximum commits to scan per repository when deriving commit-depth statistics.
# This protects API usage while still giving better historical coverage than a tiny sample.
MAX_COMMITS_TO_SCAN_PER_REPO: int = _env_int("MAX_COMMITS_TO_SCAN_PER_REPO", 500)

# README max characters to store as raw evidence.
README_MAX_CHARS: int = _env_int("README_MAX_CHARS", 4000)

# Profile README max characters to store.
PROFILE_README_MAX_CHARS: int = _env_int("PROFILE_README_MAX_CHARS", 5000)


# ─────────────────────────────────────────────────────────────────────────────
# Feature flags for richer evidence collection
# ─────────────────────────────────────────────────────────────────────────────
# Keep these configurable because GitHub API rate limits can become a bottleneck.
ENABLE_COLLABORATION_COLLECTION: bool = _env_bool("ENABLE_COLLABORATION_COLLECTION", True)
ENABLE_RELEASE_COLLECTION: bool = _env_bool("ENABLE_RELEASE_COLLECTION", True)
ENABLE_ENGINEERING_MATURITY_COLLECTION: bool = _env_bool(
    "ENABLE_ENGINEERING_MATURITY_COLLECTION", True
)
ENABLE_CI_TEST_DETECTION: bool = _env_bool("ENABLE_CI_TEST_DETECTION", True)
ENABLE_TOOLING_DETECTION: bool = _env_bool("ENABLE_TOOLING_DETECTION", True)
ENABLE_PROFILE_README: bool = _env_bool("ENABLE_PROFILE_README", True)
ENABLE_PINNED_REPOSITORIES: bool = _env_bool("ENABLE_PINNED_REPOSITORIES", True)
ENABLE_ORGANISATION_COLLECTION: bool = _env_bool("ENABLE_ORGANISATION_COLLECTION", True)
ENABLE_SPONSORSHIP_COLLECTION: bool = _env_bool("ENABLE_SPONSORSHIP_COLLECTION", True)
ENABLE_HISTORICAL_MONTHLY_ANALYSIS: bool = _env_bool(
    "ENABLE_HISTORICAL_MONTHLY_ANALYSIS", True
)


# ─────────────────────────────────────────────────────────────────────────────
# API safety limits
# ─────────────────────────────────────────────────────────────────────────────
# Number of recently closed issues to inspect for average close-time calculation.
ISSUE_CLOSE_TIME_SAMPLE_SIZE: int = _env_int("ISSUE_CLOSE_TIME_SAMPLE_SIZE", 30)

# Number of pinned repositories GitHub allows in the UI is usually up to 6.
PINNED_REPOS_LIMIT: int = _env_int("PINNED_REPOS_LIMIT", 6)

# GraphQL timeout in seconds.
GRAPHQL_TIMEOUT_SECONDS: int = _env_int("GRAPHQL_TIMEOUT_SECONDS", 15)

# REST request timeout in seconds.
REST_TIMEOUT_SECONDS: int = _env_int("REST_TIMEOUT_SECONDS", 15)

# Used by historical analysis to flag inactivity.
INACTIVE_GAP_THRESHOLD_MONTHS: int = _env_int("INACTIVE_GAP_THRESHOLD_MONTHS", 3)

# Recent activity window for aggregate signals.
RECENT_ACTIVITY_MONTHS: int = _env_int("RECENT_ACTIVITY_MONTHS", 6)


# ─────────────────────────────────────────────────────────────────────────────
# Caching
# ─────────────────────────────────────────────────────────────────────────────
ENABLE_CACHE: bool = _env_bool("ENABLE_CACHE", False)
CACHE_TTL_SECONDS: int = _env_int("CACHE_TTL_SECONDS", 60 * 60 * 24)  # 1 day


# ─────────────────────────────────────────────────────────────────────────────
# Relevance scoring weights
# ─────────────────────────────────────────────────────────────────────────────
# These weights are applied during repository scoring. Adjust to change ranking.
SCORING_WEIGHTS: dict = {
    "stars": 0.25,
    "forks": 0.20,
    "size": 0.10,
    "is_owner": 0.20,       # user owns the repo (not a fork)
    "recency": 0.15,        # how recently the repo was pushed to
    "age_bonus": 0.10,      # older repos get a small bonus for historical coverage
}

# Minimum score threshold to include a repo in full detail.
# Repos below this still appear but with reduced evidence fields.
SCORE_THRESHOLD: float = _env_float("SCORE_THRESHOLD", 0.0)  # 0 = include all


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
