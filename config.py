"""
config.py
---------
Central configuration for the Skill Mining GitHub collector.
Reads from environment variables / .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the project root (two levels up from this file if needed)
load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("OUTPUT_DIR", BASE_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"
CACHE_DIR = BASE_DIR / ".cache"

for _d in (RAW_DIR, PROCESSED_DIR, LOGS_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── GitHub API ─────────────────────────────────────────────────────────────
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

# Maximum repositories to collect per user.
# Set high to capture historical repos; relevance scoring will rank them.
MAX_REPOS_PER_USER: int = int(os.getenv("MAX_REPOS_PER_USER", "200"))

# How many commits to sample per repository (spread across history)
COMMITS_PER_REPO: int = int(os.getenv("COMMITS_PER_REPO", "30"))

# README max characters to store as raw evidence
README_MAX_CHARS: int = int(os.getenv("README_MAX_CHARS", "4000"))

# ── Caching ────────────────────────────────────────────────────────────────
ENABLE_CACHE: bool = os.getenv("ENABLE_CACHE", "false").lower() == "true"
CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", str(60 * 60 * 24)))  # 1 day

# ── Relevance scoring weights ──────────────────────────────────────────────
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
SCORE_THRESHOLD: float = float(os.getenv("SCORE_THRESHOLD", "0.0"))  # 0 = include all

# ── Logging ────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
