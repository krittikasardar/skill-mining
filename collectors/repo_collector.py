"""
collectors/repo_collector.py :

Collects all public repositories for a GitHub user, including:
  - Repository metadata
  - Language breakdown
  - README content (raw evidence)
  - Commit samples spanning historical and recent periods
  - Role / ownership signals
  - Issue / PR / fork signals
  - Relevance scoring for downstream prioritisation
"""

import base64
import math
from datetime import datetime, timezone
from typing import Optional

from github import Github, Repository, GithubException

import config
from github_client import with_retry, wait_for_rate_limit
from utils.helpers import (
    get_logger,
    datetime_to_iso,
    safe_get,
    extract_readme_excerpt,
    years_since,
    iso_to_year,
)

logger = get_logger(__name__)

# Relevance Scoring

def score_repository(repo_meta: dict, owner_login: str) -> float:
    """
    Compute a relevance score in [0, 1] for a repository.

    The score is a weighted combination of normalised signals.
    All weights are configurable via config.SCORING_WEIGHTS.

    Signals
    -------
    stars      : log-normalised stargazer count
    forks      : log-normalised fork count
    size       : log-normalised repo size in KB
    is_owner   : 1.0 if user owns the repo (not a fork), else 0.0
    recency    : 1.0 if pushed recently, decays exponentially over 5 years
    age_bonus  : small bonus for repos created > 3 years ago (historical value)
    """
    w = config.SCORING_WEIGHTS

    def log_norm(value: int, scale: float = 100.0) -> float:
        return math.log1p(max(0, value)) / math.log1p(scale)

    stars_score = min(log_norm(repo_meta.get("stargazers_count", 0), 1000), 1.0)
    forks_score = min(log_norm(repo_meta.get("forks_count", 0), 200), 1.0)
    size_score = min(log_norm(repo_meta.get("size", 0), 50000), 1.0)

    is_owner_score = 1.0 if not repo_meta.get("fork", True) else 0.0

    # Recency: exponential decay; half-life ~2 years
    pushed_at = repo_meta.get("pushed_at")
    age_years = years_since(pushed_at) if pushed_at else 5.0
    recency_score = math.exp(-0.35 * age_years)

    # Age bonus: repos older than 3 years get a small historical importance boost
    created_at = repo_meta.get("created_at")
    repo_age_years = years_since(created_at) if created_at else 0.0
    age_bonus_score = min(repo_age_years / 10.0, 1.0) if repo_age_years > 3 else 0.0

    score = (
        w.get("stars", 0.25) * stars_score
        + w.get("forks", 0.20) * forks_score
        + w.get("size", 0.10) * size_score
        + w.get("is_owner", 0.20) * is_owner_score
        + w.get("recency", 0.15) * recency_score
        + w.get("age_bonus", 0.10) * age_bonus_score
    )
    return round(min(score, 1.0), 4)

# README collection

def _fetch_readme(repo: Repository) -> tuple[str, str]:
    """
    Return (raw_text, excerpt) for the repository README.
    Returns ('', '') if the README cannot be fetched.
    """
    try:
        readme_content = with_retry(
            lambda: repo.get_readme(),
            label=f"readme({repo.full_name})",
        )
        raw = base64.b64decode(readme_content.content).decode("utf-8", errors="replace")
        excerpt = extract_readme_excerpt(raw, config.README_MAX_CHARS)
        return raw, excerpt
    except GithubException:
        return "", ""

# Commit sampling

def _sample_commits(repo: Repository, author_login: str, n: int = 30) -> list[dict]:
    """
    Sample up to `n` commits by the author, spread across the repository's lifetime.

    Strategy:
    1. Collect paginated commits by author (up to 200).
    2. Sort by date ascending.
    3. Pick n evenly-spaced entries to cover history breadth.

    Each commit entry includes: sha, message snippet, date, additions, deletions.
    """
    try:
        commits_pager = with_retry(
            lambda: repo.get_commits(author=author_login),
            label=f"commits({repo.full_name})",
        )
        all_commits = []
        for i, commit in enumerate(commits_pager):
            if i >= 200:  # cap at 200 to control API cost
                break
            commit_date = datetime_to_iso(safe_get(commit.commit.author, "date"))
            stats = safe_get(commit, "stats")
            all_commits.append({
                "sha": commit.sha[:10],
                "message": truncate_msg(safe_get(commit.commit, "message", "")),
                "date": commit_date,
                "additions": safe_get(stats, "additions", 0) if stats else 0,
                "deletions": safe_get(stats, "deletions", 0) if stats else 0,
                "url": safe_get(commit, "html_url"),
            })

        if not all_commits:
            return []

        # Sort chronologically
        all_commits.sort(key=lambda c: c["date"] or "")

        # Evenly sample n commits across the full history
        if len(all_commits) <= n:
            return all_commits
        step = len(all_commits) / n
        return [all_commits[int(i * step)] for i in range(n)]

    except GithubException as exc:
        logger.warning("Could not fetch commits for %s: %s", repo.full_name, exc)
        return []


def truncate_msg(msg: str, max_chars: int = 200) -> str:
    msg = msg.strip().splitlines()[0] if msg.strip() else ""
    return msg[:max_chars] + ("…" if len(msg) > max_chars else "")

# Language breakdown

def _fetch_languages(repo: Repository) -> dict:
    """Return language -> byte count mapping using direct REST call."""
    try:
        import requests
        response = requests.get(
            f"https://api.github.com/repos/{repo.full_name}/languages",
            headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
            timeout=10,
        )
        if response.status_code == 200:
            return {lang: int(val) for lang, val in response.json().items()}
        return {}
    except Exception:
        return {}

# Topics

def _fetch_topics(repo: Repository) -> list[str]:
    try:
        return with_retry(lambda: repo.get_topics(), label=f"topics({repo.full_name})")
    except GithubException:
        return []

# Single repository collector

def collect_single_repo(repo: Repository, owner_login: str) -> dict:
    """
    Collect full evidence-rich data for a single repository.

    Returns a dict matching the `repositories[i]` section of the output schema.
    """
    logger.debug("  Collecting repo: %s", repo.full_name)

    # Repository metadata 
    meta = {
        "name": safe_get(repo, "name"),
        "full_name": safe_get(repo, "full_name"),
        "description": safe_get(repo, "description"),
        "html_url": safe_get(repo, "html_url"),
        "homepage": safe_get(repo, "homepage"),
        "language": safe_get(repo, "language"),
        "stargazers_count": safe_get(repo, "stargazers_count", 0),
        "forks_count": safe_get(repo, "forks_count", 0),
        "watchers_count": safe_get(repo, "watchers_count", 0),
        "open_issues_count": safe_get(repo, "open_issues_count", 0),
        "size": safe_get(repo, "size", 0),
        "default_branch": safe_get(repo, "default_branch"),
        "fork": safe_get(repo, "fork", False),
        "archived": safe_get(repo, "archived", False),
        "visibility": safe_get(repo, "visibility", "public"),
        "created_at": datetime_to_iso(safe_get(repo, "created_at")),
        "updated_at": datetime_to_iso(safe_get(repo, "updated_at")),
        "pushed_at": datetime_to_iso(safe_get(repo, "pushed_at")),
        "license": safe_get(safe_get(repo, "license"), "name") if safe_get(repo, "license") else None,
        "topics": _fetch_topics(repo),
    }

    # Languages 
    languages = _fetch_languages(repo)
    total_bytes = sum(languages.values()) or 1
    language_breakdown = {
        lang: {"bytes": b, "pct": round(b / total_bytes * 100, 1)}
        for lang, b in sorted(languages.items(), key=lambda x: -x[1])
    }

    # README
    readme_raw, readme_excerpt = _fetch_readme(repo)

    # Commit samples 
    commits = _sample_commits(repo, owner_login, n=config.COMMITS_PER_REPO)
    commit_years = sorted(set(
        iso_to_year(c["date"]) for c in commits if c.get("date")
    ))

    # Role evidence
    is_owner = not meta["fork"]
    role_evidence = {
        "is_owner": is_owner,
        "is_fork": meta["fork"],
        # Parent repo name if this is a fork (evidence for contributor role)
        "forked_from": safe_get(safe_get(repo, "parent"), "full_name") if meta["fork"] else None,
        "has_wiki": safe_get(repo, "has_wiki", False),
        "has_issues": safe_get(repo, "has_issues", False),
        "has_projects": safe_get(repo, "has_projects", False),
        "has_pages": safe_get(repo, "has_pages", False),
        # Stars and forks received are signals others found the repo valuable
        "stars_received": meta["stargazers_count"],
        "forks_received": meta["forks_count"],
    }

    # Leadership evidence 
    # Proxy signals: owned repo with multiple forks/stars; wiki; GitHub Pages
    leadership_signals = []
    if is_owner and meta["stargazers_count"] >= 10:
        leadership_signals.append(f"Owner of repo with {meta['stargazers_count']} stars")
    if is_owner and meta["forks_count"] >= 5:
        leadership_signals.append(f"Repo forked {meta['forks_count']} times by others")
    if meta.get("has_pages"):
        leadership_signals.append("Maintains GitHub Pages (documentation/project site)")
    if meta.get("open_issues_count", 0) > 10:
        leadership_signals.append(
            f"Active issue tracker with {meta['open_issues_count']} open issues"
        )
    if commits:
        span_years = (max(commit_years) - min(commit_years)) if len(commit_years) > 1 else 0
        if span_years >= 2:
            leadership_signals.append(
                f"Sustained contributions over {span_years}+ years ({min(commit_years)}–{max(commit_years)})"
            )

    leadership_evidence = {
        "signals": leadership_signals,
        "commit_year_span": (
            {"from": min(commit_years), "to": max(commit_years)}
            if commit_years else {}
        ),
    }

    # Skill evidence 
    skill_evidence = {
        "primary_language": meta["language"],
        "languages_used": list(language_breakdown.keys()),
        "language_breakdown": language_breakdown,
        "topics": meta["topics"],
        "readme_keywords": _extract_tech_keywords(readme_raw),
        "commit_count_sampled": len(commits),
        "commit_years_covered": commit_years,
    }

    # Relevance score
    relevance_score = score_repository(meta, owner_login)

    return {
        "repository_metadata": meta,
        "relevance_score": relevance_score,
        "skill_evidence": skill_evidence,
        "role_evidence": role_evidence,
        "leadership_evidence": leadership_evidence,
        "raw_text_evidence": {
            "readme_excerpt": readme_excerpt,
            "commit_samples": commits,
            "description": meta.get("description") or "",
        },
    }

# All-repositories collector

def collect_all_repos(client: Github, username: str) -> list[dict]:
    """
    Collect all public repositories for `username`.

    Repositories are:
    1. Fetched (up to MAX_REPOS_PER_USER).
    2. Scored for relevance.
    3. Sorted: highest-scoring first, but ALL are retained so historical
       repos are never silently dropped.

    Parameters
    ----------
    client   : authenticated PyGitHub instance
    username : GitHub login

    Returns
    -------
    List of repository dicts sorted by relevance_score descending.
    """
    logger.info("Collecting repositories for: %s", username)

    user = with_retry(lambda: client.get_user(username), label=f"get_user({username})")

    # Fetch all public repos (including forks) sorted by update time first
    repos_pager = with_retry(
        lambda: user.get_repos(type="public", sort="updated"),
        label=f"get_repos({username})",
    )

    collected = []
    for i, repo in enumerate(repos_pager):
        if i >= config.MAX_REPOS_PER_USER:
            logger.info("Reached MAX_REPOS_PER_USER (%d) for %s", config.MAX_REPOS_PER_USER, username)
            break

        # Check rate limit headroom every 10 repos
        if i > 0 and i % 10 == 0:
            wait_for_rate_limit(client, buffer=20)
            logger.info("  Progress: %d repositories collected for %s", i, username)

        try:
            repo_data = collect_single_repo(repo, username)
            collected.append(repo_data)
        except Exception as exc:
            logger.warning("Failed to collect repo %s: %s", repo.full_name, exc)

    # Sort by relevance score descending; all repos remain in the output
    collected.sort(key=lambda r: r.get("relevance_score", 0), reverse=True)
    logger.info("Collected %d repositories for %s", len(collected), username)
    return collected

# Tech keyword extraction (lightweight, no ML)

_TECH_KEYWORDS = {
    "python", "javascript", "typescript", "java", "go", "rust", "c++", "c#",
    "swift", "kotlin", "ruby", "php", "scala", "elixir", "clojure", "haskell",
    "react", "vue", "angular", "svelte", "next.js", "nuxt", "django", "flask",
    "fastapi", "rails", "spring", "express", "graphql", "rest", "grpc",
    "docker", "kubernetes", "terraform", "ansible", "ci/cd", "github actions",
    "aws", "gcp", "azure", "cloud", "serverless", "lambda",
    "machine learning", "deep learning", "neural", "transformer", "llm",
    "pytorch", "tensorflow", "scikit", "pandas", "numpy",
    "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "kafka",
    "microservices", "api", "cli", "sdk", "library", "framework",
    "open source", "testing", "tdd", "bdd", "devops", "mlops",
}

def _extract_tech_keywords(text: str) -> list[str]:
    """Return tech keywords found in text (case-insensitive)."""
    if not text:
        return []
    lower = text.lower()
    return sorted(kw for kw in _TECH_KEYWORDS if kw in lower)
