"""
collectors/repo_collector.py
-----------------------------
Collects all public repositories for a GitHub user, including:
  - Repository metadata
  - Language breakdown
  - README content (raw evidence)
  - Commit samples spanning historical and recent periods
  - Role / ownership signals
  - Issue / PR / fork signals
  - Relevance scoring for downstream prioritisation

version2 additions
------------
Repository metadata
  releases_count, latest_release_tag, latest_release_date
  has_ci_cd, has_tests, has_contributing_guide, has_code_of_conduct
  dependency_count, contributor_count

Skill evidence
  frameworks_detected, devops_tools_detected, testing_frameworks_detected
  ci_platforms_detected, cloud_platforms_detected, db_technologies_detected
  api_patterns_detected, documentation_quality_score
  total_commit_count (REST-based exact count, no sampling cap)
  commit_frequency_per_year ({year: count} from sampled commits)
  avg_commits_per_active_month
  last_commit_date
  total_lines_added, total_lines_deleted
  commit_message_quality_score, contribution_gap_months
  commit_size_distribution (small/medium/large buckets)

Role evidence
  issues_opened_count, closed_issues_count
  pr_open_count, pr_closed_count, pr_merged_count
  avg_issue_close_time_days, pr_merge_rate_pct

Aggregate signals (returned from collect_all_repos)
  monthly_commit_heatmap  ({YYYY-MM: count})
  recent_6_month_commit_count
  total_commits_all_repos
  total_prs_opened, total_prs_merged
  total_issues_opened, total_issues_closed
  external_contributions_count
  longest_contribution_streak_days
  most_active_year
  inactive_periods (list of {from, to} year-month strings for gaps > 3 months)
  collection_completeness_pct
"""

import base64
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests as _req
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

# Relevance scoring (unchanged from v1)

def score_repository(repo_meta: dict, owner_login: str) -> float:
    """
    Compute a relevance score in [0, 1] for a repository.

    Signals: stars, forks, size, is_owner, recency, age_bonus.
    All weights are configurable via config.SCORING_WEIGHTS.
    """
    w = config.SCORING_WEIGHTS

    def log_norm(value: int, scale: float = 100.0) -> float:
        return math.log1p(max(0, value)) / math.log1p(scale)

    stars_score   = min(log_norm(repo_meta.get("stargazers_count", 0), 1000), 1.0)
    forks_score   = min(log_norm(repo_meta.get("forks_count", 0), 200), 1.0)
    size_score    = min(log_norm(repo_meta.get("size", 0), 50000), 1.0)
    is_owner_score = 1.0 if not repo_meta.get("fork", True) else 0.0

    pushed_at   = repo_meta.get("pushed_at")
    age_years   = years_since(pushed_at) if pushed_at else 5.0
    recency_score = math.exp(-0.35 * age_years)

    created_at      = repo_meta.get("created_at")
    repo_age_years  = years_since(created_at) if created_at else 0.0
    age_bonus_score = min(repo_age_years / 10.0, 1.0) if repo_age_years > 3 else 0.0

    score = (
        w.get("stars",     0.25) * stars_score
        + w.get("forks",   0.20) * forks_score
        + w.get("size",    0.10) * size_score
        + w.get("is_owner",0.20) * is_owner_score
        + w.get("recency", 0.15) * recency_score
        + w.get("age_bonus",0.10) * age_bonus_score
    )
    return round(min(score, 1.0), 4)


# README collection (v1 unchanged)


def _fetch_readme(repo: Repository) -> tuple[str, str]:
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

# Commit sampling — v2: returns full collection metadata


def _sample_commits(
    repo: Repository, author_login: str, n: int = 30
) -> tuple[list[dict], dict]:
    """
    Collect up to 500 commits by the author, return:
      - sampled list of up to `n` evenly-spaced commit dicts
      - stats dict with aggregated commit analytics

    Each commit dict: sha, message, date, additions, deletions, url.

    Stats dict:
      total_sampled_count   : number actually fetched (≤500)
      commit_frequency_per_year : {year: count}
      monthly_counts        : {YYYY-MM: count}  (for heatmap roll-up)
      total_lines_added     : int
      total_lines_deleted   : int
      last_commit_date      : ISO string
      commit_size_distribution : {small: N, medium: N, large: N}
          small  < 10 lines changed
          medium 10–99 lines
          large  ≥ 100 lines
    """
    try:
        commits_pager = with_retry(
            lambda: repo.get_commits(author=author_login),
            label=f"commits({repo.full_name})",
        )
        all_commits: list[dict] = []
        for i, commit in enumerate(commits_pager):
            if i >= 500:          # v2: raised from 200 → 500
                break
            commit_date = datetime_to_iso(safe_get(commit.commit.author, "date"))
            stats = safe_get(commit, "stats")
            adds = safe_get(stats, "additions", 0) if stats else 0
            dels = safe_get(stats, "deletions", 0) if stats else 0
            all_commits.append({
                "sha":       commit.sha[:10],
                "message":   _truncate_msg(safe_get(commit.commit, "message", "")),
                "date":      commit_date,
                "additions": adds,
                "deletions": dels,
                "url":       safe_get(commit, "html_url"),
            })

        if not all_commits:
            return [], _empty_commit_stats()

        all_commits.sort(key=lambda c: c["date"] or "")

        # --- aggregate stats over the FULL collected set ---
        freq_per_year: dict[int, int] = defaultdict(int)
        monthly:       dict[str, int] = defaultdict(int)
        total_adds = total_dels = 0
        size_dist = {"small": 0, "medium": 0, "large": 0}

        for c in all_commits:
            year = iso_to_year(c["date"])
            if year:
                freq_per_year[year] += 1
            if c["date"] and len(c["date"]) >= 7:
                ym = c["date"][:7]   # "YYYY-MM"
                monthly[ym] += 1
            total_adds += c.get("additions", 0)
            total_dels += c.get("deletions", 0)
            chg = c.get("additions", 0) + c.get("deletions", 0)
            if chg < 10:
                size_dist["small"] += 1
            elif chg < 100:
                size_dist["medium"] += 1
            else:
                size_dist["large"] += 1

        # avg commits per active month
        active_months = len(monthly)
        avg_per_month = round(len(all_commits) / active_months, 2) if active_months else 0.0

        messages = [c.get("message", "") for c in all_commits]
        message_quality = _commit_message_quality_score(messages)
        contribution_gap_months = _max_gap_months(monthly)

        commit_stats = {
            "total_sampled_count":         len(all_commits),
            "commit_frequency_per_year":   dict(sorted(freq_per_year.items())),
            "monthly_counts":              dict(sorted(monthly.items())),
            "total_lines_added":           total_adds,
            "total_lines_deleted":         total_dels,
            "last_commit_date":            all_commits[-1]["date"],
            "avg_commits_per_active_month": avg_per_month,
            "commit_message_quality_score": message_quality,
            "contribution_gap_months":      contribution_gap_months,
            "commit_size_distribution":    size_dist,
        }

        # --- evenly-spaced sample for raw_text_evidence ---
        if len(all_commits) <= n:
            sampled = all_commits
        else:
            step    = len(all_commits) / n
            sampled = [all_commits[int(i * step)] for i in range(n)]

        return sampled, commit_stats

    except GithubException as exc:
        logger.warning("Could not fetch commits for %s: %s", repo.full_name, exc)
        return [], _empty_commit_stats()


def _empty_commit_stats() -> dict:
    return {
        "total_sampled_count": 0,
        "commit_frequency_per_year": {},
        "monthly_counts": {},
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "last_commit_date": None,
        "avg_commits_per_active_month": 0.0,
        "commit_message_quality_score": 0.0,
        "contribution_gap_months": 0,
        "commit_size_distribution": {"small": 0, "medium": 0, "large": 0},
    }


def _truncate_msg(msg: str, max_chars: int = 200) -> str:
    msg = msg.strip().splitlines()[0] if msg.strip() else ""
    return msg[:max_chars] + ("…" if len(msg) > max_chars else "")


def _commit_message_quality_score(messages: list[str]) -> float:
    """
    Simple 0–10 proxy score for commit message quality.

    Higher score = more descriptive, less vague, less noisy.
    This is not a perfect metric, but it is useful as a lightweight
    HR-facing signal for professional discipline.
    """
    if not messages:
        return 0.0

    vague = {"update", "fix", "changes", "changed", "test", "wip", "misc", "final"}
    action_words = {
        "add", "implement", "refactor", "remove", "fix", "improve",
        "update", "create", "support", "handle", "validate", "optimize",
    }
    scores = []

    for msg in messages:
        clean = (msg or "").strip().lower()
        if not clean:
            scores.append(0)
            continue

        score = 5
        words = clean.split()
        first_word = words[0] if words else ""

        if len(clean) >= 20:
            score += 2
        elif len(clean) < 8:
            score -= 2

        if first_word in vague and len(words) <= 3:
            score -= 2

        if any(word in clean for word in action_words):
            score += 1

        if "#" in clean or re.search(r"\b(issue|ticket|bug|feature|task)\b", clean):
            score += 1

        scores.append(max(0, min(score, 10)))

    return round(sum(scores) / len(scores), 2)

# kept for external callers
truncate_msg = _truncate_msg

# v2: Exact commit count via REST (avoids pagination cost for count-only)


def _fetch_total_commit_count(full_name: str, author_login: str) -> int:
    """
    Use the GitHub REST contributor stats endpoint to get the total commit count
    for one contributor on a repo. Falls back to 0 on error.

    Note: stats API returns 202 + empty body while GitHub computes the data.
    We retry up to 3 times with a short sleep.
    """
    import time
    url = f"https://api.github.com/repos/{full_name}/contributors"
    headers = {"Authorization": f"token {config.GITHUB_TOKEN}"}
    for attempt in range(3):
        try:
            resp = _req.get(url, headers=headers, params={"per_page": 100}, timeout=15)
            if resp.status_code == 202:
                time.sleep(3)
                continue
            if resp.status_code == 200:
                for contributor in resp.json():
                    if contributor.get("login", "").lower() == author_login.lower():
                        return contributor.get("contributions", 0)
                return 0
        except Exception:
            pass
    return 0


# Language breakdown (v1 unchanged)


def _fetch_languages(repo: Repository) -> dict:
    try:
        response = _req.get(
            f"https://api.github.com/repos/{repo.full_name}/languages",
            headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
            timeout=10,
        )
        if response.status_code == 200:
            return {lang: int(val) for lang, val in response.json().items()}
        return {}
    except Exception:
        return {}


# Topics (v1 unchanged)


def _fetch_topics(repo: Repository) -> list[str]:
    try:
        return with_retry(lambda: repo.get_topics(), label=f"topics({repo.full_name})")
    except GithubException:
        return []


# v2: PR & Issue statistics


def _fetch_issue_pr_stats(repo: Repository) -> dict:
    """
    Fetch lightweight PR and issue stats via the REST API.

    Returns:
      pr_open_count, pr_closed_count, pr_merged_count, pr_merge_rate_pct
      issues_opened_count, closed_issues_count, avg_issue_close_time_days
    """
    headers = {"Authorization": f"token {config.GITHUB_TOKEN}"}
    base_url = f"https://api.github.com/repos/{repo.full_name}"

    def _count(endpoint: str, params: dict) -> int:
        """Return total count from a list endpoint using the Link header when available."""
        try:
            params_with_page = {**params, "per_page": 1}
            r = _req.get(
                f"{base_url}/{endpoint}",
                headers=headers,
                params=params_with_page,
                timeout=10,
            )
            if r.status_code != 200:
                return 0

            link = r.headers.get("Link", "")
            match = re.search(r'page=(\d+)>; rel="last"', link)
            if match:
                return int(match.group(1))

            return len(r.json())
        except Exception:
            return 0

    pr_open = _count("pulls", {"state": "open"})
    pr_closed = _count("pulls", {"state": "closed"})  # includes merged + rejected

    # Merged count via search API. This is more accurate than checking a small PR sample.
    pr_merged = 0
    try:
        r = _req.get(
            "https://api.github.com/search/issues",
            headers=headers,
            params={"q": f"repo:{repo.full_name} is:pr is:merged", "per_page": 1},
            timeout=10,
        )
        if r.status_code == 200:
            pr_merged = r.json().get("total_count", 0)
    except Exception:
        pr_merged = 0

    # pr_closed already includes merged PRs, so do not add pr_merged again.
    pr_merge_rate = round(pr_merged / pr_closed * 100, 1) if pr_closed > 0 else None

    issues_opened = _count("issues", {"state": "open"})
    closed_issues = _count("issues", {"state": "closed"})

    # Average issue close time — sample the 30 most recently closed issues.
    avg_close_time = None
    try:
        r = _req.get(
            f"{base_url}/issues",
            headers=headers,
            params={
                "state": "closed",
                "per_page": 30,
                "sort": "updated",
                "direction": "desc",
            },
            timeout=15,
        )
        if r.status_code == 200:
            durations = []
            for issue in r.json():
                if issue.get("pull_request"):
                    continue
                created = issue.get("created_at")
                closed = issue.get("closed_at")
                if created and closed:
                    try:
                        t0 = datetime.fromisoformat(created.replace("Z", "+00:00"))
                        t1 = datetime.fromisoformat(closed.replace("Z", "+00:00"))
                        durations.append((t1 - t0).total_seconds() / 86400)
                    except Exception:
                        pass
            if durations:
                avg_close_time = round(sum(durations) / len(durations), 1)
    except Exception:
        pass

    return {
        "pr_open_count": pr_open,
        "pr_closed_count": pr_closed,
        "pr_merged_count": pr_merged,
        "pr_merge_rate_pct": pr_merge_rate,
        "issues_opened_count": issues_opened,
        "closed_issues_count": closed_issues,
        "avg_issue_close_time_days": avg_close_time,
    }

# v2: Contributor count


def _fetch_contributor_count(full_name: str) -> int:
    """Return number of distinct contributors to the repo."""
    try:
        r = _req.get(
            f"https://api.github.com/repos/{full_name}/contributors",
            headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
            params={"per_page": 1, "anon": "true"},
            timeout=10,
        )
        if r.status_code != 200:
            return 0
        link = r.headers.get("Link", "")
        match = re.search(r'page=(\d+)>; rel="last"', link)
        return int(match.group(1)) if match else len(r.json())
    except Exception:
        return 0


# v2: Release information


def _fetch_release_info(full_name: str) -> dict:
    """
    Return:
      releases_count       : int
      latest_release_tag   : str | None
      latest_release_date  : ISO str | None
    """
    try:
        r = _req.get(
            f"https://api.github.com/repos/{full_name}/releases/latest",
            headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
            timeout=10,
        )
        latest_tag  = None
        latest_date = None
        if r.status_code == 200:
            data        = r.json()
            latest_tag  = data.get("tag_name")
            latest_date = data.get("published_at")

        # Count all releases (may be slow for repos with thousands; capped at 1 req)
        r2 = _req.get(
            f"https://api.github.com/repos/{full_name}/releases",
            headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
            params={"per_page": 1},
            timeout=10,
        )
        releases_count = 0
        if r2.status_code == 200:
            link = r2.headers.get("Link", "")
            match = re.search(r'page=(\d+)>; rel="last"', link)
            releases_count = int(match.group(1)) if match else len(r2.json())

        return {
            "releases_count":      releases_count,
            "latest_release_tag":  latest_tag,
            "latest_release_date": latest_date,
        }
    except Exception:
        return {"releases_count": 0, "latest_release_tag": None, "latest_release_date": None}


# v2: Engineering maturity signals (CI, tests, contributing guide, CoC)


def _fetch_engineering_maturity(repo: Repository) -> dict:
    """
    Check for presence of common engineering-quality files in the default branch.

    Returns:
      has_ci_cd              : bool  (any .github/workflows/*.yml or .travis.yml etc.)
      has_tests              : bool  (tests/ or test/ directory or *test*.py)
      has_contributing_guide : bool  (CONTRIBUTING.md or similar)
      has_code_of_conduct    : bool  (CODE_OF_CONDUCT.md or similar)
      dependency_count       : int   (rough count from requirements.txt / package.json)
    """
    result = {
        "has_ci_cd":              False,
        "has_tests":              False,
        "has_contributing_guide": False,
        "has_code_of_conduct":    False,
        "dependency_count":       0,
    }
    try:
        tree = with_retry(
            lambda: repo.get_git_tree(repo.default_branch, recursive=True),
            label=f"tree({repo.full_name})",
        )
        paths = [item.path.lower() for item in (tree.tree or [])]

        # CI/CD: GitHub Actions workflows, Travis, CircleCI, Jenkins
        ci_patterns = [
            ".github/workflows/", ".travis.yml", "circle.yml", ".circleci/",
            "jenkinsfile", ".gitlab-ci.yml", "azure-pipelines.yml",
        ]
        result["has_ci_cd"] = any(
            any(p.startswith(ci) or p == ci for ci in ci_patterns)
            for p in paths
        )

        # Tests: common test directory/file patterns
        test_patterns = ["test/", "tests/", "spec/", "__tests__/", "test_", "_test.py",
                         ".test.js", ".spec.js", ".spec.ts"]
        result["has_tests"] = any(
            any(tp in p for tp in test_patterns) for p in paths
        )

        # Contributing guide
        result["has_contributing_guide"] = any(
            "contributing" in p for p in paths
        )

        # Code of conduct
        result["has_code_of_conduct"] = any(
            "code_of_conduct" in p or "code-of-conduct" in p for p in paths
        )

        # Dependency count — parse requirements.txt or package.json
        dep_count = 0
        if "requirements.txt" in paths:
            try:
                content = with_retry(
                    lambda: repo.get_contents("requirements.txt"),
                    label=f"req({repo.full_name})",
                )
                text = base64.b64decode(content.content).decode("utf-8", errors="replace")
                dep_count = len([
                    l for l in text.splitlines()
                    if l.strip() and not l.strip().startswith("#")
                ])
            except Exception:
                pass
        elif "package.json" in paths:
            try:
                content = with_retry(
                    lambda: repo.get_contents("package.json"),
                    label=f"pkg({repo.full_name})",
                )
                import json as _json
                pkg = _json.loads(
                    base64.b64decode(content.content).decode("utf-8", errors="replace")
                )
                dep_count = len(pkg.get("dependencies", {})) + \
                            len(pkg.get("devDependencies", {}))
            except Exception:
                pass

        result["dependency_count"] = dep_count

    except GithubException as exc:
        logger.warning("Engineering maturity check failed for %s: %s",
                       repo.full_name, exc)

    return result


# v2: Framework / tooling detection from file tree + README

_FRAMEWORK_PATTERNS: dict[str, dict[str, list[str]]] = {
    "frameworks": {
        "react": ["react", "jsx", "tsx"],
        "vue": ["vue.js", "vue.config"],
        "angular": ["angular.json", "@angular"],
        "django": ["django", "manage.py"],
        "flask": ["flask", "from flask"],
        "fastapi": ["fastapi", "from fastapi"],
        "rails": ["gemfile", "rails"],
        "spring": ["pom.xml", "spring-boot"],
        "next.js": ["next.config", "next/router"],
        "svelte": ["svelte.config", ".svelte"],
        "express": ["express()", "require('express')"],
        "laravel": ["artisan", "laravel"],
        "pytorch": ["import torch", "torch.nn"],
        "tensorflow": ["import tensorflow", "tf.keras"],
        "scikit-learn": ["sklearn", "scikit-learn"],
        "huggingface": ["transformers", "from transformers"],
    },
    "devops_tools": {
        "docker": ["dockerfile", "docker-compose.yml", "docker-compose.yaml"],
        "kubernetes": ["kubernetes", "k8s", "deployment.yaml", "service.yaml"],
        "terraform": ["main.tf", ".tf"],
        "ansible": ["playbook.yml", "ansible"],
        "helm": ["chart.yaml", "helm"],
    },
    "testing_frameworks": {
        "pytest": ["pytest", "conftest.py"],
        "unittest": ["unittest", "testcase"],
        "jest": ["jest.config", "describe(", "it("],
        "mocha": ["mocha", ".mocharc"],
        "cypress": ["cypress.json", "cypress/"],
        "playwright": ["playwright.config"],
        "rspec": ["rspec", "_spec.rb"],
    },
    "ci_platforms": {
        "github_actions": [".github/workflows"],
        "circleci": [".circleci"],
        "travisci": [".travis.yml"],
        "gitlab_ci": [".gitlab-ci.yml"],
        "jenkins": ["jenkinsfile"],
        "azure_pipelines": ["azure-pipelines.yml"],
    },
    "cloud_platforms": {
        "aws": ["boto3", "aws-sdk", "aws_", "amazon", "lambda", "s3"],
        "gcp": ["google-cloud", "gcp", "firebase"],
        "azure": ["azure-", "microsoft azure"],
        "heroku": ["procfile", "heroku"],
        "vercel": ["vercel.json", "vercel"],
        "netlify": ["netlify.toml", "netlify"],
    },
    "databases": {
        "postgresql": ["postgresql", "postgres", "psycopg2", "pg_"],
        "mysql": ["mysql", "pymysql"],
        "mongodb": ["mongodb", "mongoose", "pymongo"],
        "redis": ["redis", "aioredis"],
        "elasticsearch": ["elasticsearch", "opensearch"],
        "sqlite": ["sqlite", "sqlite3"],
        "kafka": ["kafka", "confluent"],
    },
    "api_patterns": {
        "rest": ["rest api", "restful", "requests.get", "fetch("],
        "graphql": ["graphql", "apollo", "gql"],
        "grpc": ["grpc", "protobuf", ".proto"],
        "websocket": ["websocket", "socket.io"],
        "openapi": ["openapi", "swagger"],
    },
}

def _detect_tooling(readme_raw: str, file_paths: list[str]) -> dict[str, list[str]]:
    """
    Scan README text and repo file paths for framework / tooling signals.
    Returns a dict keyed by category with a list of detected tool names.
    """
    text_lower  = readme_raw.lower()
    paths_lower = " ".join(file_paths).lower()
    combined    = text_lower + " " + paths_lower

    results: dict[str, list[str]] = {}
    for category, tools in _FRAMEWORK_PATTERNS.items():
        detected = []
        for tool_name, signals in tools.items():
            if any(sig in combined for sig in signals):
                detected.append(tool_name)
        results[category] = detected

    return results


def _documentation_quality_score(
    readme_raw: str,
    has_contributing: bool,
    has_coc: bool,
    has_wiki: bool,
    has_pages: bool,
) -> int:
    """
    Simple 0–10 score for documentation quality.

    Criteria (1 point each unless noted):
      README exists (+1), has installation section (+1), has usage section (+1),
      has badges (+1), has screenshots/gifs (+1), has license section (+1),
      CONTRIBUTING.md (+1), CODE_OF_CONDUCT.md (+1), has wiki (+1), has pages (+1)
    """
    score = 0
    if readme_raw:
        score += 1
        lower = readme_raw.lower()
        if "install" in lower or "getting started" in lower:  score += 1
        if "usage" in lower  or "how to use" in lower:        score += 1
        if "[![" in readme_raw:                                score += 1  # badges
        if ".gif" in lower or ".png" in lower or ".jpg" in lower: score += 1
        if "license" in lower:                                 score += 1
    if has_contributing: score += 1
    if has_coc:          score += 1
    if has_wiki:         score += 1
    if has_pages:        score += 1
    return min(score, 10)

# Single repository collector — v2

def collect_single_repo(repo: Repository, owner_login: str) -> dict:
    """
    Collect full evidence-rich data for a single repository (v2).

    Returns a dict matching the `repositories[i]` section of the output schema.
    """
    logger.debug("  Collecting repo (v2): %s", repo.full_name)

    # --- Repository metadata (v1 fields) ---
    meta = {
        "name":              safe_get(repo, "name"),
        "full_name":         safe_get(repo, "full_name"),
        "description":       safe_get(repo, "description"),
        "html_url":          safe_get(repo, "html_url"),
        "homepage":          safe_get(repo, "homepage"),
        "language":          safe_get(repo, "language"),
        "stargazers_count":  safe_get(repo, "stargazers_count", 0),
        "forks_count":       safe_get(repo, "forks_count", 0),
        "watchers_count":    safe_get(repo, "watchers_count", 0),
        "open_issues_count": safe_get(repo, "open_issues_count", 0),
        "size":              safe_get(repo, "size", 0),
        "default_branch":    safe_get(repo, "default_branch"),
        "fork":              safe_get(repo, "fork", False),
        "archived":          safe_get(repo, "archived", False),
        "visibility":        safe_get(repo, "visibility", "public"),
        "created_at":        datetime_to_iso(safe_get(repo, "created_at")),
        "updated_at":        datetime_to_iso(safe_get(repo, "updated_at")),
        "pushed_at":         datetime_to_iso(safe_get(repo, "pushed_at")),
        "license":           (
            safe_get(safe_get(repo, "license"), "name")
            if safe_get(repo, "license") else None
        ),
        "topics":            _fetch_topics(repo),
    }

    # --- v2: Engineering maturity ---
    engineering = _fetch_engineering_maturity(repo)
    meta.update({
        "has_ci_cd":              engineering["has_ci_cd"],
        "has_tests":              engineering["has_tests"],
        "has_contributing_guide": engineering["has_contributing_guide"],
        "has_code_of_conduct":    engineering["has_code_of_conduct"],
        "dependency_count":       engineering["dependency_count"],
    })

    # --- v2: Releases ---
    release_info = _fetch_release_info(meta["full_name"])
    meta.update(release_info)

    # --- v2: Contributor count ---
    meta["contributor_count"] = _fetch_contributor_count(meta["full_name"])

    # --- Languages ---
    languages   = _fetch_languages(repo)
    total_bytes = sum(languages.values()) or 1
    language_breakdown = {
        lang: {"bytes": b, "pct": round(b / total_bytes * 100, 1)}
        for lang, b in sorted(languages.items(), key=lambda x: -x[1])
    }

    # --- README ---
    readme_raw, readme_excerpt = _fetch_readme(repo)

    # --- File tree for tooling detection ---
    try:
        tree  = with_retry(
            lambda: repo.get_git_tree(repo.default_branch or "main", recursive=True),
            label=f"tree({repo.full_name})",
        )
        paths = [item.path for item in (tree.tree or [])]
    except GithubException:
        paths = []

    # --- Tooling detection ---
    tooling = _detect_tooling(readme_raw, paths)

    # --- Documentation quality score ---
    doc_score = _documentation_quality_score(
        readme_raw,
        has_contributing = engineering["has_contributing_guide"],
        has_coc          = engineering["has_code_of_conduct"],
        has_wiki         = safe_get(repo, "has_wiki", False),
        has_pages        = safe_get(repo, "has_pages", False),
    )

    # --- Commits (v2: returns sample + stats) ---
    commits, commit_stats = _sample_commits(repo, owner_login, n=config.COMMITS_PER_REPO)

    # --- v2: exact total commit count ---
    total_commits = _fetch_total_commit_count(meta["full_name"], owner_login)
    # If REST gave 0 but we sampled commits, use sampled count as fallback
    if total_commits == 0 and commit_stats["total_sampled_count"] > 0:
        total_commits = commit_stats["total_sampled_count"]

    commit_years = sorted(set(
        iso_to_year(c["date"]) for c in commits if c.get("date")
    ))

    # --- Role evidence (v1 + v2 PR/issue stats) ---
    is_owner    = not meta["fork"]
    pr_issue    = _fetch_issue_pr_stats(repo)

    role_evidence = {
        # v1
        "is_owner":    is_owner,
        "is_fork":     meta["fork"],
        "forked_from": (
            safe_get(safe_get(repo, "parent"), "full_name") if meta["fork"] else None
        ),
        "has_wiki":    safe_get(repo, "has_wiki", False),
        "has_issues":  safe_get(repo, "has_issues", False),
        "has_projects":safe_get(repo, "has_projects", False),
        "has_pages":   safe_get(repo, "has_pages", False),
        "stars_received": meta["stargazers_count"],
        "forks_received": meta["forks_count"],
        # v2 — PR / issue / maintainer quality
        "issues_opened_count":        pr_issue["issues_opened_count"],
        "closed_issues_count":        pr_issue["closed_issues_count"],
        "pr_open_count":              pr_issue["pr_open_count"],
        "pr_closed_count":            pr_issue["pr_closed_count"],
        "pr_merged_count":            pr_issue["pr_merged_count"],
        "pr_merge_rate_pct":          pr_issue["pr_merge_rate_pct"],
        "avg_issue_close_time_days":  pr_issue["avg_issue_close_time_days"],
        "contributor_count":          meta["contributor_count"],
    }

    # --- Leadership evidence (v1 logic preserved, new signals added) ---
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
                f"Sustained contributions over {span_years}+ years "
                f"({min(commit_years)}–{max(commit_years)})"
            )
    if meta["contributor_count"] >= 5:
        leadership_signals.append(
            f"Attracted {meta['contributor_count']} contributors to the project"
        )
    if meta.get("has_ci_cd"):
        leadership_signals.append("Project has CI/CD pipeline configured")
    if release_info["releases_count"] >= 3:
        leadership_signals.append(
            f"Published {release_info['releases_count']} releases "
            f"(latest: {release_info.get('latest_release_tag', 'N/A')})"
        )

    leadership_evidence = {
        "signals":          leadership_signals,
        "commit_year_span": (
            {"from": min(commit_years), "to": max(commit_years)}
            if commit_years else {}
        ),
    }

    #  --- Skill evidence (v1 + v2) ---
    skill_evidence = {
        # v1
        "primary_language":   meta["language"],
        "languages_used":     list(language_breakdown.keys()),
        "language_breakdown": language_breakdown,
        "topics":             meta["topics"],
        "readme_keywords":    _extract_tech_keywords(readme_raw),
        "commit_count_sampled": len(commits),
        "commit_years_covered": commit_years,
        # v2 — commit depth
        "total_commit_count":           total_commits,
        "total_commits_to_repo":        total_commits,
        "commit_frequency_per_year":    commit_stats["commit_frequency_per_year"],
        "avg_commits_per_active_month": commit_stats["avg_commits_per_active_month"],
        "last_commit_date":             commit_stats["last_commit_date"],
        "commit_message_quality_score": commit_stats["commit_message_quality_score"],
        "contribution_gap_months":      commit_stats["contribution_gap_months"],
        "total_lines_added":            commit_stats["total_lines_added"],
        "total_lines_deleted":          commit_stats["total_lines_deleted"],
        "commit_size_distribution":     commit_stats["commit_size_distribution"],
        # v2 — tooling
        "frameworks_detected":          tooling.get("frameworks", []),
        "devops_tools_detected":         tooling.get("devops_tools", []),
        "testing_frameworks_detected":   tooling.get("testing_frameworks", []),
        "ci_platforms_detected":         tooling.get("ci_platforms", []),
        "cloud_platforms_detected":      tooling.get("cloud_platforms", []),
        "db_technologies_detected":      tooling.get("databases", []),
        "api_patterns_detected":         tooling.get("api_patterns", []),
        "documentation_quality_score":   doc_score,
    }

    relevance_score = score_repository(meta, owner_login)

    return {
        "repository_metadata": meta,
        "relevance_score":     relevance_score,
        "skill_evidence":      skill_evidence,
        "role_evidence":       role_evidence,
        "leadership_evidence": leadership_evidence,
        "raw_text_evidence": {
            "readme_excerpt":  readme_excerpt,
            "commit_samples":  commits,
            "description":     meta.get("description") or "",
            # v2: expose per-repo monthly commit heatmap for later roll-up
            "_monthly_commit_counts": commit_stats["monthly_counts"],
        },
    }


# All-repositories collector — v2 (adds aggregate signals)


def collect_all_repos(client: Github, username: str) -> tuple[list[dict], dict]:
    """
    Collect all public repositories for `username`.

    Returns
    -------
    (repositories, extra_aggregate_signals)
      repositories             : list sorted by relevance_score desc (all retained)
      extra_aggregate_signals  : v2-only aggregate fields to merge into the
                                 top-level `aggregate_signals` block
    """
    logger.info("Collecting repositories for: %s (v2)", username)

    user = with_retry(lambda: client.get_user(username), label=f"get_user({username})")

    repos_pager = with_retry(
        lambda: user.get_repos(type="public", sort="updated"),
        label=f"get_repos({username})",
    )

    total_public_repos = safe_get(user, "public_repos", 0)

    collected = []
    for i, repo in enumerate(repos_pager):
        if i >= config.MAX_REPOS_PER_USER:
            logger.info(
                "Reached MAX_REPOS_PER_USER (%d) for %s", config.MAX_REPOS_PER_USER, username
            )
            break
        if i > 0 and i % 10 == 0:
            wait_for_rate_limit(client, buffer=20)
            logger.info("  Progress: %d repos collected for %s", i, username)
        try:
            repo_data = collect_single_repo(repo, username)
            collected.append(repo_data)
        except Exception as exc:
            logger.warning("Failed to collect repo %s: %s", repo.full_name, exc)

    collected.sort(key=lambda r: r.get("relevance_score", 0), reverse=True)
    logger.info("Collected %d repositories for %s", len(collected), username)

  
    # Build v2 aggregate signals from per-repo data
   
    global_monthly: dict[str, int] = defaultdict(int)
    total_commits  = 0
    total_adds     = 0
    total_dels     = 0
    total_prs_open = total_prs_merged = 0
    total_issues_opened = 0
    total_issues_closed = 0
    external_contributions = 0
    yearly_totals: dict[int, int] = defaultdict(int)

    for repo_data in collected:
        se = repo_data.get("skill_evidence", {})
        re_ = repo_data.get("role_evidence", {})
        rte = repo_data.get("raw_text_evidence", {})

        # Commit roll-up
        for ym, cnt in rte.get("_monthly_commit_counts", {}).items():
            global_monthly[ym] += cnt
        total_commits += se.get("total_commit_count", 0)
        total_adds    += se.get("total_lines_added", 0)
        total_dels    += se.get("total_lines_deleted", 0)
        for yr, cnt in se.get("commit_frequency_per_year", {}).items():
            yearly_totals[int(yr)] += cnt

        # PR / issue roll-up
        total_prs_open    += re_.get("pr_open_count", 0)
        total_prs_merged  += re_.get("pr_merged_count", 0)
        total_issues_opened += re_.get("issues_opened_count", 0)
        total_issues_closed += re_.get("closed_issues_count", 0)

        # External contributions (repos not owned by user)
        if re_.get("is_fork"):
            external_contributions += 1

    # Recent 6-month commit count
    now = datetime.now(timezone.utc)
    recent_cutoff = f"{(now - timedelta(days=182)).strftime('%Y-%m')}"
    recent_6m = sum(
        cnt for ym, cnt in global_monthly.items() if ym >= recent_cutoff
    )

    # Contribution streak (longest run of consecutive months with ≥1 commit)
    longest_streak = _compute_longest_streak(global_monthly)

    # Most active year
    most_active_year = max(yearly_totals, key=yearly_totals.get) if yearly_totals else None

    # Inactive periods (gaps > 3 months)
    inactive_periods = _find_inactive_periods(global_monthly)

    # Collection completeness
    completeness_pct = (
        round(len(collected) / total_public_repos * 100, 1)
        if total_public_repos > 0 else None
    )

    extra_aggregate = {
        "monthly_commit_heatmap":       dict(sorted(global_monthly.items())),
        "recent_6_month_commit_count":  recent_6m,
        "total_commits_all_repos":      total_commits,
        "total_lines_added_all_repos":  total_adds,
        "total_lines_deleted_all_repos":total_dels,
        "total_prs_opened":             total_prs_open,
        "total_prs_merged":             total_prs_merged,
        "total_issues_opened":          total_issues_opened,
        "total_issues_closed":          total_issues_closed,
        "external_contributions_count": external_contributions,
        "longest_contribution_streak_months": longest_streak,
        "most_active_year":             most_active_year,
        "inactive_periods":             inactive_periods,
        "collection_completeness_pct":  completeness_pct,
    }

    return collected, extra_aggregate

# Streak & gap helpers


def _compute_longest_streak(monthly: dict[str, int]) -> int:
    """Return the longest run of consecutive months that each have ≥1 commit."""
    if not monthly:
        return 0
    months = sorted(monthly.keys())
    streak = best = 1
    for i in range(1, len(months)):
        prev = datetime.strptime(months[i - 1], "%Y-%m")
        curr = datetime.strptime(months[i],     "%Y-%m")
        delta_months = (curr.year - prev.year) * 12 + (curr.month - prev.month)
        if delta_months == 1:
            streak += 1
            best = max(best, streak)
        else:
            streak = 1
    return best


def _max_gap_months(monthly: dict[str, int]) -> int:
    """Return the largest inactive gap between active commit months."""
    if not monthly:
        return 0

    months = sorted(monthly.keys())
    max_gap = 0

    for i in range(1, len(months)):
        prev = datetime.strptime(months[i - 1], "%Y-%m")
        curr = datetime.strptime(months[i], "%Y-%m")
        delta_months = (curr.year - prev.year) * 12 + (curr.month - prev.month)
        max_gap = max(max_gap, delta_months - 1)

    return max_gap


def _find_inactive_periods(monthly: dict[str, int]) -> list[dict]:
    """
    Return list of {from, to} dicts for gaps > 3 consecutive months with no commits.
    """
    if not monthly:
        return []
    months  = sorted(monthly.keys())
    gaps    = []
    for i in range(1, len(months)):
        prev = datetime.strptime(months[i - 1], "%Y-%m")
        curr = datetime.strptime(months[i],     "%Y-%m")
        delta_months = (curr.year - prev.year) * 12 + (curr.month - prev.month)
        if delta_months > 3:
            gaps.append({
                "from": months[i - 1],
                "to":   months[i],
                "gap_months": delta_months - 1,
            })
    return gaps

# Tech keyword extraction (v1 unchanged, extended set)

_TECH_KEYWORDS = {
    # Languages
    "python", "javascript", "typescript", "java", "go", "rust", "c++", "c#",
    "swift", "kotlin", "ruby", "php", "scala", "elixir", "clojure", "haskell",
    "r language", "matlab", "julia", "dart", "flutter",
    # Frameworks
    "react", "vue", "angular", "svelte", "next.js", "nuxt", "django", "flask",
    "fastapi", "rails", "spring", "express", "graphql", "rest", "grpc",
    "laravel", "nestjs", "starlette",
    # DevOps / Infra
    "docker", "kubernetes", "terraform", "ansible", "ci/cd", "github actions",
    "helm", "prometheus", "grafana", "nginx",
    # Cloud
    "aws", "gcp", "azure", "cloud", "serverless", "lambda", "heroku", "vercel",
    # ML / AI
    "machine learning", "deep learning", "neural", "transformer", "llm",
    "pytorch", "tensorflow", "scikit", "pandas", "numpy", "mlops",
    "hugging face", "langchain", "rag", "fine-tuning",
    # Databases
    "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "kafka",
    "sqlite", "cassandra", "dynamodb", "supabase",
    # Patterns / practices
    "microservices", "api", "cli", "sdk", "library", "framework",
    "open source", "testing", "tdd", "bdd", "devops", "websocket", "grpc",
    "oauth", "jwt", "openapi", "swagger",
}


def _extract_tech_keywords(text: str) -> list[str]:
    """Return tech keywords found in text (case-insensitive)."""
    if not text:
        return []
    lower = text.lower()
    return sorted(kw for kw in _TECH_KEYWORDS if kw in lower)