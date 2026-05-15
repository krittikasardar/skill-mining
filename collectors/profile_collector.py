"""
collectors/profile_collector.py
--------------------------------
Collects public user profile metadata from the GitHub API.

v2.1 additions / fixes
----------------------
- hireable                         : user's self-reported open-to-work flag
- profile_readme                   : content of the special <username>/<username> repo README
- profile_readme_content           : alias used by the gap-analysis document
- pinned_repositories              : user's self-curated best-work repos via GraphQL
- organisations                    : public organisation memberships
- organisations_member_of          : alias used by the gap-analysis document
- is_sponsored                     : whether GitHub Sponsors listing is enabled
- sponsoring_count                 : number of users/orgs this user sponsors
- years_on_github                  : derived from account creation date
- followers_to_following_ratio     : derived influence proxy
"""

import base64
from datetime import datetime, timezone

import requests as _requests
from github import Github, NamedUser, GithubException

import config
from github_client import with_retry
from utils.helpers import get_logger, datetime_to_iso, safe_get

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Shared request helpers
# ─────────────────────────────────────────────────────────────────────────────

def _github_headers(token: str | None = None) -> dict:
    """
    Build GitHub API headers.

    Works even when token is missing, but authenticated requests are strongly
    preferred because GraphQL and rate limits depend on the token.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "skill-mining-profile-collector",
    }
    token = token or getattr(config, "GITHUB_TOKEN", None)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _run_graphql(query: str, variables: dict, token: str | None = None) -> dict:
    """
    Execute a GitHub GraphQL query and return the 'data' object.

    Returns {} on failure so the collector remains robust for public profiles.
    """
    token = token or getattr(config, "GITHUB_TOKEN", None)
    if not token:
        logger.warning("GitHub token missing; skipping GraphQL-only profile fields.")
        return {}

    try:
        resp = _requests.post(
            "https://api.github.com/graphql",
            json={"query": query, "variables": variables},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "skill-mining-profile-collector",
            },
            timeout=20,
        )

        if resp.status_code != 200:
            logger.warning(
                "GraphQL request failed: status=%s body=%s",
                resp.status_code,
                resp.text[:300],
            )
            return {}

        payload = resp.json()
        if payload.get("errors"):
            logger.warning("GraphQL errors: %s", payload.get("errors"))
            return {}

        return payload.get("data", {}) or {}

    except Exception as exc:
        logger.warning("GraphQL request failed: %s", exc)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Profile README
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_profile_readme(username: str) -> str:
    """
    Many developers maintain a profile README at <username>/<username>/README.md.

    Prefer the GitHub REST contents API so we do not need to guess whether the
    default branch is main or master. Fall back to raw URLs for resilience.
    """
    headers = _github_headers()

    # Preferred: GitHub Contents API
    try:
        url = f"https://api.github.com/repos/{username}/{username}/readme"
        resp = _requests.get(url, headers=headers, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            encoded = data.get("content", "")
            if encoded:
                raw = base64.b64decode(encoded).decode("utf-8", errors="replace")
                return raw[:5000]
    except Exception as exc:
        logger.debug("Profile README API fetch failed for %s: %s", username, exc)

    # Fallback: raw URLs for common branch names
    for branch in ("main", "master"):
        try:
            raw_url = f"https://raw.githubusercontent.com/{username}/{username}/{branch}/README.md"
            resp = _requests.get(raw_url, timeout=10)
            if resp.status_code == 200:
                return resp.text[:5000]
        except Exception:
            continue

    return ""


# ─────────────────────────────────────────────────────────────────────────────
# Pinned repositories / sponsorships through GraphQL
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_pinned_repos(username: str, token: str | None = None) -> list[dict]:
    """
    Fetch up to 6 pinned repositories via GitHub GraphQL API.

    Returns:
      [{name, full_name, description, url, stars, forks, language, is_fork}]
    """
    query = """
    query($login: String!) {
      user(login: $login) {
        pinnedItems(first: 6, types: REPOSITORY) {
          nodes {
            ... on Repository {
              name
              nameWithOwner
              description
              url
              stargazerCount
              forkCount
              isFork
              primaryLanguage { name }
              pushedAt
            }
          }
        }
      }
    }
    """

    data = _run_graphql(query, {"login": username}, token)
    nodes = (
        data.get("user", {})
        .get("pinnedItems", {})
        .get("nodes", [])
    )

    pinned: list[dict] = []
    for node in nodes:
        if not node:
            continue
        pinned.append({
            "name": node.get("name"),
            "full_name": node.get("nameWithOwner"),
            "description": node.get("description"),
            "url": node.get("url"),
            "stars": node.get("stargazerCount", 0),
            "forks": node.get("forkCount", 0),
            "language": (node.get("primaryLanguage") or {}).get("name"),
            "is_fork": node.get("isFork"),
            "pushed_at": node.get("pushedAt"),
        })

    return pinned


def _fetch_sponsorship_info(username: str, token: str | None = None) -> dict:
    """
    Use GraphQL to check sponsorship signals.

    Returns:
      {
        "is_sponsored": bool,
        "sponsoring_count": int
      }
    """
    query = """
    query($login: String!) {
      user(login: $login) {
        hasSponsorsListing
        sponsoring(first: 0) { totalCount }
      }
    }
    """

    data = _run_graphql(query, {"login": username}, token)
    user_data = data.get("user", {}) or {}

    return {
        "is_sponsored": bool(user_data.get("hasSponsorsListing", False)),
        "sponsoring_count": (
            user_data.get("sponsoring", {}) or {}
        ).get("totalCount", 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Organisations
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_organisations(client: Github, username: str) -> list[dict]:
    """
    Return list of public organisations the user belongs to.

    Each entry:
      {login, display_name, url, description}
    """
    try:
        user = with_retry(
            lambda: client.get_user(username),
            label=f"get_user({username})",
        )

        orgs: list[dict] = []
        for org in user.get_orgs():
            orgs.append({
                "login": safe_get(org, "login"),
                "display_name": safe_get(org, "name"),
                "url": safe_get(org, "html_url"),
                "description": safe_get(org, "description"),
            })

        return orgs

    except GithubException as exc:
        logger.warning("Could not fetch orgs for %s: %s", username, exc)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Main collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_profile(client: Github, username: str) -> dict:
    """
    Fetch and return structured profile metadata for a GitHub user.

    Parameters
    ----------
    client   : authenticated PyGitHub instance
    username : GitHub login name

    Returns
    -------
    dict with profile fields matching the output schema's `profile` section.
    """
    logger.info("Collecting profile for: %s", username)

    user: NamedUser = with_retry(
        lambda: client.get_user(username),
        label=f"get_user({username})",
    )

    created_at_iso = datetime_to_iso(safe_get(user, "created_at"))
    followers = safe_get(user, "followers", 0)
    following = safe_get(user, "following", 0)

    profile = {
        # v1 fields retained
        "login": safe_get(user, "login"),
        "name": safe_get(user, "name"),
        "bio": safe_get(user, "bio"),
        "company": safe_get(user, "company"),
        "blog": safe_get(user, "blog"),
        "location": safe_get(user, "location"),
        "email": safe_get(user, "email"),
        "twitter_username": safe_get(user, "twitter_username"),
        "followers": followers,
        "following": following,
        "public_repos": safe_get(user, "public_repos", 0),
        "public_gists": safe_get(user, "public_gists", 0),
        "created_at": created_at_iso,
        "updated_at": datetime_to_iso(safe_get(user, "updated_at")),
        "avatar_url": safe_get(user, "avatar_url"),
        "html_url": safe_get(user, "html_url"),
        "type": safe_get(user, "type"),
        "site_admin": safe_get(user, "site_admin", False),

        # v2 — career / discoverability signals
        "hireable": safe_get(user, "hireable", None),
        "years_on_github": _years_since_iso(created_at_iso),
        "followers_to_following_ratio": _safe_ratio(followers, following),
    }

    # Profile README: keep both names for compatibility.
    logger.debug("Fetching profile README for %s", username)
    profile_readme = _fetch_profile_readme(username)
    profile["profile_readme"] = profile_readme
    profile["profile_readme_content"] = profile_readme

    # Pinned repositories: self-curated best work.
    logger.debug("Fetching pinned repos for %s", username)
    pinned = _fetch_pinned_repos(username, getattr(config, "GITHUB_TOKEN", None))
    profile["pinned_repositories"] = pinned

    # Organisation memberships: keep both names for compatibility.
    logger.debug("Fetching organisations for %s", username)
    orgs = _fetch_organisations(client, username)
    profile["organisations"] = orgs
    profile["organisations_member_of"] = [
        org.get("login") for org in orgs if org.get("login")
    ]

    # Sponsorship signals.
    logger.debug("Fetching sponsorship info for %s", username)
    sponsorship = _fetch_sponsorship_info(username, getattr(config, "GITHUB_TOKEN", None))
    profile["is_sponsored"] = sponsorship["is_sponsored"]
    profile["sponsoring_count"] = sponsorship["sponsoring_count"]

    logger.debug("Profile collected (v2.1): %s", username)
    return profile


# ─────────────────────────────────────────────────────────────────────────────
# Private utilities
# ─────────────────────────────────────────────────────────────────────────────

def _years_since_iso(iso_str: str | None) -> int | None:
    """Return full years elapsed since an ISO-8601 date string."""
    if not iso_str:
        return None

    try:
        created = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return (now - created).days // 365
    except Exception:
        return None


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    """Return numerator / denominator rounded to 2 dp; None if denominator is 0."""
    if denominator == 0:
        return None
    return round(numerator / denominator, 2)
