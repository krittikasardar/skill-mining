"""
github_client.py
----------------
Builds and exposes a PyGitHub client and shared GitHub API helpers.

v2-compatible changes
---------------------
- build_client(token=None) accepts an optional token, so it works with main.py.
- Allows unauthenticated GitHub usage, but warns about stricter rate limits.
- Uses config REST timeout values when available.
- Adds consistent GitHub REST headers helper.
- Makes rate-limit helpers resilient to missing token / failed requests.
- Keeps old public functions: build_client, log_rate_limit, wait_for_rate_limit,
  with_retry, cached.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import requests
from github import Github, GithubException, RateLimitExceededException
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import config
from utils.helpers import get_logger, utcnow_iso

logger = get_logger(__name__)

REST_TIMEOUT_SECONDS = int(getattr(config, "REST_TIMEOUT_SECONDS", 10))


# Optional disk cache
_cache = None
if getattr(config, "ENABLE_CACHE", False):
    try:
        import diskcache

        _cache = diskcache.Cache(str(config.CACHE_DIR))
        logger.info("Disk cache enabled at %s", config.CACHE_DIR)
    except ImportError:
        logger.warning("diskcache not installed; caching disabled.")


def _get_cache_key(label: str) -> str:
    """Return a stable cache key for the given label."""
    return hashlib.md5(label.encode("utf-8")).hexdigest()


def cached(label: str, ttl: int | None = None):
    """Decorator: cache the return value of a callable if disk cache is enabled."""
    ttl = ttl if ttl is not None else int(getattr(config, "CACHE_TTL_SECONDS", 86400))

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            if _cache is None:
                return fn(*args, **kwargs)
            key = _get_cache_key(label + str(args) + str(kwargs))
            if key in _cache:
                logger.debug("Cache hit: %s", label)
                return _cache[key]
            result = fn(*args, **kwargs)
            _cache.set(key, result, expire=ttl)
            return result

        return wrapper

    return decorator


def github_headers(token: str | None = None) -> dict[str, str]:
    """
    Return standard headers for GitHub REST/GraphQL calls.

    Works with or without a token. If no token is provided, GitHub will apply
    unauthenticated rate limits.
    """
    token = token if token is not None else getattr(config, "GITHUB_TOKEN", "")
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "skill-mining-collector",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


# GitHub client builder

def build_client(token: str | None = None) -> Github:
    """
    Build and return a PyGitHub client.

    Parameters
    ----------
    token : optional GitHub token. If omitted, config.GITHUB_TOKEN is used.

    Notes
    -----
    Earlier code called build_client() with no arguments. Updated main.py may
    call build_client(config.GITHUB_TOKEN). This signature supports both.
    """
    token = token if token is not None else getattr(config, "GITHUB_TOKEN", "")

    if token:
        client = Github(token, per_page=100, retry=3, timeout=REST_TIMEOUT_SECONDS)
        logger.info("GitHub client initialised with token.")
    else:
        client = Github(per_page=100, retry=3, timeout=REST_TIMEOUT_SECONDS)
        logger.warning(
            "GitHub client initialised without token. Unauthenticated rate limits are much lower."
        )

    return client


# Rate-limit helpers

def _iso_from_epoch(ts: int | float | None) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else ""


def log_rate_limit(client: Github | None = None) -> dict:
    """Log current GitHub rate-limit status and return it as a dict."""
    try:
        resp = requests.get(
            "https://api.github.com/rate_limit",
            headers=github_headers(),
            timeout=REST_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        resources = resp.json().get("resources", {})
    except Exception as exc:
        logger.warning("Could not fetch rate limit via REST API: %s", exc)
        if client is not None:
            try:
                rl = client.get_rate_limit()
                info = {
                    "core": {
                        "limit": rl.core.limit,
                        "remaining": rl.core.remaining,
                        "reset": rl.core.reset.isoformat(),
                    },
                    "search": {
                        "limit": rl.search.limit,
                        "remaining": rl.search.remaining,
                        "reset": rl.search.reset.isoformat(),
                    },
                    "checked_at": utcnow_iso(),
                }
                return info
            except Exception:
                pass
        return {"checked_at": utcnow_iso(), "error": str(exc)}

    core = resources.get("core", {})
    search = resources.get("search", {})
    graphql = resources.get("graphql", {})

    info = {
        "core": {
            "limit": core.get("limit", 0),
            "remaining": core.get("remaining", 0),
            "reset": _iso_from_epoch(core.get("reset")),
        },
        "search": {
            "limit": search.get("limit", 0),
            "remaining": search.get("remaining", 0),
            "reset": _iso_from_epoch(search.get("reset")),
        },
        "graphql": {
            "limit": graphql.get("limit", 0),
            "remaining": graphql.get("remaining", 0),
            "reset": _iso_from_epoch(graphql.get("reset")),
        },
        "checked_at": utcnow_iso(),
    }

    logger.info(
        "Rate limit — core: %s/%s (resets %s), search: %s/%s",
        info["core"]["remaining"],
        info["core"]["limit"],
        info["core"]["reset"],
        info["search"]["remaining"],
        info["search"]["limit"],
    )
    return info


def wait_for_rate_limit(client: Github | None = None, buffer: int = 10) -> None:
    """
    Sleep until the GitHub core rate limit has at least `buffer` requests left.
    Returns immediately if rate-limit data cannot be read.
    """
    try:
        resp = requests.get(
            "https://api.github.com/rate_limit",
            headers=github_headers(),
            timeout=REST_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        core = resp.json().get("resources", {}).get("core", {})
        remaining = int(core.get("remaining", 999))
        reset_ts = int(core.get("reset", 0))
    except Exception as exc:
        logger.warning("Could not check rate limit; continuing cautiously: %s", exc)
        return

    if remaining <= buffer:
        wait_secs = max(0, reset_ts - time.time()) + 5
        logger.warning(
            "Rate limit nearly exhausted (%d remaining). Sleeping %.0f seconds until reset.",
            remaining,
            wait_secs,
        )
        time.sleep(wait_secs)


# Retry wrapper

T = TypeVar("T")


def with_retry(fn: Callable[[], T], label: str = "") -> T:
    """
    Call `fn()` with exponential backoff retry on rate-limit errors.

    Non-rate-limit GithubExceptions are re-raised after logging, because those
    usually indicate a real request issue such as missing repository access.
    """

    @retry(
        retry=retry_if_exception_type(RateLimitExceededException),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=10, max=120),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _inner() -> T:
        try:
            return fn()
        except RateLimitExceededException:
            logger.warning("Rate limit exceeded during '%s'. Retrying...", label)
            raise
        except GithubException as exc:
            logger.error("GithubException in '%s': %s", label, exc)
            raise

    return _inner()
