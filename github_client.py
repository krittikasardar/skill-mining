"""
github_client.py:

Builds and exposes an authenticated PyGitHub client.
Includes:
  - Rate-limit inspection helpers
  - Retry wrapper via tenacity
  - Optional disk cache for API responses
"""

import time
import json
import hashlib
from typing import Any, Callable, Optional, TypeVar

from github import Github, RateLimitExceededException, GithubException
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

import config
from utils.helpers import get_logger, utcnow_iso

logger = get_logger(__name__)

# Optional disk cache (diskcache library)
_cache = None
if config.ENABLE_CACHE:
    try:
        import diskcache
        _cache = diskcache.Cache(str(config.CACHE_DIR))
        logger.info("Disk cache enabled at %s", config.CACHE_DIR)
    except ImportError:
        logger.warning("diskcache not installed; caching disabled.")


def _get_cache_key(label: str) -> str:
    return hashlib.md5(label.encode()).hexdigest()


def cached(label: str, ttl: int = config.CACHE_TTL_SECONDS):
    """Decorator: cache the return value of a zero-argument callable."""
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


# GitHub client singleton
def build_client() -> Github:
    """
    Build and return an authenticated PyGitHub client.
    Raises RuntimeError if the token is missing.
    """
    token = config.GITHUB_TOKEN
    if not token:
        raise RuntimeError(
            "GITHUB_TOKEN is not set. "
            "Copy .env.example to .env and fill in your token."
        )
    client = Github(token, per_page=100, retry=3)
    logger.info("GitHub client initialised.")
    return client


# Rate-limit helpers

def log_rate_limit(client: Github) -> dict:
    """Log current rate limit status and return it as a dict."""
    import requests as _requests
    from datetime import datetime, timezone as _tz

    resp = _requests.get(
        "https://api.github.com/rate_limit",
        headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
        timeout=10,
    )
    data = resp.json().get("resources", {})
    core = data.get("core", {})
    search = data.get("search", {})

    def _iso(ts):
        return datetime.fromtimestamp(ts, tz=_tz.utc).isoformat() if ts else ""

    info = {
        "core": {
            "limit": core.get("limit", 0),
            "remaining": core.get("remaining", 0),
            "reset": _iso(core.get("reset")),
        },
        "search": {
            "limit": search.get("limit", 0),
            "remaining": search.get("remaining", 0),
            "reset": _iso(search.get("reset")),
        },
        "checked_at": utcnow_iso(),
    }
    logger.info(
        "Rate limit — core: %d/%d (resets %s)",
        info["core"]["remaining"], info["core"]["limit"], info["core"]["reset"],
    )
    return info


def wait_for_rate_limit(client: Github, buffer: int = 10) -> None:
    """
    Block until the core rate limit has at least `buffer` requests remaining.
    """
    import requests as _requests
    resp = _requests.get(
        "https://api.github.com/rate_limit",
        headers={"Authorization": f"token {config.GITHUB_TOKEN}"},
        timeout=10,
    )
    remaining = resp.json().get("resources", {}).get("core", {}).get("remaining", 999)
    reset_ts = resp.json().get("resources", {}).get("core", {}).get("reset", 0)

    if remaining <= buffer:
        wait_secs = max(0, reset_ts - time.time()) + 5
        logger.warning(
            "Rate limit nearly exhausted (%d remaining). "
            "Sleeping %.0f seconds until reset.",
            remaining, wait_secs,
        )
        time.sleep(wait_secs)


# Retry wrapper

T = TypeVar("T")

def with_retry(fn: Callable[[], T], label: str = "") -> T:
    """
    Call `fn()` with exponential backoff retry on rate-limit errors.
    Non-rate-limit GithubExceptions are re-raised immediately.
    """
    @retry(
        retry=retry_if_exception_type(RateLimitExceededException),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=10, max=120),
        before_sleep=before_sleep_log(logger, 20),  # 20 = logging.WARNING
        reraise=True,
    )
    def _inner():
        try:
            return fn()
        except RateLimitExceededException:
            raise
        except GithubException as exc:
            logger.error("GithubException in '%s': %s", label, exc)
            raise

    return _inner()
