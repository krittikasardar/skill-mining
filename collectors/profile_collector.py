"""
collectors/profile_collector.py
--------------------------------
Collects public user profile metadata from the GitHub API.
"""

from github import Github, NamedUser

from github_client import with_retry
from utils.helpers import get_logger, datetime_to_iso, safe_get

logger = get_logger(__name__)


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

    profile = {
        "login": safe_get(user, "login"),
        "name": safe_get(user, "name"),
        "bio": safe_get(user, "bio"),
        "company": safe_get(user, "company"),
        "blog": safe_get(user, "blog"),
        "location": safe_get(user, "location"),
        "email": safe_get(user, "email"),        # only available if user makes it public
        "twitter_username": safe_get(user, "twitter_username"),
        "followers": safe_get(user, "followers", 0),
        "following": safe_get(user, "following", 0),
        "public_repos": safe_get(user, "public_repos", 0),
        "public_gists": safe_get(user, "public_gists", 0),
        "created_at": datetime_to_iso(safe_get(user, "created_at")),
        "updated_at": datetime_to_iso(safe_get(user, "updated_at")),
        "avatar_url": safe_get(user, "avatar_url"),
        "html_url": safe_get(user, "html_url"),
        "type": safe_get(user, "type"),          # "User" or "Organization"
        "site_admin": safe_get(user, "site_admin", False),
    }

    logger.debug("Profile collected: %s", username)
    return profile
