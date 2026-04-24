"""
transformers/schema_builder.py
-------------------------------
Assembles the final evidence-preserving JSON document for a single GitHub user.

Output schema top-level sections
---------------------------------
  profile             – user metadata
  repositories        – per-repo evidence records (sorted by relevance)
  aggregate_signals   – cross-repo computed features
  evidence_index      – flat list of citable evidence snippets for RAG
  collection_metadata – run info, timestamps, API usage
"""

from collections import Counter
from typing import Optional

from utils.helpers import get_logger, utcnow_iso, iso_to_year, years_since

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Aggregate signal computation
# ─────────────────────────────────────────────────────────────────────────────

def _compute_aggregate_signals(repositories: list[dict]) -> dict:
    """
    Compute lightweight aggregate features across all collected repositories.
    These are designed to give downstream agents a quick overview without
    requiring them to read every repository record.
    """
    if not repositories:
        return {}

    lang_bytes: Counter = Counter()
    topic_counter: Counter = Counter()
    total_stars = 0
    total_forks_received = 0
    owned_count = 0
    forked_count = 0
    all_years: list[int] = []
    recent_repos = []
    historical_repos = []

    for repo in repositories:
        meta = repo.get("repository_metadata", {})
        skill = repo.get("skill_evidence", {})
        role = repo.get("role_evidence", {})

        # Languages (aggregate bytes across repos)
        for lang, info in skill.get("language_breakdown", {}).items():
            lang_bytes[lang] += info.get("bytes", 0)

        # Topics
        topic_counter.update(meta.get("topics", []))

        # Star / fork signals
        total_stars += meta.get("stargazers_count", 0)
        total_forks_received += meta.get("forks_count", 0)

        # Ownership
        if role.get("is_owner"):
            owned_count += 1
        else:
            forked_count += 1

        # Active years (from commit samples and repo dates)
        for y in skill.get("commit_years_covered", []):
            if y:
                all_years.append(y)
        for ts_field in ("created_at", "pushed_at"):
            y = iso_to_year(meta.get(ts_field))
            if y:
                all_years.append(y)

        # Recent vs historical bucket
        pushed = meta.get("pushed_at")
        age_yrs = years_since(pushed) if pushed else 99
        if age_yrs <= 2:
            recent_repos.append(meta.get("full_name"))
        elif age_yrs >= 4:
            historical_repos.append(meta.get("full_name"))

    # Top languages by total bytes
    total_bytes = sum(lang_bytes.values()) or 1
    top_languages = [
        {"language": lang, "bytes": b, "pct": round(b / total_bytes * 100, 1)}
        for lang, b in lang_bytes.most_common(10)
    ]

    # Top topics
    top_topics = [t for t, _ in topic_counter.most_common(20)]

    active_years = sorted(set(all_years))

    return {
        "top_languages": top_languages,
        "top_topics": top_topics,
        "total_stars_received": total_stars,
        "total_forks_received": total_forks_received,
        "total_repos_collected": len(repositories),
        "owned_repo_count": owned_count,
        "forked_repo_count": forked_count,
        "active_years": active_years,
        "first_active_year": min(active_years) if active_years else None,
        "last_active_year": max(active_years) if active_years else None,
        "activity_span_years": (
            max(active_years) - min(active_years) if len(active_years) > 1 else 0
        ),
        "recent_activity_summary": {
            "repos_pushed_last_2_years": len(recent_repos),
            "repo_names": recent_repos[:10],
        },
        "historical_activity_summary": {
            "repos_4_plus_years_old": len(historical_repos),
            "repo_names": historical_repos[:10],
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Evidence index
# ─────────────────────────────────────────────────────────────────────────────

def _build_evidence_index(profile: dict, repositories: list[dict]) -> list[dict]:
    """
    Build a flat list of citable evidence snippets.

    Each entry has:
      - evidence_id   : unique string key
      - type          : skill | role | leadership | profile
      - source        : e.g. "repo:torvalds/linux"
      - content       : the evidence text
      - metadata      : dict of supporting context

    This index is designed to be chunked and embedded for RAG retrieval.
    """
    index: list[dict] = []
    counter = 0

    def add(ev_type: str, source: str, content: str, metadata: dict):
        nonlocal counter
        counter += 1
        index.append({
            "evidence_id": f"ev_{counter:04d}",
            "type": ev_type,
            "source": source,
            "content": content,
            "metadata": metadata,
        })

    # ── Profile bio as evidence ────────────────────────────────────────────
    if profile.get("bio"):
        add("profile", f"profile:{profile['login']}", profile["bio"],
            {"field": "bio", "login": profile["login"]})

    if profile.get("company"):
        add("profile", f"profile:{profile['login']}",
            f"Works at / affiliated with: {profile['company']}",
            {"field": "company"})

    # ── Per-repo evidence ──────────────────────────────────────────────────
    for repo in repositories:
        meta = repo.get("repository_metadata", {})
        full_name = meta.get("full_name", "unknown")
        source = f"repo:{full_name}"

        # Description
        desc = meta.get("description")
        if desc:
            add("skill", source, desc, {
                "repo": full_name, "field": "description",
                "language": meta.get("language"),
                "stars": meta.get("stargazers_count"),
            })

        # README excerpt
        readme = repo.get("raw_text_evidence", {}).get("readme_excerpt")
        if readme:
            add("skill", source, readme, {
                "repo": full_name, "field": "readme_excerpt",
                "topics": meta.get("topics", []),
            })

        # Tech keywords from README
        kws = repo.get("skill_evidence", {}).get("readme_keywords", [])
        if kws:
            add("skill", source,
                f"Technologies/tools mentioned in README: {', '.join(kws)}",
                {"repo": full_name, "field": "readme_keywords"})

        # Language breakdown
        lang_bd = repo.get("skill_evidence", {}).get("language_breakdown", {})
        if lang_bd:
            lang_str = ", ".join(
                f"{lang} ({info['pct']}%)" for lang, info in lang_bd.items()
            )
            add("skill", source, f"Languages used: {lang_str}",
                {"repo": full_name, "field": "language_breakdown"})

        # Topics
        topics = meta.get("topics", [])
        if topics:
            add("skill", source, f"Repository topics: {', '.join(topics)}",
                {"repo": full_name, "field": "topics"})

        # Role
        role = repo.get("role_evidence", {})
        if role.get("is_owner"):
            add("role", source,
                f"User is the owner of {full_name}.",
                {"repo": full_name, "is_owner": True,
                 "stars": meta.get("stargazers_count"),
                 "forks": meta.get("forks_count")})
        elif role.get("forked_from"):
            add("role", source,
                f"User forked {full_name} from {role['forked_from']}, indicating contributor role.",
                {"repo": full_name, "is_owner": False,
                 "forked_from": role["forked_from"]})

        # Leadership signals
        leadership_sigs = repo.get("leadership_evidence", {}).get("signals", [])
        for sig in leadership_sigs:
            add("leadership", source, sig,
                {"repo": full_name,
                 "stars": meta.get("stargazers_count"),
                 "forks": meta.get("forks_count")})

        # Commit samples (select first + last as temporal anchors)
        commits = repo.get("raw_text_evidence", {}).get("commit_samples", [])
        if commits:
            temporal_commits = [commits[0]]
            if len(commits) > 1:
                temporal_commits.append(commits[-1])
            for c in temporal_commits:
                if c.get("message"):
                    add("skill", source,
                        f"Commit ({c.get('date', 'unknown date')}): {c['message']}",
                        {"repo": full_name, "sha": c.get("sha"),
                         "date": c.get("date"), "field": "commit_sample"})

    return index


# ─────────────────────────────────────────────────────────────────────────────
# Top-level schema assembler
# ─────────────────────────────────────────────────────────────────────────────

def build_schema(
    username: str,
    profile: dict,
    repositories: list[dict],
    rate_limit_info: Optional[dict] = None,
    elapsed_seconds: Optional[float] = None,
) -> dict:
    """
    Assemble the full evidence-preserving JSON document for one user.

    Parameters
    ----------
    username         : GitHub login
    profile          : dict from profile_collector
    repositories     : list of dicts from repo_collector
    rate_limit_info  : optional dict from github_client.log_rate_limit
    elapsed_seconds  : optional total collection time in seconds

    Returns
    -------
    Full output schema dict, ready to be serialised to JSON.
    """
    logger.info("Building output schema for: %s", username)

    aggregate = _compute_aggregate_signals(repositories)
    evidence_index = _build_evidence_index(profile, repositories)

    return {
        "schema_version": "1.0",
        "profile": profile,
        "repositories": repositories,
        "aggregate_signals": aggregate,
        "evidence_index": evidence_index,
        "collection_metadata": {
            "username": username,
            "collected_at": utcnow_iso(),
            "elapsed_seconds": elapsed_seconds,
            "total_repos": len(repositories),
            "total_evidence_items": len(evidence_index),
            "rate_limit_snapshot": rate_limit_info or {},
            "collector_version": "1.0",
            "notes": (
                "Repositories are sorted by relevance_score (descending). "
                "All public repos are retained — none are discarded — to preserve historical evidence."
            ),
        },
    }
