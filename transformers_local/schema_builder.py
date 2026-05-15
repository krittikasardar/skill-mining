"""
transformers/schema_builder.py
-------------------------------
Assembles the final evidence-preserving JSON document for a single GitHub user.

v2 updates for the GitHub Profile Data Gap Analysis
---------------------------------------------------
Adds support for richer repo_collector.py output:
  - commit depth and contribution cadence aggregate signals
  - PR / issue / collaboration aggregate signals
  - framework, tool, CI/CD, test, release and dependency signals
  - commit and contribution evidence types in the evidence_index
  - optional merge of aggregate signals returned by collect_all_repos(...)

Output schema top-level sections
---------------------------------
  profile             – user metadata
  repositories        – per-repo evidence records (sorted by relevance)
  aggregate_signals   – cross-repo computed features
  evidence_index      – flat list of citable evidence snippets for RAG
  collection_metadata – run info, timestamps, API usage
"""

from collections import Counter, defaultdict
from typing import Optional, Any

from utils.helpers import get_logger, utcnow_iso, iso_to_year, years_since

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────────────────────

def _as_number(value: Any, default: float = 0) -> float:
    """Safely convert a numeric-ish value to float/int-compatible number."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _round_or_none(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def _top_counter(counter: Counter, limit: int = 20) -> list[dict]:
    """Return a stable list representation of a Counter."""
    return [
        {"name": name, "count": count}
        for name, count in counter.most_common(limit)
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Aggregate signal computation
# ─────────────────────────────────────────────────────────────────────────────

def _compute_aggregate_signals(repositories: list[dict]) -> dict:
    """
    Compute aggregate features across all collected repositories.

    This version keeps the original v1 aggregate fields and adds the fields
    needed by the data-gap analysis: commit depth, monthly activity, PR/issue
    collaboration, engineering maturity, and framework/tool depth.
    """
    if not repositories:
        return {}

    lang_bytes: Counter = Counter()
    topic_counter: Counter = Counter()

    framework_counter: Counter = Counter()
    devops_counter: Counter = Counter()
    testing_counter: Counter = Counter()
    ci_counter: Counter = Counter()
    cloud_counter: Counter = Counter()
    database_counter: Counter = Counter()
    api_pattern_counter: Counter = Counter()

    total_stars = 0
    total_forks_received = 0
    owned_count = 0
    forked_count = 0
    all_years: list[int] = []
    recent_repos: list[str] = []
    historical_repos: list[str] = []

    # v2 aggregate signals
    monthly_commit_heatmap: dict[str, int] = defaultdict(int)
    commit_frequency_all_years: dict[int, int] = defaultdict(int)
    total_commits_all_time = 0
    total_lines_added_all_repos = 0
    total_lines_deleted_all_repos = 0
    contribution_gap_values: list[float] = []
    commit_quality_scores: list[float] = []
    avg_commits_per_active_month_values: list[float] = []

    total_prs_opened = 0
    total_prs_closed = 0
    total_prs_merged = 0
    total_issues_opened = 0
    total_issues_closed = 0
    issue_close_time_values: list[float] = []

    repos_with_ci_cd = 0
    repos_with_tests = 0
    repos_with_contributing_guide = 0
    repos_with_code_of_conduct = 0
    repos_with_releases = 0
    total_dependencies_detected = 0
    total_contributors_across_repos = 0

    most_recent_commit_date = None
    repo_creation_years: list[int] = []

    for repo in repositories:
        meta = repo.get("repository_metadata", {})
        skill = repo.get("skill_evidence", {})
        role = repo.get("role_evidence", {})
        raw = repo.get("raw_text_evidence", {})

        full_name = meta.get("full_name")

        # Languages: aggregate bytes across repos
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

        # Active years from commit samples and repo dates
        for y in skill.get("commit_years_covered", []):
            if y:
                all_years.append(y)
        for ts_field in ("created_at", "pushed_at"):
            y = iso_to_year(meta.get(ts_field))
            if y:
                all_years.append(y)
        created_year = iso_to_year(meta.get("created_at"))
        if created_year:
            repo_creation_years.append(created_year)

        # Recent vs historical bucket
        pushed = meta.get("pushed_at")
        age_yrs = years_since(pushed) if pushed else 99
        if age_yrs <= 2:
            recent_repos.append(full_name)
        elif age_yrs >= 4:
            historical_repos.append(full_name)

        # v2: commit depth
        total_commit_count = (
            skill.get("total_commits_to_repo")
            if skill.get("total_commits_to_repo") is not None
            else skill.get("total_commit_count", 0)
        )
        total_commits_all_time += int(_as_number(total_commit_count, 0))
        total_lines_added_all_repos += int(_as_number(skill.get("total_lines_added", 0), 0))
        total_lines_deleted_all_repos += int(_as_number(skill.get("total_lines_deleted", 0), 0))

        for year, count in skill.get("commit_frequency_per_year", {}).items():
            try:
                commit_frequency_all_years[int(year)] += int(count)
            except (TypeError, ValueError):
                continue

        for ym, count in raw.get("_monthly_commit_counts", {}).items():
            monthly_commit_heatmap[ym] += int(_as_number(count, 0))

        if skill.get("contribution_gap_months") is not None:
            contribution_gap_values.append(_as_number(skill.get("contribution_gap_months"), 0))
        if skill.get("commit_message_quality_score") is not None:
            commit_quality_scores.append(_as_number(skill.get("commit_message_quality_score"), 0))
        if skill.get("avg_commits_per_active_month") is not None:
            avg_commits_per_active_month_values.append(
                _as_number(skill.get("avg_commits_per_active_month"), 0)
            )

        last_commit = skill.get("last_commit_date")
        if last_commit and (most_recent_commit_date is None or last_commit > most_recent_commit_date):
            most_recent_commit_date = last_commit

        # v2: collaboration / maintainer quality
        total_prs_opened += int(_as_number(role.get("pr_open_count", 0), 0))
        total_prs_closed += int(_as_number(role.get("pr_closed_count", 0), 0))
        total_prs_merged += int(_as_number(role.get("pr_merged_count", 0), 0))
        total_issues_opened += int(_as_number(role.get("issues_opened_count", 0), 0))
        total_issues_closed += int(_as_number(role.get("closed_issues_count", 0), 0))
        if role.get("avg_issue_close_time_days") is not None:
            issue_close_time_values.append(_as_number(role.get("avg_issue_close_time_days"), 0))

        # v2: engineering maturity
        if meta.get("has_ci_cd"):
            repos_with_ci_cd += 1
        if meta.get("has_tests"):
            repos_with_tests += 1
        if meta.get("has_contributing_guide"):
            repos_with_contributing_guide += 1
        if meta.get("has_code_of_conduct"):
            repos_with_code_of_conduct += 1
        if int(_as_number(meta.get("releases_count", 0), 0)) > 0:
            repos_with_releases += 1
        total_dependencies_detected += int(_as_number(meta.get("dependency_count", 0), 0))
        total_contributors_across_repos += int(_as_number(meta.get("contributor_count", 0), 0))

        # v2: skill depth / tooling
        framework_counter.update(skill.get("frameworks_detected", []))
        devops_counter.update(skill.get("devops_tools_detected", []))
        testing_counter.update(skill.get("testing_frameworks_detected", []))
        ci_counter.update(skill.get("ci_platforms_detected", []))
        cloud_counter.update(skill.get("cloud_platforms_detected", []))
        database_counter.update(skill.get("db_technologies_detected", []))
        api_pattern_counter.update(skill.get("api_patterns_detected", []))

    # Top languages by total bytes
    total_bytes = sum(lang_bytes.values()) or 1
    top_languages = [
        {"language": lang, "bytes": b, "pct": round(b / total_bytes * 100, 1)}
        for lang, b in lang_bytes.most_common(10)
    ]

    top_topics = [t for t, _ in topic_counter.most_common(20)]
    active_years = sorted(set(all_years))

    total_repos = len(repositories)
    pr_merge_rate_pct = (
        round(total_prs_merged / total_prs_closed * 100, 1)
        if total_prs_closed > 0 else None
    )

    return {
        # v1 fields preserved
        "top_languages": top_languages,
        "top_topics": top_topics,
        "total_stars_received": total_stars,
        "total_forks_received": total_forks_received,
        "total_repos_collected": total_repos,
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

        # v2 commit and temporal fields from the PDF gap analysis
        "total_commits_all_time": total_commits_all_time,
        "commit_frequency_all_years": dict(sorted(commit_frequency_all_years.items())),
        "monthly_commit_heatmap": dict(sorted(monthly_commit_heatmap.items())),
        "recent_6_month_commit_count": _recent_month_count(monthly_commit_heatmap, months=6),
        "most_recent_commit_date": most_recent_commit_date,
        "avg_commits_per_active_month": _round_or_none(
            sum(avg_commits_per_active_month_values) / len(avg_commits_per_active_month_values), 2
        ) if avg_commits_per_active_month_values else None,
        "avg_commit_message_quality_score": _round_or_none(
            sum(commit_quality_scores) / len(commit_quality_scores), 2
        ) if commit_quality_scores else None,
        "max_contribution_gap_months": int(max(contribution_gap_values)) if contribution_gap_values else 0,
        "avg_contribution_gap_months": _round_or_none(
            sum(contribution_gap_values) / len(contribution_gap_values), 2
        ) if contribution_gap_values else None,
        "repo_creation_cadence": dict(Counter(repo_creation_years)),

        # v2 PR / issue / collaboration fields
        "total_prs_opened": total_prs_opened,
        "total_prs_closed": total_prs_closed,
        "total_prs_merged": total_prs_merged,
        "pr_merge_rate_pct": pr_merge_rate_pct,
        "total_issues_opened": total_issues_opened,
        "total_issues_closed": total_issues_closed,
        "avg_issue_close_time_days": _round_or_none(
            sum(issue_close_time_values) / len(issue_close_time_values), 2
        ) if issue_close_time_values else None,
        "external_repo_contributions": forked_count,

        # v2 engineering maturity fields
        "repos_with_ci_cd": repos_with_ci_cd,
        "repos_with_tests": repos_with_tests,
        "repos_with_contributing_guide": repos_with_contributing_guide,
        "repos_with_code_of_conduct": repos_with_code_of_conduct,
        "repos_with_releases": repos_with_releases,
        "total_dependencies_detected": total_dependencies_detected,
        "total_contributors_across_repos": total_contributors_across_repos,

        # v2 skill depth fields
        "top_frameworks_detected": _top_counter(framework_counter, 20),
        "top_devops_tools_detected": _top_counter(devops_counter, 20),
        "top_testing_frameworks_detected": _top_counter(testing_counter, 20),
        "top_ci_platforms_detected": _top_counter(ci_counter, 20),
        "top_cloud_platforms_detected": _top_counter(cloud_counter, 20),
        "top_database_technologies_detected": _top_counter(database_counter, 20),
        "top_api_patterns_detected": _top_counter(api_pattern_counter, 20),
    }


def _recent_month_count(monthly: dict[str, int], months: int = 6) -> int:
    """
    Count commits in the most recent N active calendar months.

    This intentionally uses the latest month present in the collected data rather
    than wall-clock today, so old profiles still get a stable historical summary.
    """
    if not monthly:
        return 0

    sorted_months = sorted(monthly.keys())
    latest = sorted_months[-1]
    latest_year, latest_month = [int(x) for x in latest.split("-")]

    allowed = set()
    y, m = latest_year, latest_month
    for _ in range(months):
        allowed.add(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    return sum(count for ym, count in monthly.items() if ym in allowed)


# ─────────────────────────────────────────────────────────────────────────────
# Evidence index
# ─────────────────────────────────────────────────────────────────────────────

def _build_evidence_index(profile: dict, repositories: list[dict]) -> list[dict]:
    """
    Build a flat list of citable evidence snippets.

    Each entry has:
      - evidence_id   : unique string key
      - type          : skill | role | leadership | profile | commit | contribution
      - source        : e.g. "repo:torvalds/linux"
      - content       : evidence text
      - metadata      : supporting context

    v2 adds commit-level and contribution-level entries, matching the PDF's note
    that the evidence index should not stay limited to profile/skill/role/leadership.
    """
    index: list[dict] = []
    counter = 0

    def add(ev_type: str, source: str, content: str, metadata: dict):
        nonlocal counter
        if not content:
            return
        counter += 1
        index.append({
            "evidence_id": f"ev_{counter:04d}",
            "type": ev_type,
            "source": source,
            "content": content,
            "metadata": metadata,
        })

    login = profile.get("login", "unknown")
    profile_source = f"profile:{login}"

    # ── Profile evidence ───────────────────────────────────────────────────
    if profile.get("bio"):
        add("profile", profile_source, profile["bio"],
            {"field": "bio", "login": login})

    if profile.get("company"):
        add("profile", profile_source,
            f"Works at / affiliated with: {profile['company']}",
            {"field": "company", "login": login})

    if profile.get("hireable") is not None:
        add("profile", profile_source,
            f"Hireable flag on GitHub profile: {profile.get('hireable')}",
            {"field": "hireable", "login": login})

    organisations = profile.get("organisations_member_of") or profile.get("organizations") or []
    if organisations:
        add("profile", profile_source,
            f"GitHub organisation memberships: {', '.join(map(str, organisations))}",
            {"field": "organisations_member_of", "login": login})

    pinned = profile.get("pinned_repositories") or []
    if pinned:
        pinned_names = [p.get("name", p) if isinstance(p, dict) else p for p in pinned]
        add("profile", profile_source,
            f"Pinned repositories: {', '.join(map(str, pinned_names))}",
            {"field": "pinned_repositories", "login": login})

    profile_readme = profile.get("profile_readme_content") or profile.get("profile_readme_excerpt")
    if profile_readme:
        add("profile", profile_source, profile_readme,
            {"field": "profile_readme_content", "login": login})

    # ── Per-repo evidence ──────────────────────────────────────────────────
    for repo in repositories:
        meta = repo.get("repository_metadata", {})
        skill = repo.get("skill_evidence", {})
        role = repo.get("role_evidence", {})
        raw = repo.get("raw_text_evidence", {})
        leadership = repo.get("leadership_evidence", {})

        full_name = meta.get("full_name", "unknown")
        source = f"repo:{full_name}"

        # Description
        desc = meta.get("description")
        if desc:
            add("skill", source, desc, {
                "repo": full_name,
                "field": "description",
                "language": meta.get("language"),
                "stars": meta.get("stargazers_count"),
            })

        # README excerpt
        readme = raw.get("readme_excerpt")
        if readme:
            add("skill", source, readme, {
                "repo": full_name,
                "field": "readme_excerpt",
                "topics": meta.get("topics", []),
            })

        # Tech keywords from README
        kws = skill.get("readme_keywords", [])
        if kws:
            add("skill", source,
                f"Technologies/tools mentioned in README: {', '.join(kws)}",
                {"repo": full_name, "field": "readme_keywords"})

        # Language breakdown
        lang_bd = skill.get("language_breakdown", {})
        if lang_bd:
            lang_str = ", ".join(
                f"{lang} ({info.get('pct', 0)}%)" for lang, info in lang_bd.items()
            )
            add("skill", source, f"Languages used: {lang_str}",
                {"repo": full_name, "field": "language_breakdown"})

        # Topics
        topics = meta.get("topics", [])
        if topics:
            add("skill", source, f"Repository topics: {', '.join(topics)}",
                {"repo": full_name, "field": "topics"})

        # v2: detected frameworks/tools as explicit skill-depth evidence
        _add_detected_list(
            add, "skill", source, full_name,
            "frameworks_detected", "Frameworks detected", skill.get("frameworks_detected", [])
        )
        _add_detected_list(
            add, "skill", source, full_name,
            "devops_tools_detected", "DevOps tools detected", skill.get("devops_tools_detected", [])
        )
        _add_detected_list(
            add, "skill", source, full_name,
            "testing_frameworks_detected", "Testing frameworks detected", skill.get("testing_frameworks_detected", [])
        )
        _add_detected_list(
            add, "skill", source, full_name,
            "ci_platforms_detected", "CI platforms detected", skill.get("ci_platforms_detected", [])
        )
        _add_detected_list(
            add, "skill", source, full_name,
            "cloud_platforms_detected", "Cloud platforms detected", skill.get("cloud_platforms_detected", [])
        )
        _add_detected_list(
            add, "skill", source, full_name,
            "db_technologies_detected", "Database technologies detected", skill.get("db_technologies_detected", [])
        )
        _add_detected_list(
            add, "skill", source, full_name,
            "api_patterns_detected", "API design patterns detected", skill.get("api_patterns_detected", [])
        )

        if skill.get("documentation_quality_score") is not None:
            add("skill", source,
                f"Documentation quality score: {skill.get('documentation_quality_score')}/10.",
                {"repo": full_name, "field": "documentation_quality_score"})

        # v2: engineering maturity evidence
        maturity_parts = []
        if meta.get("has_ci_cd"):
            maturity_parts.append("CI/CD configured")
        if meta.get("has_tests"):
            maturity_parts.append("tests detected")
        if meta.get("has_contributing_guide"):
            maturity_parts.append("contributing guide present")
        if meta.get("has_code_of_conduct"):
            maturity_parts.append("code of conduct present")
        if meta.get("releases_count"):
            maturity_parts.append(f"{meta.get('releases_count')} releases")
        if meta.get("dependency_count"):
            maturity_parts.append(f"{meta.get('dependency_count')} dependencies detected")
        if maturity_parts:
            add("role", source,
                f"Engineering maturity signals for {full_name}: {', '.join(maturity_parts)}.",
                {
                    "repo": full_name,
                    "field": "engineering_maturity",
                    "has_ci_cd": meta.get("has_ci_cd"),
                    "has_tests": meta.get("has_tests"),
                    "releases_count": meta.get("releases_count"),
                    "dependency_count": meta.get("dependency_count"),
                })

        # Role / ownership
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

        # v2: contribution / collaboration evidence
        contribution_parts = []
        if role.get("pr_open_count") is not None:
            contribution_parts.append(f"open PRs: {role.get('pr_open_count')}")
        if role.get("pr_merged_count") is not None:
            contribution_parts.append(f"merged PRs: {role.get('pr_merged_count')}")
        if role.get("pr_merge_rate_pct") is not None:
            contribution_parts.append(f"PR merge rate: {role.get('pr_merge_rate_pct')}%")
        if role.get("issues_opened_count") is not None:
            contribution_parts.append(f"open issues: {role.get('issues_opened_count')}")
        if role.get("closed_issues_count") is not None:
            contribution_parts.append(f"closed issues: {role.get('closed_issues_count')}")
        if role.get("avg_issue_close_time_days") is not None:
            contribution_parts.append(f"avg issue close time: {role.get('avg_issue_close_time_days')} days")
        if role.get("contributor_count") is not None:
            contribution_parts.append(f"contributors: {role.get('contributor_count')}")
        if contribution_parts:
            add("contribution", source,
                f"Collaboration and maintainer signals for {full_name}: {', '.join(contribution_parts)}.",
                {"repo": full_name, "field": "collaboration_summary", **role})

        # Leadership signals
        for sig in leadership.get("signals", []):
            add("leadership", source, sig,
                {"repo": full_name,
                 "stars": meta.get("stargazers_count"),
                 "forks": meta.get("forks_count")})

        # v2: commit depth summary evidence
        total_commits = (
            skill.get("total_commits_to_repo")
            if skill.get("total_commits_to_repo") is not None
            else skill.get("total_commit_count")
        )
        commit_summary_parts = []
        if total_commits is not None:
            commit_summary_parts.append(f"total commits: {total_commits}")
        if skill.get("last_commit_date"):
            commit_summary_parts.append(f"last commit: {skill.get('last_commit_date')}")
        if skill.get("avg_commits_per_active_month") is not None:
            commit_summary_parts.append(
                f"avg commits per active month: {skill.get('avg_commits_per_active_month')}"
            )
        if skill.get("contribution_gap_months") is not None:
            commit_summary_parts.append(f"max contribution gap: {skill.get('contribution_gap_months')} months")
        if skill.get("commit_message_quality_score") is not None:
            commit_summary_parts.append(
                f"commit message quality score: {skill.get('commit_message_quality_score')}/10"
            )
        if commit_summary_parts:
            add("commit", source,
                f"Commit activity summary for {full_name}: {', '.join(commit_summary_parts)}.",
                {
                    "repo": full_name,
                    "field": "commit_activity_summary",
                    "total_commits_to_repo": total_commits,
                    "commit_frequency_per_year": skill.get("commit_frequency_per_year", {}),
                    "commit_size_distribution": skill.get("commit_size_distribution", {}),
                    "total_lines_added": skill.get("total_lines_added"),
                    "total_lines_deleted": skill.get("total_lines_deleted"),
                })

        # Commit samples as actual commit-level evidence, not generic skill evidence
        commits = raw.get("commit_samples", [])
        if commits:
            temporal_commits = [commits[0]]
            if len(commits) > 1:
                temporal_commits.append(commits[-1])
            for c in temporal_commits:
                if c.get("message"):
                    add("commit", source,
                        f"Commit ({c.get('date', 'unknown date')}): {c['message']}",
                        {"repo": full_name,
                         "sha": c.get("sha"),
                         "date": c.get("date"),
                         "additions": c.get("additions"),
                         "deletions": c.get("deletions"),
                         "field": "commit_sample"})

    return index


def _add_detected_list(add_fn, ev_type: str, source: str, repo_name: str,
                       field: str, label: str, values: list[str]) -> None:
    """Add one evidence item for a non-empty detected tooling list."""
    if values:
        add_fn(ev_type, source,
               f"{label}: {', '.join(values)}.",
               {"repo": repo_name, "field": field, "values": values})


# ─────────────────────────────────────────────────────────────────────────────
# Top-level schema assembler
# ─────────────────────────────────────────────────────────────────────────────

def build_schema(
    username: str,
    profile: dict,
    repositories: list[dict] | tuple[list[dict], dict],
    rate_limit_info: Optional[dict] = None,
    elapsed_seconds: Optional[float] = None,
    extra_aggregate_signals: Optional[dict] = None,
) -> dict:
    """
    Assemble the full evidence-preserving JSON document for one user.

    Parameters
    ----------
    username                 : GitHub login
    profile                  : dict from profile_collector
    repositories             : list of dicts from repo_collector. Also accepts
                               the v2 tuple returned by collect_all_repos:
                               (repositories, extra_aggregate_signals)
    rate_limit_info          : optional dict from github_client.log_rate_limit
    elapsed_seconds          : optional total collection time in seconds
    extra_aggregate_signals  : optional v2 aggregate dict from repo_collector

    Returns
    -------
    Full output schema dict, ready to be serialised to JSON.
    """
    logger.info("Building output schema for: %s", username)

    # Backward-compatible support for the v2 repo_collector return shape:
    # repositories, extra_aggregate = collect_all_repos(...)
    collector_extra_aggregate = {}
    if isinstance(repositories, tuple) and len(repositories) == 2:
        repositories, collector_extra_aggregate = repositories

    repositories = repositories or []

    aggregate = _compute_aggregate_signals(repositories)

    # Merge collector-provided aggregate signals last because those values may
    # be more exact than recomputing from stored samples.
    if collector_extra_aggregate:
        aggregate.update(collector_extra_aggregate)
    if extra_aggregate_signals:
        aggregate.update(extra_aggregate_signals)

    evidence_index = _build_evidence_index(profile, repositories)

    return {
        "schema_version": "1.1",
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
            "evidence_types": sorted({ev.get("type") for ev in evidence_index}),
            "rate_limit_snapshot": rate_limit_info or {},
            "collector_version": "2.0",
            "notes": (
                "Repositories are sorted by relevance_score (descending). "
                "All public repos are retained to preserve historical evidence. "
                "Evidence index includes profile, skill, role, leadership, commit, "
                "and contribution entries for stronger RAG grounding."
            ),
        },
    }
