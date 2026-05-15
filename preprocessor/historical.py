"""
preprocessor/historical.py
--------------------------
Derives temporal and technology-evolution signals from repository data.

Analyses produced:
  commits_by_year              : commit count per calendar year
  languages_by_year            : dominant languages per year (by repo push date)
  activity_trend               : 'growing' | 'stable' | 'declining'
  peak_activity_year           : year with the highest commit count
  tech_evolution               : language shifts between early and recent activity periods

v2 additions for the GitHub profile data-gap fix:
  monthly_commit_heatmap       : {YYYY-MM: count}
  weekday_vs_weekend_ratio     : weekday commits / weekend commits
  most_active_month_of_year    : month number and month name with highest activity
  inactive_periods             : gaps longer than 3 months between active months
  recent_6_month_commit_count  : recency signal
  repo_creation_cadence        : repository creation counts by year and average repos/year
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from utils.helpers import get_logger, iso_to_year

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-like datetime string safely."""
    if not value:
        return None
    try:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _month_add(dt: datetime, months: int) -> datetime:
    """Add months to a datetime, keeping the day at 1 to avoid month-end issues."""
    month_index = (dt.year * 12 + dt.month - 1) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return datetime(year, month, 1, tzinfo=dt.tzinfo)


def _iter_commit_dates(repositories: list[dict]) -> list[datetime]:
    """
    Return parsed commit datetimes from commit samples.

    Note:
    The collector now also exposes aggregate monthly counts. For weekday/weekend
    analysis we still need individual dates, so this function uses commit samples.
    """
    dates: list[datetime] = []
    for repo in repositories:
        samples = repo.get("raw_text_evidence", {}).get("commit_samples", [])
        for commit in samples:
            dt = _parse_datetime(commit.get("date"))
            if dt:
                dates.append(dt)
    return dates


# ─────────────────────────────────────────────────────────────────────────────
# Existing analyses, upgraded to use richer collector fields when available
# ─────────────────────────────────────────────────────────────────────────────

def commits_by_year(repositories: list[dict]) -> dict[int, int]:
    """
    Count commits per calendar year across all repositories.

    Preference order:
      1. skill_evidence.commit_frequency_per_year from the updated repo collector
      2. raw_text_evidence.commit_samples fallback
    """
    counts: Counter = Counter()

    for repo in repositories:
        skill = repo.get("skill_evidence", {})
        freq = skill.get("commit_frequency_per_year", {})

        if freq:
            for year, count in freq.items():
                try:
                    counts[int(year)] += int(count)
                except Exception:
                    continue
            continue

        # Fallback for old collector output
        samples = repo.get("raw_text_evidence", {}).get("commit_samples", [])
        for commit in samples:
            year = iso_to_year(commit.get("date"))
            if year:
                counts[year] += 1

    return dict(sorted(counts.items()))


def languages_by_year(repositories: list[dict]) -> dict[int, list[dict]]:
    """
    Associate each repo's language byte counts with its pushed_at year.
    Returns {year: [{language, bytes, pct}, ...]} sorted by bytes descending.
    """
    year_bytes: dict[int, Counter] = defaultdict(Counter)

    for repo in repositories:
        meta = repo.get("repository_metadata", {})
        year = iso_to_year(meta.get("pushed_at")) or iso_to_year(meta.get("created_at"))
        if not year:
            continue

        for lang, info in (
            repo.get("skill_evidence", {}).get("language_breakdown", {}).items()
        ):
            year_bytes[year][lang] += info.get("bytes", 0)

    result: dict[int, list[dict]] = {}
    for year, lang_counter in sorted(year_bytes.items()):
        total = sum(lang_counter.values()) or 1
        result[year] = [
            {"language": lang, "bytes": b, "pct": round(b / total * 100, 1)}
            for lang, b in lang_counter.most_common(5)
        ]

    return result


def compute_trend(cby: dict[int, int]) -> str:
    """
    Classify overall commit activity as 'growing', 'stable', or 'declining'.
    Compares the mean commit count in the earlier half of active years to the later half.
    """
    if len(cby) < 2:
        return "stable"

    years = sorted(cby.keys())
    mid = len(years) // 2

    early_avg = sum(cby[y] for y in years[:mid]) / mid
    late_avg = sum(cby[y] for y in years[mid:]) / (len(years) - mid)

    if early_avg == 0:
        return "growing" if late_avg > 0 else "stable"

    ratio = late_avg / early_avg

    if ratio >= 1.2:
        return "growing"
    if ratio <= 0.8:
        return "declining"
    return "stable"


def tech_evolution(lby: dict[int, list[dict]]) -> list[dict]:
    """
    Summarise language shifts between the earliest and most recent activity periods.
    Uses the first 40% of active years as 'early' and the last 40% as 'recent'.
    """
    if not lby:
        return []

    years = sorted(lby.keys())
    n = len(years)
    cutoff = max(1, int(n * 0.4))

    early_years = years[:cutoff]
    recent_years = years[n - cutoff:]

    def top_langs(bucket: list[int]) -> list[str]:
        agg: Counter = Counter()
        for y in bucket:
            for entry in lby.get(y, []):
                agg[entry["language"]] += entry["bytes"]
        return [lang for lang, _ in agg.most_common(5)]

    early_langs = top_langs(early_years)
    recent_langs = top_langs(recent_years)

    evolution = []

    if early_years:
        evolution.append({
            "period": f"early ({early_years[0]}–{early_years[-1]})",
            "dominant_languages": early_langs,
        })

    if recent_years and recent_years != early_years:
        evolution.append({
            "period": f"recent ({recent_years[0]}–{recent_years[-1]})",
            "dominant_languages": recent_langs,
        })

    new_langs = [lang for lang in recent_langs if lang not in early_langs]
    dropped_langs = [lang for lang in early_langs if lang not in recent_langs]

    if new_langs or dropped_langs:
        evolution.append({
            "new_languages": new_langs,
            "dropped_languages": dropped_langs,
        })

    return evolution


# ─────────────────────────────────────────────────────────────────────────────
# v2 temporal activity analyses
# ─────────────────────────────────────────────────────────────────────────────

def monthly_commit_heatmap(repositories: list[dict]) -> dict[str, int]:
    """
    Build {YYYY-MM: count} across all repositories.

    Preference order:
      1. raw_text_evidence._monthly_commit_counts from updated repo_collector.py
      2. raw_text_evidence.commit_samples fallback
    """
    counts: Counter = Counter()

    for repo in repositories:
        raw = repo.get("raw_text_evidence", {})
        monthly = raw.get("_monthly_commit_counts", {})

        if monthly:
            for ym, count in monthly.items():
                if ym:
                    counts[str(ym)] += int(count or 0)
            continue

        # Fallback for old collector output
        for commit in raw.get("commit_samples", []):
            date = commit.get("date")
            if date and len(date) >= 7:
                counts[date[:7]] += 1

    return dict(sorted(counts.items()))


def weekday_vs_weekend_ratio(repositories: list[dict]) -> dict:
    """
    Compute weekday/weekend commit distribution from available commit samples.

    Returns both counts and ratio because ratio alone is hard to interpret.
    """
    dates = _iter_commit_dates(repositories)

    weekday = 0
    weekend = 0

    for dt in dates:
        if dt.weekday() < 5:
            weekday += 1
        else:
            weekend += 1

    ratio = None if weekend == 0 else round(weekday / weekend, 2)

    return {
        "weekday_commits_sampled": weekday,
        "weekend_commits_sampled": weekend,
        "weekday_vs_weekend_ratio": ratio,
        "interpretation": (
            "No weekend commits in sample; ratio unavailable."
            if weekend == 0 else
            "Ratio = weekday commits divided by weekend commits."
        ),
    }


def most_active_month_of_year(monthly_counts: dict[str, int]) -> dict | None:
    """
    Return the calendar month with the highest total commit count across years.
    Example: if March is most active across 2022, 2023, 2024, returns month=3.
    """
    if not monthly_counts:
        return None

    month_totals: Counter = Counter()

    for ym, count in monthly_counts.items():
        try:
            month = int(str(ym).split("-")[1])
            month_totals[month] += int(count)
        except Exception:
            continue

    if not month_totals:
        return None

    month = month_totals.most_common(1)[0][0]
    count = month_totals[month]

    month_name = datetime(2000, month, 1).strftime("%B")

    return {
        "month": month,
        "month_name": month_name,
        "commit_count": count,
    }


def inactive_periods(monthly_counts: dict[str, int], min_gap_months: int = 3) -> list[dict]:
    """
    Return gaps longer than `min_gap_months` between active months.

    Example:
      active in 2022-01 and then 2022-06 means inactive months are Feb-May = 4.
    """
    if not monthly_counts:
        return []

    active_months = sorted(monthly_counts.keys())
    gaps: list[dict] = []

    for i in range(1, len(active_months)):
        prev = datetime.strptime(active_months[i - 1], "%Y-%m")
        curr = datetime.strptime(active_months[i], "%Y-%m")

        delta_months = (curr.year - prev.year) * 12 + (curr.month - prev.month)
        missing_months = delta_months - 1

        if missing_months > min_gap_months:
            gap_start = _month_add(prev, 1).strftime("%Y-%m")
            gap_end = _month_add(curr, -1).strftime("%Y-%m")
            gaps.append({
                "from": gap_start,
                "to": gap_end,
                "gap_months": missing_months,
                "previous_active_month": active_months[i - 1],
                "next_active_month": active_months[i],
            })

    return gaps


def recent_6_month_commit_count(monthly_counts: dict[str, int]) -> int:
    """Count commits in the most recent 6 months relative to the current date."""
    if not monthly_counts:
        return 0

    now = datetime.now(timezone.utc)
    cutoff = datetime(now.year, now.month, 1, tzinfo=timezone.utc) - timedelta(days=183)
    cutoff_ym = cutoff.strftime("%Y-%m")

    return sum(int(count) for ym, count in monthly_counts.items() if ym >= cutoff_ym)


def repo_creation_cadence(repositories: list[dict]) -> dict:
    """
    Summarise how often the user creates repositories.

    Returns:
      repos_created_by_year : {year: count}
      first_repo_year
      latest_repo_year
      active_creation_years
      avg_repos_created_per_year
    """
    by_year: Counter = Counter()

    for repo in repositories:
        meta = repo.get("repository_metadata", {})
        year = iso_to_year(meta.get("created_at"))
        if year:
            by_year[year] += 1

    if not by_year:
        return {
            "repos_created_by_year": {},
            "first_repo_year": None,
            "latest_repo_year": None,
            "active_creation_years": 0,
            "avg_repos_created_per_year": 0.0,
        }

    years = sorted(by_year.keys())
    span = years[-1] - years[0] + 1

    return {
        "repos_created_by_year": dict(sorted(by_year.items())),
        "first_repo_year": years[0],
        "latest_repo_year": years[-1],
        "active_creation_years": span,
        "avg_repos_created_per_year": round(sum(by_year.values()) / span, 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Top-level historical analysis
# ─────────────────────────────────────────────────────────────────────────────

def build_historical_analysis(repositories: list[dict]) -> dict:
    """Produce the full historical analysis dict for one user's repositories."""
    cby = commits_by_year(repositories)
    lby = languages_by_year(repositories)
    trend = compute_trend(cby)
    peak = max(cby, key=cby.get) if cby else None

    monthly = monthly_commit_heatmap(repositories)

    return {
        # Existing outputs
        "commits_by_year": cby,
        "languages_by_year": lby,
        "activity_trend": trend,
        "peak_activity_year": peak,
        "tech_evolution": tech_evolution(lby),

        # v2 temporal fields requested by the data-gap analysis
        "monthly_commit_heatmap": monthly,
        "weekday_vs_weekend_ratio": weekday_vs_weekend_ratio(repositories),
        "most_active_month_of_year": most_active_month_of_year(monthly),
        "inactive_periods": inactive_periods(monthly, min_gap_months=3),
        "recent_6_month_commit_count": recent_6_month_commit_count(monthly),
        "repo_creation_cadence": repo_creation_cadence(repositories),
    }
