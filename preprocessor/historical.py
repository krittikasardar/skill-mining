"""
preprocessor/historical.py
--------------------------
Derives temporal and technology-evolution signals from repository data.

Analyses produced:
  commits_by_year    : sampled commit count per calendar year
  languages_by_year  : dominant languages per year (by repo push date)
  activity_trend     : 'growing' | 'stable' | 'declining'
  peak_activity_year : year with the highest sampled commit count
  tech_evolution     : language shifts between early and recent activity periods
"""

from collections import Counter, defaultdict
from utils.helpers import get_logger, iso_to_year

logger = get_logger(__name__)


def commits_by_year(repositories: list[dict]) -> dict[int, int]:
    """Count sampled commits per calendar year across all repositories."""
    counts: Counter = Counter()
    for repo in repositories:
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
    recent_years = years[n - cutoff :]

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
    new_langs = [l for l in recent_langs if l not in early_langs]
    dropped_langs = [l for l in early_langs if l not in recent_langs]
    if new_langs or dropped_langs:
        evolution.append({
            "new_languages": new_langs,
            "dropped_languages": dropped_langs,
        })
    return evolution


def build_historical_analysis(repositories: list[dict]) -> dict:
    """Produce the full historical analysis dict for one user's repositories."""
    cby = commits_by_year(repositories)
    lby = languages_by_year(repositories)
    trend = compute_trend(cby)
    peak = max(cby, key=cby.get) if cby else None
    return {
        "commits_by_year": cby,
        "languages_by_year": lby,
        "activity_trend": trend,
        "peak_activity_year": peak,
        "tech_evolution": tech_evolution(lby),
    }
