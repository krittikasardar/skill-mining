"""
main.py (v2)
------------
Entry point for the GitHub skill-mining data collection pipeline.

For each username in usernames.txt (or CLI args), this script:
  1. Collects profile metadata              -> collectors/profile_collector.py
  2. Collects repository evidence           -> collectors/repo_collector.py
  3. Builds the evidence-preserving schema  -> schema_builder.py
  4. Adds historical/month-level analysis   -> preprocessor/historical.py
  5. Writes data/raw/<user>_raw_v2.json
  6. Runs the preprocessing pipeline         -> preprocessor/pipeline.py
  7. Writes data/processed/<user>_processed_v2.json

Usage
-----
    python main.py
    python main.py hnarayanan nayafia
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from github import Github

import config
from github_client import build_client, wait_for_rate_limit
from collectors.profile_collector import collect_profile
from collectors.repo_collector import collect_all_repos
from preprocessor.historical import build_historical_analysis
from preprocessor.pipeline import transform as preprocess_v2
from utils.helpers import get_logger

# The project has used different folder names in earlier versions.
# Keep this import flexible so main.py works whether schema_builder.py lives in
# transformers_local/, transformers/, or the project root.
try:
    from transformers_local.schema_builder import build_schema
except ImportError:  # pragma: no cover - fallback for older repo layouts
    try:
        from transformers.schema_builder import build_schema
    except ImportError:  # pragma: no cover - fallback for flat layout
        from schema_builder import build_schema

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = Path(getattr(config, "RAW_DIR", BASE_DIR / "data" / "raw"))
PROC_DIR = Path(getattr(config, "PROCESSED_DIR", BASE_DIR / "data" / "processed"))
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)


def run_for_user(client: Github, username: str) -> dict[str, Path]:
    """
    Run the full v2 collection + preprocessing pipeline for one GitHub user.

    Returns
    -------
    dict with paths to the written raw and processed JSON files.
    """
    start = time.time()
    logger.info("=" * 70)
    logger.info("Starting v2 collection for: %s", username)

    # 1. Profile-level evidence
    profile = collect_profile(client, username)

    # 2. Repository-level evidence
    # Updated repo_collector.py returns:
    #   repositories, extra_aggregate_signals
    # This fallback keeps main.py compatible with older collectors that returned
    # only repositories.
    wait_for_rate_limit(client, buffer=50)
    repo_result = collect_all_repos(client, username)
    if isinstance(repo_result, tuple) and len(repo_result) == 2:
        repositories, extra_aggregate_signals = repo_result
    else:
        repositories = repo_result
        extra_aggregate_signals = {}

    elapsed = round(time.time() - start, 1)

    # 3. Build canonical schema using the updated schema_builder.py
    raw_doc = build_schema(
        username=username,
        profile=profile,
        repositories=repositories,
        rate_limit_info=_rate_limit_snapshot(client),
        elapsed_seconds=elapsed,
        extra_aggregate_signals=extra_aggregate_signals,
    )

    # 4. Add historical/month-level analysis from updated historical.py
    historical_analysis = build_historical_analysis(repositories)
    raw_doc["historical_analysis"] = historical_analysis

    # Optional: expose the most useful historical fields inside aggregate_signals
    # too, so downstream agents can retrieve them without checking another block.
    aggregate = raw_doc.setdefault("aggregate_signals", {})
    for key in (
        "weekday_vs_weekend_ratio",
        "most_active_month_of_year",
        "repo_creation_cadence",
    ):
        aggregate.setdefault(key, historical_analysis.get(key))

    # 5. Keep metadata explicit for v2 output files
    raw_doc["schema_version"] = "2.0"
    raw_doc.setdefault("collection_metadata", {})
    raw_doc["collection_metadata"].update({
        "username": username,
        "elapsed_seconds": elapsed,
        "total_repos": len(repositories),
        "raw_output_file": f"{username}_raw_v2.json",
        "processed_output_file": f"{username}_processed_v2.json",
        "main_version": "2.0",
        "pipeline_notes": (
            "v2 includes richer commit depth, collaboration signals, tooling "
            "detection, profile README/pinned repositories, and monthly "
            "historical activity analysis."
        ),
    })

    # 6. Write raw_v2 JSON
    raw_path = RAW_DIR / f"{username}_raw_v2.json"
    _write_json(raw_path, raw_doc)
    logger.info(
        "Saved raw_v2 -> %s (%d repos, %d evidence items, %.1fs)",
        raw_path,
        len(repositories),
        len(raw_doc.get("evidence_index", [])),
        elapsed,
    )

    # 7. Run preprocessing pipeline and write processed_v2 JSON
    processed_doc = preprocess_v2(raw_doc)
    processed_path = PROC_DIR / f"{username}_processed_v2.json"
    _write_json(processed_path, processed_doc)
    logger.info("Saved processed_v2 -> %s", processed_path)

    return {"raw": raw_path, "processed": processed_path}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON with UTF-8 encoding and stable formatting."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _rate_limit_snapshot(client: Github) -> dict:
    """Return a compact GitHub API rate-limit snapshot."""
    try:
        rl = client.get_rate_limit()
        checked_at = datetime.now(timezone.utc).isoformat()
        return {
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
            "checked_at": checked_at,
        }
    except Exception as exc:
        logger.warning("Could not read GitHub rate limit snapshot: %s", exc)
        return {}


def _load_usernames() -> list[str]:
    """Load usernames from usernames.txt if no CLI usernames are supplied."""
    path = BASE_DIR / "usernames.txt"
    if path.exists():
        return [u.strip() for u in path.read_text(encoding="utf-8").splitlines() if u.strip()]
    return []


def main() -> None:
    usernames = sys.argv[1:] or _load_usernames()
    if not usernames:
        logger.error("No usernames supplied and usernames.txt is empty or missing.")
        sys.exit(1)

    if not getattr(config, "GITHUB_TOKEN", ""):
        logger.warning(
            "GITHUB_TOKEN is empty. Unauthenticated GitHub API calls may hit rate limits quickly."
        )

    client = build_client(config.GITHUB_TOKEN)

    failed: list[str] = []
    for username in usernames:
        try:
            run_for_user(client, username)
        except Exception as exc:
            failed.append(username)
            logger.error("Pipeline failed for %s: %s", username, exc, exc_info=True)

    if failed:
        logger.error("Completed with failures for: %s", ", ".join(failed))
        sys.exit(1)

    logger.info("All done.")
@app.command()
def analyze(
    username: str = typer.Option(..., "--username", "-u", help="GitHub username to analyze with AI agents"),
):
    """Analyze a GitHub profile using AI agents to extract skills, roles, and summary."""
    from data.agents.pipeline import build_pipeline

    console.print(f"\n[bold cyan]▶ Analyzing:[/bold cyan] {username}")
    console.print("=" * 50)

    pipeline = build_pipeline()

    # Initial state — only username is known at start
    initial_state = {
        "username": username,
        "skills": "",
        "roles": "",
        "summary": ""
    }

    # Run the full pipeline
    result = pipeline.invoke(initial_state)

    console.print("\n" + "=" * 50)
    console.print("DEVELOPER PROFILE")
    console.print("=" * 50)
    console.print("\nSKILLS:")

    # Format skills in readable paragraphs
    skills_data = result["skills"]
    if isinstance(skills_data, dict) and "skills" in skills_data:
        skills_list = skills_data["skills"]
    elif isinstance(skills_data, list):
        skills_list = skills_data
    else:
        console.print(skills_data)
        skills_list = []

    for i, skill in enumerate(skills_list, 1):
        console.print(f"\n{i}. {skill['name']}")
        console.print(f"   Confidence: {skill['confidence']:.1f}")
        console.print(f"   Justification: {skill['justification']}")

        # Handle evidence - could be strings or dicts
        evidence_items = []
        for ev in skill['evidence'][:2]:  # Show first 2 evidence items
            if isinstance(ev, str):
                evidence_items.append(ev)
            elif isinstance(ev, dict):
                # If it's a dict, try to get a text field or convert to string
                evidence_items.append(str(ev.get('text', ev)))
            else:
                evidence_items.append(str(ev))

        console.print(f"   Evidence: {', '.join(evidence_items)}")

    console.print("\nROLE:")
    console.print(result["roles"])
    console.print("\nSUMMARY:")
    console.print(result["summary"])


if __name__ == "__main__":
    main()
