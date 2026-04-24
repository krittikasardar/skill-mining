"""
main.py:

CLI entry point for the Skill Mining GitHub data collector.

Usage examples:

# Single username
python main.py collect --username torvalds

# Multiple usernames from a file
python main.py collect --file usernames.txt

# With caching enabled (avoids repeated API calls)
ENABLE_CACHE=true python main.py collect --username antirez

# Inspect remaining API rate limit
python main.py rate-limit

# Generate markdown summaries after collection
python main.py collect --username gvanrossum --markdown
"""

import csv
import json
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

import config
from github_client import build_client, log_rate_limit
from collectors.profile_collector import collect_profile
from collectors.repo_collector import collect_all_repos
from transformers_local.schema_builder import build_schema
from utils.helpers import get_logger, utcnow_iso

app = typer.Typer(help="Skill Mining – GitHub profile data collector")
console = Console()
logger = get_logger("main")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_usernames_from_file(path: str) -> list[str]:
    """
    Load usernames from a .txt, .csv, or .json file.
    - .txt : one username per line
    - .csv : first column is the username
    - .json: list of strings, or list of dicts with a 'login' key
    """
    p = Path(path)
    if not p.exists():
        typer.echo(f"ERROR: File not found: {path}", err=True)
        raise typer.Exit(1)

    suffix = p.suffix.lower()
    if suffix == ".txt":
        return [ln.strip() for ln in p.read_text().splitlines() if ln.strip()]
    elif suffix == ".csv":
        with p.open() as f:
            reader = csv.reader(f)
            return [row[0].strip() for row in reader if row and row[0].strip()]
    elif suffix == ".json":
        data = json.loads(p.read_text())
        if isinstance(data, list):
            return [
                (item["login"] if isinstance(item, dict) else str(item)).strip()
                for item in data
            ]
        raise ValueError("JSON file must contain a list")
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def _save_raw(username: str, doc: dict) -> Path:
    out = config.RAW_DIR / f"{username}.json"
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def _save_summary(username: str, doc: dict) -> Path:
    """Save a lightweight summary JSON (no raw text, no commit samples)."""
    agg = doc.get("aggregate_signals", {})
    summary = {
        "username": username,
        "profile": {k: v for k, v in doc["profile"].items()
                    if k in ("login", "name", "bio", "location", "company",
                             "followers", "public_repos", "created_at")},
        "aggregate_signals": agg,
        "top_repos": [
            {
                "full_name": r["repository_metadata"]["full_name"],
                "language": r["repository_metadata"]["language"],
                "stars": r["repository_metadata"]["stargazers_count"],
                "forks": r["repository_metadata"]["forks_count"],
                "relevance_score": r["relevance_score"],
                "topics": r["repository_metadata"]["topics"],
            }
            for r in doc["repositories"][:10]
        ],
        "collection_metadata": doc.get("collection_metadata", {}),
    }
    out = config.PROCESSED_DIR / f"{username}_summary.json"
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def _save_markdown_summary(username: str, doc: dict) -> Path:
    """Generate a human-readable Markdown report for manual inspection."""
    profile = doc.get("profile", {})
    agg = doc.get("aggregate_signals", {})
    top_repos = doc.get("repositories", [])[:5]

    lines = [
        f"# GitHub Profile: {profile.get('name') or username}",
        f"**Login:** {profile.get('login')}  ",
        f"**Bio:** {profile.get('bio') or 'N/A'}  ",
        f"**Location:** {profile.get('location') or 'N/A'}  ",
        f"**Followers:** {profile.get('followers')}  ",
        f"**Public Repos:** {profile.get('public_repos')}  ",
        "",
        "## Aggregate Signals",
        f"- **Active years:** {agg.get('first_active_year')} – {agg.get('last_active_year')}",
        f"- **Total stars received:** {agg.get('total_stars_received')}",
        f"- **Owned repos:** {agg.get('owned_repo_count')}",
        f"- **Top languages:** {', '.join(l['language'] for l in agg.get('top_languages', [])[:5])}",
        f"- **Top topics:** {', '.join(agg.get('top_topics', [])[:8])}",
        "",
        "## Top Repositories (by relevance score)",
    ]

    for repo in top_repos:
        meta = repo["repository_metadata"]
        lines += [
            f"### {meta['full_name']}",
            f"- **Score:** {repo['relevance_score']}",
            f"- **Stars:** {meta['stargazers_count']} | **Forks:** {meta['forks_count']}",
            f"- **Language:** {meta['language']}",
            f"- **Topics:** {', '.join(meta.get('topics', []))}",
            f"- **Description:** {meta.get('description') or 'N/A'}",
            "",
        ]

    md = "\n".join(lines)
    out = config.PROCESSED_DIR / f"{username}_summary.md"
    out.write_text(md, encoding="utf-8")
    return out


def _save_master_csv(all_summaries: list[dict]) -> Path:
    """Save a master CSV across all processed users."""
    out = config.PROCESSED_DIR / "master_summary.csv"
    if not all_summaries:
        return out

    fieldnames = [
        "username", "name", "location", "followers", "public_repos",
        "first_active_year", "last_active_year", "activity_span_years",
        "total_stars_received", "owned_repo_count", "forked_repo_count",
        "top_languages", "top_topics", "total_repos_collected",
    ]
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for s in all_summaries:
            prof = s.get("profile", {})
            agg = s.get("aggregate_signals", {})
            writer.writerow({
                "username": s.get("username"),
                "name": prof.get("name"),
                "location": prof.get("location"),
                "followers": prof.get("followers"),
                "public_repos": prof.get("public_repos"),
                "first_active_year": agg.get("first_active_year"),
                "last_active_year": agg.get("last_active_year"),
                "activity_span_years": agg.get("activity_span_years"),
                "total_stars_received": agg.get("total_stars_received"),
                "owned_repo_count": agg.get("owned_repo_count"),
                "forked_repo_count": agg.get("forked_repo_count"),
                "top_languages": "|".join(
                    l["language"] for l in agg.get("top_languages", [])[:5]
                ),
                "top_topics": "|".join(agg.get("top_topics", [])[:5]),
                "total_repos_collected": agg.get("total_repos_collected"),
            })
    return out


def _save_run_metadata(run_meta: dict) -> Path:
    out = config.LOGS_DIR / f"run_{run_meta['run_id']}.json"
    out.write_text(json.dumps(run_meta, indent=2, ensure_ascii=False))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Process a single username
# ─────────────────────────────────────────────────────────────────────────────

def _process_username(client, username: str, markdown: bool) -> dict:
    """Collect, build schema, save files. Returns summary dict."""
    console.print(f"\n[bold cyan]▶ Processing:[/bold cyan] {username}")
    t0 = time.time()

    profile = collect_profile(client, username)
    repositories = collect_all_repos(client, username)
    rl_info = log_rate_limit(client)
    elapsed = round(time.time() - t0, 1)

    doc = build_schema(username, profile, repositories, rl_info, elapsed)

    raw_path = _save_raw(username, doc)
    summary_path = _save_summary(username, doc)

    console.print(f"  [green]✓[/green] Raw JSON  → {raw_path}")
    console.print(f"  [green]✓[/green] Summary   → {summary_path}")

    if markdown:
        md_path = _save_markdown_summary(username, doc)
        console.print(f"  [green]✓[/green] Markdown  → {md_path}")

    agg = doc.get("aggregate_signals", {})
    console.print(
        f"  Repos: {agg.get('total_repos_collected')} | "
        f"Stars: {agg.get('total_stars_received')} | "
        f"Evidence items: {doc['collection_metadata']['total_evidence_items']} | "
        f"Elapsed: {elapsed}s"
    )
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# CLI commands
# ─────────────────────────────────────────────────────────────────────────────

@app.command()
def collect(
    username: Optional[str] = typer.Option(None, "--username", "-u", help="Single GitHub username"),
    file: Optional[str] = typer.Option(None, "--file", "-f", help="Path to .txt/.csv/.json username list"),
    markdown: bool = typer.Option(False, "--markdown", "-m", help="Also generate Markdown summaries"),
):
    """Collect GitHub profile data and save structured JSON evidence."""
    if not username and not file:
        typer.echo("ERROR: Provide --username or --file.", err=True)
        raise typer.Exit(1)

    usernames: list[str] = []
    if username:
        usernames.append(username)
    if file:
        usernames.extend(_load_usernames_from_file(file))
    usernames = list(dict.fromkeys(usernames))  # deduplicate, preserve order

    run_id = utcnow_iso().replace(":", "-").replace("+", "Z")
    run_meta = {
        "run_id": run_id,
        "started_at": utcnow_iso(),
        "usernames": usernames,
        "results": [],
    }

    client = build_client()
    all_summaries = []

    for uname in usernames:
        try:
            doc = _process_username(client, uname, markdown)
            all_summaries.append({"username": uname, **doc})
            run_meta["results"].append({"username": uname, "status": "success"})
        except Exception as exc:
            logger.error("Failed to process %s: %s", uname, exc)
            console.print(f"  [red]✗[/red] {uname}: {exc}")
            run_meta["results"].append({"username": uname, "status": "error", "error": str(exc)})

    # Master CSV
    csv_path = _save_master_csv(all_summaries)
    console.print(f"\n[bold green]Master CSV → {csv_path}[/bold green]")

    # Run metadata log
    run_meta["finished_at"] = utcnow_iso()
    log_path = _save_run_metadata(run_meta)
    console.print(f"[bold green]Run log    → {log_path}[/bold green]")


@app.command()
def preprocess(
    username: Optional[str] = typer.Option(None, "--username", "-u", help="Preprocess a single user's raw JSON"),
    all_users: bool = typer.Option(False, "--all", "-a", help="Preprocess all raw JSON files in data/raw/"),
    chunk_size: int = typer.Option(1500, "--chunk-size", help="Max characters per chunk (default 1500)"),
):
    """Clean, chunk, and run historical analysis on collected raw JSON data."""
    from preprocessor.pipeline import preprocess as _preprocess, preprocess_all, PREPROCESSED_DIR

    if not username and not all_users:
        typer.echo("ERROR: Provide --username or --all.", err=True)
        raise typer.Exit(1)

    if all_users:
        console.print(f"[bold cyan]Preprocessing all users in {config.RAW_DIR}[/bold cyan]")
        results = preprocess_all(chunk_max_chars=chunk_size)
        console.print(f"\n[bold green]OK Preprocessed {len(results)} users -> {PREPROCESSED_DIR}[/bold green]")
    else:
        raw_path = config.RAW_DIR / f"{username}.json"
        if not raw_path.exists():
            typer.echo(f"ERROR: No raw JSON for '{username}' at {raw_path}", err=True)
            raise typer.Exit(1)
        doc = _preprocess(raw_path, chunk_max_chars=chunk_size)
        stats = doc["stats"]
        hist = doc["historical_analysis"]
        console.print(f"\n[bold green]OK Preprocessed: {username}[/bold green]")
        console.print(
            f"  Evidence items : {stats['original_evidence_count']} -> "
            f"{stats['chunks_produced']} chunks ({stats['items_dropped']} dropped)"
        )
        console.print(f"  Avg chunk size : {stats['avg_chunk_length_chars']} chars")
        console.print(f"  Activity trend : {hist['activity_trend']}")
        if hist.get("peak_activity_year"):
            console.print(f"  Peak year      : {hist['peak_activity_year']}")
        for ev in hist.get("tech_evolution", []):
            if "period" in ev:
                console.print(
                    f"  Tech ({ev['period']}): {', '.join(ev['dominant_languages'])}"
                )


@app.command(name="rate-limit")
def rate_limit_cmd():
    """Inspect the current GitHub API rate limit."""
    client = build_client()
    rl = log_rate_limit(client)
    table = Table(title="GitHub API Rate Limit")
    table.add_column("Bucket")
    table.add_column("Remaining")
    table.add_column("Limit")
    table.add_column("Resets at")
    for bucket, info in rl.items():
        if isinstance(info, dict):
            table.add_row(bucket, str(info["remaining"]), str(info["limit"]), info["reset"])
    console.print(table)


if __name__ == "__main__":
    app()
