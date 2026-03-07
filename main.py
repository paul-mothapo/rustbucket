#!/usr/bin/env python3
r"""
 ____  _   _ ____ _____   ____  _   _  ____ _  _______ _____
|  _ \| | | / ___|_   _| | __ )| | | |/ ___| |/ / ____|_   _|
| |_) | | | \___ \ | |   |  _ \| | | | |   | ' /|  _|   | |
|  _ <| |_| |___) || |   | |_) | |_| | |___| . \| |___  | |
|_| \_\\___/|____/ |_|   |____/ \___/ \____|_|\_\_____| |_|

Automated GitHub Rust project collector.
Fetches Rust repos from GitHub, stores richer metadata, and updates README.md.
"""

import argparse
import datetime
import json
import os
import statistics
import tempfile
import time
from collections import Counter
from dataclasses import dataclass

import requests

PER_PAGE = 100
DAILY_GOAL = 500
DEFAULT_MAX_INACTIVE_MONTHS = 18
ACTIVE_WINDOW_DAYS = 180
TOP_README_LIMIT = 500
SECTION_LIMIT = 15
CATEGORY_LIMIT = 5
STATE_FILE = "rust_bucket_state.json"
README_FILE = "README.md"
OUTPUT_JSON = "rust_projects.json"
UTC = datetime.UTC

CATEGORY_KEYWORDS = {
    "Web": {"web", "http", "server", "axum", "actix", "warp", "rocket", "hyper"},
    "CLI and TUI": {"cli", "command-line", "terminal", "tui", "console"},
    "Async and Networking": {"async", "networking", "tokio", "runtime", "grpc", "tcp", "udp"},
    "Database and Data": {"database", "sql", "postgres", "sqlite", "orm", "redis", "data"},
    "Game Dev": {"gamedev", "game-engine", "game-development", "graphics", "bevy", "wgpu"},
    "Embedded": {"embedded", "firmware", "no-std", "microcontroller"},
    "Developer Tools": {"developer-tools", "devtools", "build-tool", "linter", "testing", "compiler", "formatter"},
    "Security and Crypto": {"security", "cryptography", "crypto", "tls", "encryption"},
    "AI and ML": {"machine-learning", "ml", "ai", "llm", "neural-network", "data-science"},
    "Wasm": {"wasm", "webassembly"},
}

HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


@dataclass(slots=True)
class RunConfig:
    mode: str = "once"
    goal: int = DAILY_GOAL
    min_stars: int = 0
    include_forks: bool = False
    include_archived: bool = False
    query: str = ""
    max_inactive_months: int = DEFAULT_MAX_INACTIVE_MONTHS
    reset_state: bool = False
    readme_only: bool = False
    json_only: bool = False


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(UTC)


def default_state() -> dict:
    return {
        "page": 1,
        "collected": 0,
        "seen_ids": [],
        "last_run": {
            "started_at": None,
            "completed_at": None,
            "new_repo_count": 0,
            "new_repo_ids": [],
            "filters": {},
        },
    }


def normalize_state(state: dict | None) -> dict:
    data = default_state()
    if not state:
        return data
    data["page"] = int(state.get("page", 1) or 1)
    data["collected"] = int(state.get("collected", 0) or 0)
    data["seen_ids"] = list(state.get("seen_ids", []))
    last_run = state.get("last_run", {}) or {}
    data["last_run"] = {
        "started_at": last_run.get("started_at"),
        "completed_at": last_run.get("completed_at"),
        "new_repo_count": int(last_run.get("new_repo_count", 0) or 0),
        "new_repo_ids": list(last_run.get("new_repo_ids", [])),
        "filters": dict(last_run.get("filters", {}) or {}),
    }
    return data


def parse_timestamp(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def isoformat_utc(value: datetime.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def cutoff_date(months: int) -> datetime.date | None:
    if months <= 0:
        return None
    return (utc_now() - datetime.timedelta(days=months * 30)).date()


def atomic_write_json(path: str, data):
    directory = os.path.dirname(path) or "."
    prefix = f".{os.path.basename(path)}."
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False, prefix=prefix, suffix=".tmp") as temp:
            json.dump(data, temp, indent=2, ensure_ascii=False)
            temp_path = temp.name
        os.replace(temp_path, path)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def atomic_write_text(path: str, content: str):
    directory = os.path.dirname(path) or "."
    prefix = f".{os.path.basename(path)}."
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=directory, delete=False, prefix=prefix, suffix=".tmp") as temp:
            temp.write(content)
            temp_path = temp.name
        os.replace(temp_path, path)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            return normalize_state(json.load(f))
    return default_state()


def save_state(state: dict):
    atomic_write_json(STATE_FILE, normalize_state(state))


def load_repos() -> list[dict]:
    if not os.path.exists(OUTPUT_JSON):
        return []
    with open(OUTPUT_JSON, encoding="utf-8") as f:
        repos = json.load(f)
    return [normalize_repo(repo) for repo in repos]


def build_filters(config: RunConfig) -> dict:
    return {
        "query": config.query.strip(),
        "min_stars": config.min_stars,
        "include_forks": config.include_forks,
        "include_archived": config.include_archived,
        "max_inactive_months": config.max_inactive_months,
        "cutoff_date": cutoff_date(config.max_inactive_months).isoformat() if cutoff_date(config.max_inactive_months) else None,
    }


def build_search_query(config: RunConfig) -> str:
    parts = ["language:rust"]
    if config.query.strip():
        parts.append(config.query.strip())
    if config.min_stars > 0:
        parts.append(f"stars:>={config.min_stars}")
    if not config.include_forks:
        parts.append("fork:false")
    if not config.include_archived:
        parts.append("archived:false")
    cutoff = cutoff_date(config.max_inactive_months)
    if cutoff:
        parts.append(f"pushed:>={cutoff.isoformat()}")
    return " ".join(parts)


def comparable_filters(filters: dict) -> dict:
    return {key: value for key, value in filters.items() if key != "cutoff_date"}


def search_rust_repos(page: int, config: RunConfig) -> list[dict]:
    """Fetch one page (up to 100) of Rust repos sorted by stars."""
    url = "https://api.github.com/search/repositories"
    params = {
        "q": build_search_query(config),
        "sort": "stars",
        "order": "desc",
        "per_page": PER_PAGE,
        "page": page,
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)

    if resp.status_code == 403:
        reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(reset - int(time.time()), 5)
        print(f"  Rate limited. Sleeping {wait}s ...")
        time.sleep(wait)
        return search_rust_repos(page, config)

    resp.raise_for_status()
    return resp.json().get("items", [])


def normalize_topics(topics) -> list[str]:
    if not topics:
        return []
    cleaned = {str(topic).strip() for topic in topics if str(topic).strip()}
    return sorted(cleaned)


def license_name(item: dict) -> str | None:
    value = item.get("license")
    if isinstance(value, dict):
        return value.get("spdx_id") or value.get("name")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def repo_html_url(item: dict) -> str:
    html_url = str(item.get("html_url") or "").strip()
    if html_url:
        return html_url

    url = str(item.get("url") or "").strip()
    api_prefix = "https://api.github.com/repos/"
    web_prefix = "https://github.com/repos/"
    if url.startswith(api_prefix):
        return f"https://github.com/{url.removeprefix(api_prefix)}"
    if url.startswith(web_prefix):
        return f"https://github.com/{url.removeprefix(web_prefix)}"
    return url


def normalize_repo(item: dict) -> dict:
    fetched_at = item.get("fetched_at") or isoformat_utc(utc_now())
    updated_at = item.get("updated_at")
    pushed_at = item.get("pushed_at") or updated_at or fetched_at
    return {
        "id": item.get("id"),
        "name": item.get("name") or item.get("full_name") or "unknown",
        "description": (item.get("description") or "").strip(),
        "stars": int(item.get("stars", item.get("stargazers_count", 0)) or 0),
        "forks": int(item.get("forks", item.get("forks_count", 0)) or 0),
        "watchers": int(item.get("watchers", item.get("watchers_count", 0)) or 0),
        "open_issues": int(item.get("open_issues", item.get("open_issues_count", 0)) or 0),
        "url": repo_html_url(item),
        "homepage": (item.get("homepage") or "").strip(),
        "license": license_name(item),
        "topics": normalize_topics(item.get("topics")),
        "created_at": item.get("created_at"),
        "updated_at": updated_at,
        "pushed_at": pushed_at,
        "archived": bool(item.get("archived", False)),
        "fork": bool(item.get("fork", False)),
        "fetched_at": fetched_at,
    }


def parse_repo(item: dict) -> dict:
    repo = normalize_repo(item)
    repo["fetched_at"] = isoformat_utc(utc_now())
    return repo


def repo_matches_filters(repo: dict, config: RunConfig) -> bool:
    if repo["stars"] < config.min_stars:
        return False
    if not config.include_forks and repo.get("fork"):
        return False
    if not config.include_archived and repo.get("archived"):
        return False
    cutoff = cutoff_date(config.max_inactive_months)
    pushed_at = parse_timestamp(repo.get("pushed_at"))
    if cutoff and pushed_at and pushed_at.date() < cutoff:
        return False
    return True


def escape_markdown(text: str) -> str:
    return " ".join(text.replace("|", "\\|").split())


def truncate(text: str, limit: int = 90) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def format_date(value: str | None) -> str:
    parsed = parse_timestamp(value)
    if not parsed:
        return "-"
    return parsed.strftime("%Y-%m-%d")


def format_topics(repo: dict, limit: int = 3) -> str:
    topics = repo.get("topics", [])
    if not topics:
        return "-"
    return ", ".join(topics[:limit])


def render_repo_table(repos: list[dict], columns: tuple[str, ...]) -> list[str]:
    headers = {
        "rank": "#",
        "project": "Project",
        "stars": "Stars",
        "forks": "Forks",
        "updated": "Updated",
        "topics": "Topics",
        "license": "License",
        "description": "Description",
    }
    lines = [
        "| " + " | ".join(headers[column] for column in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]

    for index, repo in enumerate(repos, 1):
        values = {
            "rank": str(index),
            "project": f"[{escape_markdown(repo['name'])}]({repo['url']})",
            "stars": f"{repo['stars']:,}",
            "forks": f"{repo['forks']:,}",
            "updated": format_date(repo.get("pushed_at")),
            "topics": escape_markdown(format_topics(repo)),
            "license": escape_markdown(repo.get("license") or "-"),
            "description": escape_markdown(truncate(repo.get("description") or "-", 90)),
        }
        lines.append("| " + " | ".join(values[column] for column in columns) + " |")
    return lines


def repo_category_key(repo: dict) -> set[str]:
    terms = {topic.lower() for topic in repo.get("topics", [])}
    descriptor = f"{repo.get('name', '')} {repo.get('description', '')}".lower()
    matched = set()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if terms.intersection(keywords) or any(keyword in descriptor for keyword in keywords):
            matched.add(category)
    return matched


def category_sections(repos: list[dict]) -> list[tuple[str, list[dict]]]:
    grouped: dict[str, list[dict]] = {category: [] for category in CATEGORY_KEYWORDS}
    for repo in repos:
        for category in repo_category_key(repo):
            grouped[category].append(repo)

    sections = []
    for category, items in grouped.items():
        if not items:
            continue
        deduped = {repo["id"]: repo for repo in items}.values()
        ranked = sorted(deduped, key=lambda repo: repo["stars"], reverse=True)[:CATEGORY_LIMIT]
        sections.append((category, ranked))
    return sections


def summary_stats(repos: list[dict]) -> dict:
    if not repos:
        return {
            "total": 0,
            "median_stars": 0,
            "active_recently": 0,
            "licensed": 0,
            "top_licenses": [],
            "top_topics": [],
        }

    stars = [repo["stars"] for repo in repos]
    active_cutoff = utc_now() - datetime.timedelta(days=ACTIVE_WINDOW_DAYS)
    active_recently = sum(
        1
        for repo in repos
        if (pushed_at := parse_timestamp(repo.get("pushed_at"))) and pushed_at >= active_cutoff
    )
    license_counts = Counter(repo.get("license") or "Unknown" for repo in repos)
    topic_counts = Counter(topic for repo in repos for topic in repo.get("topics", []))
    return {
        "total": len(repos),
        "median_stars": int(statistics.median(stars)),
        "active_recently": active_recently,
        "licensed": sum(1 for repo in repos if repo.get("license")),
        "top_licenses": license_counts.most_common(5),
        "top_topics": topic_counts.most_common(8),
    }


def generate_readme(repos: list[dict], new_repos: list[dict], state: dict, filters: dict):
    now = utc_now().strftime("%Y-%m-%d %H:%M UTC")
    stats = summary_stats(repos)
    top = repos[:TOP_README_LIMIT]
    recent = sorted(
        repos,
        key=lambda repo: parse_timestamp(repo.get("pushed_at")) or datetime.datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )[:SECTION_LIMIT]
    new_section = sorted(new_repos, key=lambda repo: repo["stars"], reverse=True)[:SECTION_LIMIT]
    categories = category_sections(repos)

    now_badge = now.replace(" ", "%20").replace(":", "%3A").replace("+", "%2B")
    license_summary = ", ".join(f"{name} ({count})" for name, count in stats["top_licenses"]) or "-"
    topic_summary = ", ".join(f"{name} ({count})" for name, count in stats["top_topics"]) or "-"

    lines = [
        "# Rust Bucket",
        "",
        f"![Last Updated](https://img.shields.io/badge/Updated%20on-{now_badge}-brightgreen)",
        f"![Total Projects](https://img.shields.io/badge/Projects%20Indexed-{stats['total']:,}-blue)",
        "",
        f"## Updated on {now}",
        "",
        "> Automated collection of open-source Rust projects from GitHub.",
        "> Built as a filtered discovery index for active Rust repositories.",
        "",
        "## Run Summary",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Total projects indexed | {stats['total']:,} |",
        f"| New repos added this run | {len(new_repos):,} |",
        f"| Median stars | {stats['median_stars']:,} |",
        f"| Active in last {ACTIVE_WINDOW_DAYS} days | {stats['active_recently']:,} |",
        f"| Repos with a detected license | {stats['licensed']:,} |",
        f"| Top licenses | {escape_markdown(license_summary)} |",
        f"| Top topics | {escape_markdown(topic_summary)} |",
        "",
        "## Filters",
        "",
        "| Setting | Value |",
        "| --- | --- |",
        f"| Additional query | `{filters['query'] or '-'}` |",
        f"| Minimum stars | {filters['min_stars']:,} |",
        f"| Forks included | {'yes' if filters['include_forks'] else 'no'} |",
        f"| Archived repos included | {'yes' if filters['include_archived'] else 'no'} |",
        f"| Inactive cutoff | {filters['cutoff_date'] or 'disabled'} |",
        f"| Last saved page | {state.get('page', 1)} |",
    ]

    if new_section:
        lines += [
            "",
            "## New Repos Added This Run",
            "",
            *render_repo_table(new_section, ("project", "stars", "updated", "topics", "description")),
        ]

    if recent:
        lines += [
            "",
            "## Most Recently Updated Repos",
            "",
            *render_repo_table(recent, ("project", "stars", "updated", "license", "description")),
        ]

    if categories:
        lines += [
            "",
            "## Topic and Category Highlights",
        ]
        for category, items in categories:
            lines += [
                "",
                f"### {category}",
                "",
                *render_repo_table(items, ("project", "stars", "topics", "description")),
            ]

    lines += [
        "",
        f"## Top {len(top)} Rust Projects",
        "",
        *render_repo_table(top, ("rank", "project", "stars", "forks", "updated", "description")),
        "",
        "---",
        "",
        "## Usage",
        "",
        "```bash",
        "pip install requests",
        "python main.py --goal 500",
        "python main.py --query \"topic:web\" --min-stars 200",
        "python main.py --readme-only",
        "```",
        "",
        "_Generated by Rust Bucket - run `python main.py --help` for options._",
    ]

    atomic_write_text(README_FILE, "\n".join(lines) + "\n")
    print(f"README.md written with {len(top)} projects.")


def collect(config: RunConfig) -> dict:
    state = default_state() if config.reset_state else load_state()
    current_filters = build_filters(config)
    existing_map = {
        repo["id"]: repo
        for repo in load_repos()
        if repo.get("id") is not None and repo_matches_filters(repo, config)
    }
    last_filters = state.get("last_run", {}).get("filters", {})
    if not config.reset_state and last_filters and comparable_filters(last_filters) != comparable_filters(current_filters):
        print("Filters changed since the last run. Resetting pagination for the new query.")
        state = default_state()
        state["seen_ids"] = sorted(existing_map)
    seen_ids = set(state["seen_ids"])
    new_repos: list[dict] = []
    page = state["page"]
    started_at = isoformat_utc(utc_now())

    print(f"Rust Bucket - collecting up to {config.goal} repos (starting page {page})")
    print(f"Search query: {build_search_query(config)}")

    while len(new_repos) < config.goal:
        print(f"Fetching page {page} ...", end=" ", flush=True)
        items = search_rust_repos(page, config)

        if not items:
            print("no more results.")
            break

        added = 0
        for item in items:
            repo = parse_repo(item)
            if not repo_matches_filters(repo, config):
                continue
            if repo["id"] not in seen_ids:
                new_repos.append(repo)
                added += 1
            existing_map[repo["id"]] = repo
            seen_ids.add(repo["id"])

        print(f"+{added} new  (total new today: {len(new_repos)})")
        page += 1

        time.sleep(2.2)

        if len(new_repos) >= config.goal:
            break

    all_repos = sorted(existing_map.values(), key=lambda repo: repo["stars"], reverse=True)
    completed_at = isoformat_utc(utc_now())
    state = normalize_state(state)
    state["page"] = page
    state["collected"] = int(state.get("collected", 0)) + len(new_repos)
    state["seen_ids"] = sorted(seen_ids)
    state["last_run"] = {
        "started_at": started_at,
        "completed_at": completed_at,
        "new_repo_count": len(new_repos),
        "new_repo_ids": [repo["id"] for repo in new_repos],
        "filters": current_filters,
    }

    save_state(state)
    atomic_write_json(OUTPUT_JSON, all_repos)

    print(f"\nCollected {len(new_repos)} new repos. Total in database: {len(all_repos)}")
    return {
        "all_repos": all_repos,
        "new_repos": new_repos,
        "state": state,
        "filters": current_filters,
    }


def parse_args() -> RunConfig:
    parser = argparse.ArgumentParser(description="Collect Rust repositories from GitHub and update local outputs.")
    parser.add_argument("mode", nargs="?", choices=("once", "loop"), default="once")
    parser.add_argument("--goal", type=int, default=DAILY_GOAL, help=f"Maximum new repos to collect per run. Default: {DAILY_GOAL}.")
    parser.add_argument("--min-stars", type=int, default=0, help="Only include repos at or above this star count.")
    parser.add_argument("--include-forks", action="store_true", help="Include forked repositories.")
    parser.add_argument("--include-archived", action="store_true", help="Include archived repositories.")
    parser.add_argument("--query", default="", help="Extra GitHub search qualifiers to append to the base Rust query.")
    parser.add_argument(
        "--max-inactive-months",
        type=int,
        default=DEFAULT_MAX_INACTIVE_MONTHS,
        help=f"Exclude repos not pushed to within this many months. Use 0 to disable. Default: {DEFAULT_MAX_INACTIVE_MONTHS}.",
    )
    parser.add_argument("--reset-state", action="store_true", help="Reset pagination and seen IDs before collecting.")
    parser.add_argument("--readme-only", action="store_true", help="Regenerate README.md from the existing JSON database without fetching.")
    parser.add_argument("--json-only", action="store_true", help="Fetch and write JSON/state without regenerating README.md.")

    args = parser.parse_args()
    if args.goal < 0:
        parser.error("--goal must be 0 or greater.")
    if args.min_stars < 0:
        parser.error("--min-stars must be 0 or greater.")
    if args.max_inactive_months < 0:
        parser.error("--max-inactive-months must be 0 or greater.")
    if args.readme_only and args.json_only:
        parser.error("--readme-only and --json-only cannot be used together.")

    return RunConfig(
        mode=args.mode,
        goal=args.goal,
        min_stars=args.min_stars,
        include_forks=args.include_forks,
        include_archived=args.include_archived,
        query=args.query,
        max_inactive_months=args.max_inactive_months,
        reset_state=args.reset_state,
        readme_only=args.readme_only,
        json_only=args.json_only,
    )


def run_once(config: RunConfig):
    if config.reset_state:
        save_state(default_state())
        print("State reset to page 1.")

    if config.readme_only:
        state = load_state()
        repos = sorted(load_repos(), key=lambda repo: repo["stars"], reverse=True)
        filters = state.get("last_run", {}).get("filters") or build_filters(config)
        generate_readme(repos, [], state, filters)
        return

    result = collect(config)
    if not config.json_only:
        generate_readme(result["all_repos"], result["new_repos"], result["state"], result["filters"])


def run_loop(config: RunConfig, interval_hours: float = 24):
    """Keep running every `interval_hours` hours (no cron needed)."""
    while True:
        print(f"\n{'=' * 60}")
        print(f"{isoformat_utc(utc_now())} - starting scheduled run")
        print(f"{'=' * 60}")
        run_once(config)
        next_run = utc_now() + datetime.timedelta(hours=interval_hours)
        print(f"\nNext run at {next_run.strftime('%Y-%m-%d %H:%M UTC')} - sleeping ...\n")
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    config = parse_args()

    if config.mode == "loop":
        run_loop(config, interval_hours=24)
    else:
        run_once(config)
