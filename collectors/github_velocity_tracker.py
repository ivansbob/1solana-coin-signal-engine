import asyncio
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List

import aiohttp

from utils.io import read_jsonl, append_jsonl, write_json, ensure_dir
from utils.rate_limit import acquire
from utils.retry import with_retry
from config.settings import Settings

GITHUB_SEARCH_URL = "https://api.github.com/search/repositories"
SNAPSHOTS_DIR = Path("data/github_snapshots")
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

VELOCITY_WEIGHTS = {"stars": 0.50, "forks": 0.30, "commits": 0.20}
AGE_BONUS_DAYS = 30
HIGH_VELOCITY_THRESHOLD = 15.0  # stars + forks per day


async def fetch_repos(keywords: List[str], max_repos: int = 30) -> List[Dict]:
    query = (
        " ".join(keywords)
        + " language:TypeScript OR language:Rust OR language:Python created:>=2025-01-01"
    )
    params = {"q": query, "sort": "updated", "order": "desc", "per_page": max_repos}

    await acquire("github")
    async with aiohttp.ClientSession() as session:
        async with session.get(GITHUB_SEARCH_URL, params=params) as resp:
            if resp.status != 200:
                print(f"GitHub API error: {resp.status}")
                return []
            data = await resp.json()
            return data.get("items", [])


def calculate_velocity_metrics(current: Dict, previous: Dict | None) -> Dict[str, Any]:
    """Вычисляет velocity и acceleration."""
    now = datetime.now(timezone.utc)
    created_at = datetime.fromisoformat(
        current.get("created_at", "").replace("Z", "+00:00")
    )
    age_days = max(0.1, (now - created_at).total_seconds() / 86400)

    stars = current.get("stargazers_count", 0)
    forks = current.get("forks_count", 0)

    stars_vel = stars / age_days
    forks_vel = forks / age_days

    # Delta (если есть предыдущий snapshot)
    stars_delta = 0
    forks_delta = 0
    if previous:
        stars_delta = max(0, stars - previous.get("stargazers_count", 0))
        forks_delta = max(0, forks - previous.get("forks_count", 0))

    commit_vel = 0  # можно расширить через /commits эндпоинт позже

    combined_velocity = (
        VELOCITY_WEIGHTS["stars"] * stars_vel
        + VELOCITY_WEIGHTS["forks"] * forks_vel
        + VELOCITY_WEIGHTS["commits"] * commit_vel
    )

    acceleration = 0
    if previous:
        prev_vel = previous.get("combined_velocity", 0)
        acceleration = combined_velocity - prev_vel if prev_vel > 0 else 0

    age_bonus = (
        25
        if age_days <= AGE_BONUS_DAYS and combined_velocity > HIGH_VELOCITY_THRESHOLD
        else 0
    )

    return {
        "stars_velocity_24h_proxy": round(stars_vel, 2),
        "forks_velocity_24h_proxy": round(forks_vel, 2),
        "combined_velocity_score": round(combined_velocity + age_bonus, 2),
        "acceleration": round(acceleration, 2),
        "age_days": round(age_days, 1),
        "age_bonus_applied": age_bonus > 0,
    }


def save_snapshot(repos: List[Dict]):
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")[:19]
    snapshot_file = SNAPSHOTS_DIR / f"snapshot_{timestamp.replace(':', '-')}.jsonl"
    for repo in repos:
        record = {
            "full_name": repo.get("full_name"),
            "stargazers_count": repo.get("stargazers_count"),
            "forks_count": repo.get("forks_count"),
            "pushed_at": repo.get("pushed_at"),
            "created_at": repo.get("created_at"),
            "description": repo.get("description"),
            "topics": repo.get("topics", []),
            "ts": timestamp,
        }
        append_jsonl(snapshot_file, record)


async def run_github_velocity_tracker(max_repos: int = 25):
    from collectors.github_signal import KEYWORDS  # используем из PR-1/2

    current_repos = await fetch_repos(KEYWORDS, max_repos)
    save_snapshot(current_repos)

    # Загружаем предыдущий snapshot (самый свежий)
    previous = None
    snapshot_files = sorted(SNAPSHOTS_DIR.glob("snapshot_*.jsonl"), reverse=True)
    if len(snapshot_files) > 1:
        previous_rows = read_jsonl(snapshot_files[1])
        previous_dict = {r["full_name"]: r for r in previous_rows}

    enriched = []
    for repo in current_repos:
        full_name = repo.get("full_name")
        prev = previous_dict.get(full_name) if "previous_dict" in locals() else None
        velocity = calculate_velocity_metrics(repo, prev)

        enriched.append(
            {
                **repo,
                **velocity,
                "risk_adjusted_velocity": round(
                    velocity["combined_velocity_score"]
                    * (
                        0.8
                        if "meme" in (repo.get("description") or "").lower()
                        else 1.0
                    ),
                    2,
                ),
                "provenance": "github_multi_velocity_tracker_2026",
            }
        )

    # Сохраняем результат для агрегатора
    write_json(
        Path("data/processed/github_velocity.json"),
        {
            "as_of": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "repos": enriched[:15],  # топ по velocity
            "high_acceleration_count": sum(
                1 for r in enriched if r.get("acceleration", 0) > 5
            ),
        },
    )

    return enriched
