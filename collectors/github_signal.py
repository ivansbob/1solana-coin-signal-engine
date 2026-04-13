import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from utils.cache import SimpleTTLCache
from utils.rate_limit import acquire
from utils.retry import with_retry
from utils.clock import utc_now_iso

# ================== НАСТРОЙКИ ==================
KEYWORDS = [
    "zk", "zero-knowledge", "account-abstraction", "intents",
    "restaking", "modular-blockchain", "rollup", "cross-chain",
    "mev", "orderbook", "perps", "real-yield", "solana", "raydium"
]

GITHUB_SEARCH_URL = "https://api.github.com/search/repositories"
MAX_REPOS = 20
LOOKBACK_HOURS = 48

# Кэш на 4 часа (GitHub search rate-limit friendly)
_github_cache = SimpleTTLCache(ttl=14400)  # 4 часа


async def search_new_repos() -> List[Dict]:
    """
    Search GitHub for new repositories matching KEYWORDS updated in last LOOKBACK_HOURS.
    """
    date_threshold = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).strftime('%Y-%m-%dT%H:%M:%SZ')
    query = f"{' OR '.join(KEYWORDS)} pushed:>{date_threshold}"

    acquire("github")  # Rate limit

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    GITHUB_SEARCH_URL,
                    params={"q": query, "sort": "updated", "order": "desc", "per_page": 100}
                )
                response.raise_for_status()
                data = response.json()
                repos = data.get("items", [])
                return repos[:MAX_REPOS]
        except Exception as e:
            if attempt == 2:
                print(f"Warning: GitHub search failed after retries: {e}")
                return []
            await asyncio.sleep(0.2 * (2 ** attempt))  # Exponential backoff


def calculate_dev_activity_score(repo: Dict) -> Dict[str, Any]:
    """
    Calculate dev activity score 0.0-1.0 based on repo metrics.
    """
    stars = repo.get("stargazers_count", 0)
    forks = repo.get("forks_count", 0)
    open_issues = repo.get("open_issues_count", 0)
    pushed_at = repo.get("pushed_at")

    # Contributors not fetched in search API, set to 0 for now
    contributors = 0

    # Recency score
    recency_score = 0.0
    if pushed_at:
        try:
            pushed_dt = datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            hours_ago = (now - pushed_dt).total_seconds() / 3600
            if hours_ago < 24:
                recency_score = 1.0
            elif hours_ago < 72:
                recency_score = 0.6
        except ValueError:
            pass

    score = (
        0.35 * min(1.0, stars / 5000) +
        0.25 * recency_score +
        0.20 * min(1.0, forks / 200) +
        0.10 * min(1.0, contributors / 10) +
        0.10 * (1.0 if open_issues < 50 else 0.0)
    )

    reason_codes = []
    if stars >= 500:
        reason_codes.append("high_stars")
    if recency_score == 1.0:
        reason_codes.append("recent_push")
    if forks >= 50:
        reason_codes.append("high_forks")
    if open_issues < 50:
        reason_codes.append("low_issues")

    return {
        "dev_activity_score": round(score, 2),
        "reason_codes": reason_codes
    }


async def get_github_candidates() -> List[Dict]:
    """
    Main entry point: get fresh GitHub repos with dev activity scores.
    """
    cache_key = "github_candidates"
    cached = _github_cache.get(cache_key)
    if cached is not None:
        return cached

    repos = await search_new_repos()
    candidates = []
    for repo in repos:
        score_data = calculate_dev_activity_score(repo)
        candidate = {
            **repo,
            **score_data
        }
        candidates.append(candidate)

    # Sort by pushed_at descending (most recent first)
    candidates.sort(key=lambda x: x.get("pushed_at", ""), reverse=True)

    _github_cache.set(cache_key, candidates)
    return candidates


def build_github_text_section(candidates: List[Dict]) -> str:
    """
    Format candidates for daily_aggregate.txt
    """
    if not candidates:
        return "=== GITHUB REPOSITORIES (dev activity last 48h) ===\nNo recent repositories found.\n"

    lines = ["=== GITHUB REPOSITORIES (dev activity last 48h) ==="]
    for repo in candidates[:MAX_REPOS]:
        full_name = repo.get("full_name", "unknown")
        description = repo.get("description", "")[:100] + "..." if len(repo.get("description", "")) > 100 else repo.get("description", "")
        stars = repo.get("stargazers_count", 0)
        pushed_at = repo.get("pushed_at")
        forks = repo.get("forks_count", 0)
        score = repo.get("dev_activity_score", 0.0)

        # Calculate time ago
        time_ago = "unknown"
        if pushed_at:
            try:
                pushed_dt = datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                hours_ago = int((now - pushed_dt).total_seconds() / 3600)
                if hours_ago < 24:
                    time_ago = f"{hours_ago}h ago"
                else:
                    days_ago = hours_ago // 24
                    time_ago = f"{days_ago}d ago"
            except ValueError:
                pass

        lines.append(f"Repo: {full_name}")
        lines.append(f"Description: {description}")
        lines.append(f"Stars: {stars} | Pushed: {time_ago} | Forks: {forks}")
        lines.append(f"Dev Activity Score: {score}")
        lines.append("")  # Empty line between repos

    return "\n".join(lines)


# Legacy function for backward compatibility
async def get_github_dev_score(symbol: str) -> Dict[str, Any]:
    """
    Legacy: Check if project is in best-of-crypto and get GitHub metrics.
    """
    # Keep old SYMBOL_TO_GITHUB for now, but this is deprecated
    SYMBOL_TO_GITHUB = {
        "SOL": "solana-labs/solana",
        "USDC": "centre",
        "USDT": "tether",
        "RAY": "raydium-io/raydium-clmm",
        "ORCA": "orca-so/orca",
        "JUP": "jup-ag/terminal",
        "BONK": "bonk",
        "WIF": "wif",
    }

    symbol_upper = symbol.upper()
    in_best_of_crypto = symbol_upper in SYMBOL_TO_GITHUB

    if not in_best_of_crypto:
        return {"in_best_of_crypto": False, "dev_score": 0.0}

    github_id = SYMBOL_TO_GITHUB[symbol_upper]
    cache_key = f"github:{github_id}"

    # Check cache
    cached = _github_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        acquire("github")
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"https://api.github.com/repos/{github_id}")
            response.raise_for_status()
            data = response.json()

        stars = data.get("stargazers_count", 0)
        forks = data.get("forks_count", 0)
        pushed_at = data.get("pushed_at")

        last_commit_days_ago = None
        if pushed_at:
            try:
                pushed_dt = datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                last_commit_days_ago = (now - pushed_dt).days
            except ValueError:
                pass

        # Calculate dev_score
        dev_score = 0.0
        if stars > 1000:
            dev_score += 0.4
        elif stars > 100:
            dev_score += 0.2

        if last_commit_days_ago is not None:
            if last_commit_days_ago < 30:
                dev_score += 0.3
            elif last_commit_days_ago < 90:
                dev_score += 0.1

        if forks > 100:
            dev_score += 0.2

        dev_score = min(dev_score, 1.0)

        result = {
            "in_best_of_crypto": True,
            "github_stars": stars,
            "github_forks": forks,
            "last_commit_days_ago": last_commit_days_ago,
            "dev_score": round(dev_score, 2)
        }

        # Cache result
        _github_cache.set(cache_key, result)
        return result

    except Exception:
        # Failopen
        return {"in_best_of_crypto": False, "dev_score": 0.0}