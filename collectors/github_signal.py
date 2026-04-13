import asyncio
import httpx
from datetime import datetime, timezone
from typing import Dict, Any
from utils.cache import SimpleTTLCache

# Cache for GitHub API responses, TTL 24 hours
_github_cache = SimpleTTLCache(ttl=86400)

# Mapping of symbols to GitHub repo IDs
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

async def get_github_dev_score(symbol: str) -> Dict[str, Any]:
    """
    Check if project is in best-of-crypto and get GitHub metrics.
    Uses GitHub public API (no token — 60 req/hour).
    """
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