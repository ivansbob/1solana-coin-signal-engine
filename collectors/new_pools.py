import asyncio
import httpx
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Constants
DEXSCREENER_BASE = "https://api.dexscreener.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
MIN_LIQUIDITY_USD = 1000
MAX_AGE_MINUTES = 180

async def fetch_dexscreener_pairs(limit: int = 200) -> List[Dict[str, Any]]:
    """Fetch latest pairs from DexScreener"""
    url = f"{DEXSCREENER_BASE}/latest/dex/search/?q=solana"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            pairs = data.get("pairs", [])
            return pairs[:limit]
    except Exception as e:
        logger.error(f"DexScreener fetch failed: {e}")
        return []

# CoinGecko requires API key for onchain, skipping

def normalize_pool_data(source: str, pool: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize pool data to common format"""
    now = datetime.now(timezone.utc)

    if source == "dexscreener":
        base_token = pool.get("baseToken", {})
        quote_token = pool.get("quoteToken", {})
        token_address = base_token.get("address", "")
        symbol = base_token.get("symbol", "UNKNOWN")
        name = base_token.get("name", "")
        pair_address = pool.get("pairAddress", "")
        dex = pool.get("dexId", "unknown")
        liquidity_usd = pool.get("liquidity", {}).get("usd", 0.0)
        volume_h1 = pool.get("volume", {}).get("h1", 0.0)
        volume_h6 = pool.get("volume", {}).get("h6", 0.0)
        price_usd = pool.get("priceUsd", "0")
        price_change_h1 = pool.get("priceChange", {}).get("h1", 0.0)
        # Estimate age from pairCreatedAt if available
        created_at = pool.get("pairCreatedAt")
        age_minutes = 0
        if created_at:
            try:
                created_dt = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
                age_minutes = int((now - created_dt).total_seconds() / 60)
            except:
                pass



    else:
        return {}

    return {
        "token_address": token_address,
        "symbol": symbol,
        "name": name,
        "pair_address": pair_address,
        "dex": dex,
        "liquidity_usd": float(liquidity_usd),
        "volume_h1": float(volume_h1),
        "volume_h6": float(volume_h6),
        "price_usd": float(price_usd) if isinstance(price_usd, (int, float, str)) and str(price_usd).replace('.', '').isdigit() else 0.0,
        "price_change_h1": float(price_change_h1),
        "age_minutes": int(age_minutes),
        "source": source,
        "timestamp": now.isoformat()
    }

async def get_new_pools() -> List[Dict[str, Any]]:
    """Get new tokens from multiple sources"""
    logger.info("Fetching new pools from DexScreener and CoinGecko...")

    # Fetch from DexScreener
    dexscreener_pools = await fetch_dexscreener_pairs(limit=200)

    # Normalize
    all_pools = []
    for pool in dexscreener_pools:
        normalized = normalize_pool_data("dexscreener", pool)
        if normalized:
            all_pools.append(normalized)

    # Deduplicate by token_address
    seen = set()
    unique_pools = []
    for pool in all_pools:
        token = pool["token_address"]
        if token and token not in seen:
            seen.add(token)
            unique_pools.append(pool)

    # Apply filters
    filtered_pools = [
        pool for pool in unique_pools
        if pool["liquidity_usd"] > MIN_LIQUIDITY_USD and pool["age_minutes"] < MAX_AGE_MINUTES and pool["symbol"] != "UNKNOWN"
    ]

    logger.info(f"Found {len(filtered_pools)} filtered new pools")
    return filtered_pools

def save_to_file(pools: List[Dict[str, Any]], filename: str = "data/new_pools_raw.json"):
    """Save pools to JSON file"""
    os.makedirs("data", exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(pools, f, indent=2)
    logger.info(f"Saved {len(pools)} pools to {filename}")

if __name__ == "__main__":
    # Test run
    pools = asyncio.run(get_new_pools())
    print(f"Retrieved {len(pools)} pools")
    for pool in pools[:5]:  # Print first 5
        print(pool)
    save_to_file(pools)