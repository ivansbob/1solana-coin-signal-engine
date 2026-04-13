import asyncio
import httpx
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Constants
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
CHAINS = {
    "solana": "solana",
    "base": "base",
    "arbitrum": "arbitrum",
    "ethereum": "eth"
}
MIN_LIQUIDITY = 5000

async def fetch_coingecko_new_pools(chain: str) -> List[Dict[str, Any]]:
    """Fetch new pools for a specific chain from CoinGecko Onchain API"""
    url = f"{COINGECKO_BASE}/onchain/networks/{chain}/new_pools?include=dex"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
    except Exception as e:
        logger.error(f"CoinGecko {chain} fetch failed: {e}")
        return []

async def get_cross_chain_pools(min_liquidity: float = 5000) -> List[Dict[str, Any]]:
    """
    Get new pools from all chains via CoinGecko Onchain API.
    Free, no key required (may have rate limits).
    """

    # Fetch in parallel
    tasks = [fetch_coingecko_new_pools(chain) for chain in CHAINS.values()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_pools = []
    for chain, pools in zip(CHAINS.keys(), results):
        if isinstance(pools, Exception):
            logger.error(f"Error fetching {chain}: {pools}")
            continue
        for pool in pools:
            pool["chain"] = chain
            all_pools.append(pool)

    # Filter by liquidity
    filtered = [p for p in all_pools if p.get("liquidity", 0) >= min_liquidity]

    logger.info(f"Fetched {len(filtered)} cross-chain pools")
    return filtered

async def get_cross_chain_price(symbol: str, chains: List[str]) -> Dict[str, Any]:
    """
    Find same token on multiple chains and calculate price delta for arb opportunity.
    """
    # This is a simplified version; in reality, need to map token addresses across chains
    # For now, assume same symbol has same price across chains (not accurate)
    result = {"symbol": symbol}

    # Mock prices for demo; in real implementation, fetch from CoinGecko /simple/price or onchain
    # But since onchain may require key, use regular API
    prices = {}
    for chain in chains:
        try:
            url = f"{COINGECKO_BASE}/simple/price?ids={symbol.lower()}&vs_currencies=usd"
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                price = data.get(symbol.lower(), {}).get("usd")
                if price:
                    prices[chain] = price
        except Exception as e:
            logger.error(f"Price fetch for {symbol} on {chain} failed: {e}")

    result.update(prices)

    if len(prices) > 1:
        price_values = list(prices.values())
        delta_pct = (max(price_values) - min(price_values)) / min(price_values) * 100
        result["cross_chain_delta_pct"] = round(delta_pct, 3)
        result["arb_opportunity"] = delta_pct > 0.5
    else:
        result["cross_chain_delta_pct"] = 0.0
        result["arb_opportunity"] = False

    return result

def save_to_file(pools: List[Dict[str, Any]], filename: str = "data/cross_chain_pools.json"):
    """Save pools to JSON file"""
    os.makedirs("data", exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(pools, f, indent=2)
    logger.info(f"Saved {len(pools)} cross-chain pools to {filename}")

if __name__ == "__main__":
    # Test
    asyncio.run(get_new_pools_all_chains())