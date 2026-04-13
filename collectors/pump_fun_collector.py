import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import aiohttp
from dotenv import load_dotenv

load_dotenv()

# Constants
DEXSCREENER_API_URL = "https://api.dexscreener.com/latest/dex/search?q=solana"
MIN_FDV = 10000  # $10k minimum fully diluted valuation

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
seen_tokens = set()


async def fetch_new_tokens(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fetch new tokens from DexScreener API"""
    try:
        async with session.get(DEXSCREENER_API_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.error(f"DexScreener API returned status {resp.status}")
                return []

            data = await resp.json()

            logger.info(f"DexScreener API returned: {type(data)}")

            # Handle different response structures
            pairs = []
            if isinstance(data, dict):
                pairs = data.get("pairs", [])
            elif isinstance(data, list):
                pairs = data
            else:
                logger.error("Unexpected API response format")
                return []

            logger.info(f"Processing {len(pairs)} pairs")

            new_tokens = []
            for pair in pairs:
                # Extract token info from pair data
                base_token = pair.get("baseToken", {})
                token_address = base_token.get("address")
                symbol = base_token.get("symbol", "")

                # Skip if not a valid token or if it's SOL
                if not token_address or symbol == "SOL":
                    continue

                # Get pair creation time and other metrics
                pair_created_at = pair.get("pairCreatedAt")
                fdv = pair.get("fdv", 0)
                market_cap = pair.get("marketCap", 0)

                # Skip if already seen
                if token_address in seen_tokens:
                    continue

                # Apply filters - use market cap if available, otherwise fdv
                cap_to_check = market_cap if market_cap > 0 else fdv
                if cap_to_check < MIN_FDV:
                    continue

                # Check if pair created within last 24 hours
                try:
                    if isinstance(pair_created_at, str):
                        created_time = datetime.fromisoformat(pair_created_at.replace('Z', '+00:00'))
                    elif isinstance(pair_created_at, int):
                        # Unix timestamp in milliseconds
                        created_time = datetime.fromtimestamp(pair_created_at / 1000)
                    else:
                        raise ValueError(f"Unsupported timestamp format: {type(pair_created_at)}")

                    age_hours = (datetime.now() - created_time).total_seconds() / 3600
                    if age_hours > 24:
                        continue
                except Exception:
                    continue

                # Mark as seen
                seen_tokens.add(token_address)

                # Get liquidity in SOL
                liquidity_usd = pair.get("liquidity", {}).get("usd", 0)
                liquidity_sol = liquidity_usd / 200 if liquidity_usd > 0 else 0  # Rough approximation

                token_data = {
                    "token_address": token_address,
                    "creator": "unknown",
                    "liquidity_sol": round(liquidity_sol, 2),
                    "timestamp": datetime.now().isoformat(),
                    "source": "dexscreener",
                    "fdv": fdv,
                    "market_cap": market_cap
                }

                new_tokens.append(token_data)
                logger.info(f"Found new DexScreener token: {token_address} (Cap: ${cap_to_check:,.0f})")

            return new_tokens

    except Exception as e:
        logger.error(f"Error fetching from DexScreener API: {e}")
        return []


async def save_tokens_to_file(new_tokens: List[Dict[str, Any]]):
    """Save new tokens to the pools file with deduplication"""
    try:
        os.makedirs("data", exist_ok=True)
        file_path = "data/new_pools_raw.json"

        # Load existing data
        existing_pools = []
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                existing_pools = json.load(f)

        # Create set of existing token addresses
        existing_tokens = {pool["token_address"] for pool in existing_pools}

        # Filter new tokens not already in file
        filtered_new_tokens = [token for token in new_tokens if token["token_address"] not in existing_tokens]

        if filtered_new_tokens:
            all_pools = existing_pools + filtered_new_tokens
            with open(file_path, 'w') as f:
                json.dump(all_pools, f, indent=2)
            logger.info(f"Saved {len(filtered_new_tokens)} new Pump.fun tokens to {file_path}")

    except Exception as e:
        logger.error(f"Error saving Pump.fun tokens: {e}")


async def run_collector():
    """Run the DexScreener token collector"""
    logger.info("Starting DexScreener token collector...")

    # Load existing tokens to avoid duplicates
    try:
        if os.path.exists("data/new_pools_raw.json"):
            with open("data/new_pools_raw.json", 'r') as f:
                existing_pools = json.load(f)
                global seen_tokens
                seen_tokens = {pool["token_address"] for pool in existing_pools if pool.get("source") == "dexscreener"}
                logger.info(f"Loaded {len(seen_tokens)} existing DexScreener tokens")
    except Exception as e:
        logger.warning(f"Could not load existing tokens: {e}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                logger.info("Fetching new tokens from Pump.fun...")
                new_tokens = await fetch_new_tokens(session)

                if new_tokens:
                    await save_tokens_to_file(new_tokens)
                else:
                    logger.info("No new tokens found")

                # Wait 5 minutes before next fetch
                await asyncio.sleep(300)

            except KeyboardInterrupt:
                logger.info("Stopping DexScreener collector...")
                break
            except Exception as e:
                logger.error(f"Error in collector loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error


def get_recent_pools(limit: int = 50) -> List[str]:
    """Get list of recent DexScreener token addresses from the last 24 hours"""
    try:
        if not os.path.exists("data/new_pools_raw.json"):
            return []

        with open("data/new_pools_raw.json", 'r') as f:
            pools = json.load(f)

        # Filter by source and time
        cutoff = datetime.now() - timedelta(hours=24)
        recent_pools = []
        for pool in pools:
            if pool.get("source") != "dexscreener":
                continue

            try:
                pool_time = datetime.fromisoformat(pool["timestamp"])
                if pool_time > cutoff:
                    recent_pools.append(pool["token_address"])
            except:
                continue

        return recent_pools[-limit:] if len(recent_pools) > limit else recent_pools

    except Exception as e:
        logger.error(f"Error getting recent DexScreener pools: {e}")
        return []


if __name__ == "__main__":
    asyncio.run(run_collector())