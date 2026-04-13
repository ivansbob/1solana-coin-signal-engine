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
PUMP_FUN_API_URL = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC"
MIN_FDV = 10000  # $10k minimum fully diluted valuation

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
seen_tokens = set()


async def fetch_new_tokens(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fetch new tokens from Pump.fun API"""
    try:
        async with session.get(PUMP_FUN_API_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.error(f"Pump.fun API returned status {resp.status}")
                return []

            data = await resp.json()

            logger.info(f"Pump.fun API returned: {type(data)}")

            # Assume data is list of coins
            coins = data if isinstance(data, list) else []

            logger.info(f"Processing {len(coins)} coins")

            new_tokens = []
            for coin in coins:
                # Extract token info from coin data
                token_address = coin.get("mint")
                symbol = coin.get("symbol", "")

                # Skip if not a valid token
                if not token_address:
                    continue

                # Get creation time
                created_timestamp = coin.get("created_timestamp")
                if created_timestamp:
                    try:
                        created_time = datetime.fromtimestamp(created_timestamp / 1000)
                        age_hours = (datetime.now() - created_time).total_seconds() / 3600
                        if age_hours > 24:
                            continue
                    except:
                        continue
                else:
                    continue

                # Get market cap
                market_cap = coin.get("market_cap", 0)
                if market_cap < MIN_FDV:
                    continue

                # Skip if already seen
                if token_address in seen_tokens:
                    continue

                # Mark as seen
                seen_tokens.add(token_address)

                token_data = {
                    "token_address": token_address,
                    "creator": "unknown",
                    "liquidity_sol": 0.0,  # Not available
                    "timestamp": datetime.now().isoformat(),
                    "source": "pumpfun",
                    "fdv": market_cap,
                    "market_cap": market_cap
                }

                new_tokens.append(token_data)
                logger.info(f"Found new Pump.fun token: {token_address} (Cap: ${market_cap:,.0f})")

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
                seen_tokens = {pool["token_address"] for pool in existing_pools if pool.get("source") in ["dexscreener", "pumpfun"]}
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
        recent_pools = []
        for pool in pools:
            if pool.get("source") not in ["dexscreener", "pumpfun"]:
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


async def run_once():
    """Run one-time collection from DexScreener API for testing"""
    logger.info("Running one-time collection from DexScreener API...")

    async with aiohttp.ClientSession() as session:
        try:
            new_tokens = await fetch_new_tokens(session)

            if new_tokens:
                await save_tokens_to_file(new_tokens)
                logger.info(f"Saved {len(new_tokens)} tokens from DexScreener API")
            else:
                logger.info("No new tokens found")

        except Exception as e:
            logger.error(f"Error in one-time collection: {e}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        asyncio.run(run_once())
    else:
        asyncio.run(run_collector())