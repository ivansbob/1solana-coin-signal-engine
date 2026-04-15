import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import aiohttp
from dotenv import load_dotenv
from config.settings import load_settings

load_dotenv()

settings = load_settings()

# Constants
RAYDIUM_FEE_ADDRESS = "7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5"
SOL_MINT = "So11111111111111111111111111111111111111112"
RAYDIUM_LP_VAULT = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
MIN_LIQUIDITY_SOL = float(os.getenv("MIN_LIQUIDITY_SOL", "1.0"))

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
seen_signatures = set()
pools_data: List[Dict[str, Any]] = []


async def get_transaction(session: aiohttp.ClientSession, signature: str, rpc_url: str) -> Optional[Dict[str, Any]]:
    """Get transaction details with retry logic"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
        ]
    }

    for attempt in range(3):
        try:
            async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("result"):
                        return data["result"]
                    else:
                        logger.warning(f"Transaction {signature} returned null (attempt {attempt+1})")
                        await asyncio.sleep(2)
                        continue
                else:
                    logger.error(f"RPC error {resp.status} for {signature}")
        except Exception as e:
            logger.error(f"Error getting transaction {signature}: {e}")

    return None


def extract_pool_info(tx_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract pool information from transaction data"""
    try:
        meta = tx_data.get("meta", {})
        post_balances = meta.get("postTokenBalances", [])

        base_address = None
        quote_amount = 0.0

        for balance in post_balances:
            mint = balance.get("mint")
            owner = balance.get("owner")
            ui_amount = balance.get("uiTokenAmount", {}).get("uiAmount", 0)

            if owner == RAYDIUM_LP_VAULT and mint != SOL_MINT:
                base_address = mint
            elif mint == SOL_MINT:
                quote_amount = ui_amount or 0

        if base_address and quote_amount >= MIN_LIQUIDITY_SOL:
            account_keys = tx_data.get("transaction", {}).get("accountKeys", [])
            creator = account_keys[0] if account_keys else None

            return {
                "token_address": base_address,
                "symbol": "",  # Not available in websocket data
                "creator": creator,
                "liquidity_sol": quote_amount,
                "timestamp": datetime.now().isoformat(),
                "source": "raydium"
            }

    except Exception as e:
        logger.error(f"Error extracting pool info: {e}")

    return None


async def save_pools_to_file():
    """Save pools data to file with deduplication"""
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

        # Filter new pools
        new_pools = [pool for pool in pools_data if pool["token_address"] not in existing_tokens]

        if new_pools:
            all_pools = existing_pools + new_pools
            with open(file_path, 'w') as f:
                json.dump(all_pools, f, indent=2)
            logger.info(f"Saved {len(new_pools)} new pools to {file_path}")

        # Clear processed pools
        pools_data.clear()

    except Exception as e:
        logger.error(f"Error saving pools: {e}")


async def process_log_message(message: Dict[str, Any], session: aiohttp.ClientSession, rpc_url: str):
    """Process a log message from websocket"""
    try:
        params = message.get("params", {})
        result = params.get("result", {})
        value = result.get("value", {})

        if value.get("account", {}).get("pubkey") != RAYDIUM_FEE_ADDRESS:
            return

        logs = value.get("logs", [])
        signature = None

        # Extract signature from logs (simplified approach)
        for log in logs:
            if "Program log:" in log and "instruction:" in log:
                # This is a simplified extraction - in practice you'd parse more carefully
                signature = result.get("signature")
                break

        if not signature or signature in seen_signatures:
            return

        seen_signatures.add(signature)

        # Get transaction details
        tx_data = await get_transaction(session, signature, rpc_url)
        if not tx_data:
            return

        # Extract pool info
        pool_info = extract_pool_info(tx_data)
        if pool_info:
            pools_data.append(pool_info)
            logger.info(f"Found new Raydium pool: {pool_info['token_address']} with {pool_info['liquidity_sol']} SOL")

            # Save periodically (every 10 pools or so)
            if len(pools_data) >= 10:
                await save_pools_to_file()

    except Exception as e:
        logger.error(f"Error processing log message: {e}")


async def websocket_listener():
    """Main websocket listener for Raydium logs"""
    # Use Helius RPC as primary provider if API key is available
    if settings.HELIUS_API_KEY:
        ws_url = f"wss://mainnet.helius-rpc.com/?api-key={settings.HELIUS_API_KEY}"
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={settings.HELIUS_API_KEY}"
        logger.info("Using Helius RPC for Raydium data fetching")
    else:
        ws_url = os.getenv("SOLANA_RPC_WS", "wss://api.mainnet-beta.solana.com")
        rpc_url = os.getenv("SOLANA_RPC_HTTP", "https://api.mainnet-beta.solana.com")
        logger.warning("HELIUS_API_KEY not found, falling back to public Solana RPC")

    logger.info(f"Connecting to Solana websocket: {ws_url}")

    max_reconnect_attempts = 5
    reconnect_attempts = 0

    while reconnect_attempts < max_reconnect_attempts:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    logger.info("WebSocket connected")
                    reconnect_attempts = 0  # Reset on successful connection

                    # Subscribe to logs
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [RAYDIUM_FEE_ADDRESS]},
                            {"commitment": "confirmed"}
                        ]
                    }

                    await ws.send_json(subscribe_msg)
                    logger.info(f"Subscribed to logs for {RAYDIUM_FEE_ADDRESS}")

                    # Start heartbeat task
                    async def heartbeat():
                        while True:
                            await asyncio.sleep(30)
                            try:
                                # Send a ping to keep connection alive
                                await ws.ping()
                                logger.debug("Sent heartbeat ping")
                            except Exception:
                                break

                    heartbeat_task = asyncio.create_task(heartbeat())

                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    await process_log_message(data, session, rpc_url)
                                except json.JSONDecodeError:
                                    continue
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"WebSocket error: {msg}")
                                break
                    finally:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

        except Exception as e:
            reconnect_attempts += 1
            logger.error(f"WebSocket connection failed (attempt {reconnect_attempts}/{max_reconnect_attempts}): {e}")
            if reconnect_attempts < max_reconnect_attempts:
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            else:
                logger.error("Max reconnection attempts reached, exiting")
                break


async def run_collector():
    """Run the Raydium pool collector"""
    logger.info("Starting Raydium pool collector...")

    # Load existing signatures to avoid duplicates
    try:
        if os.path.exists("data/new_pools_raw.json"):
            with open("data/new_pools_raw.json", 'r') as f:
                existing_pools = json.load(f)
                global seen_signatures
                # Note: In practice, you'd want to track signatures more persistently
                seen_signatures = set()  # Reset for now
    except Exception as e:
        logger.warning(f"Could not load existing pools: {e}")

    try:
        await websocket_listener()
    except KeyboardInterrupt:
        logger.info("Stopping collector...")
        await save_pools_to_file()


async def get_raydium_pools(limit: int = 50) -> List[Dict[str, Any]]:
    """Get list of recent pools from the last 24 hours with full data"""
    try:
        if not os.path.exists("data/new_pools_raw.json"):
            return []

        with open("data/new_pools_raw.json", 'r') as f:
            pools = json.load(f)

        # Filter by last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        recent_pools = []
        for pool in pools:
            try:
                pool_time = datetime.fromisoformat(pool["timestamp"])
                if pool_time > cutoff:
                    # Add missing fields for compatibility
                    pool.setdefault("age_minutes", 0)
                    pool.setdefault("volume_1h", 0.0)
                    pool.setdefault("liquidity_usd", pool.get("liquidity_sol", 0) * 200)  # rough SOL to USD
                    recent_pools.append(pool)
            except:
                continue

        return recent_pools[:limit]
    except Exception as e:
        logger.error(f"Error getting recent pools: {e}")
        return []

        with open("data/new_pools_raw.json", 'r') as f:
            pools = json.load(f)

        # Filter by last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        recent_pools = []
        for pool in pools:
            try:
                pool_time = datetime.fromisoformat(pool["timestamp"])
                if pool_time > cutoff:
                    recent_pools.append(pool["token_address"])
            except:
                continue

        return recent_pools[-limit:] if len(recent_pools) > limit else recent_pools

    except Exception as e:
        logger.error(f"Error getting recent pools: {e}")
        return []


async def run_once():
    """Run one-time collection from Raydium HTTP API for testing"""
    logger.info("Running one-time collection from Raydium API...")

    url = "https://api.raydium.io/v2/main/pairs"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    logger.error(f"Raydium API returned status {resp.status}")
                    return

                data = await resp.json()

                # Sort by createdAt descending and take first 20
                if isinstance(data, list):
                    sorted_pairs = sorted(data, key=lambda x: x.get("createdAt", 0), reverse=True)
                    recent_pairs = sorted_pairs[:20]
                    logger.info(f"Processing {len(recent_pairs)} most recent pairs")
                else:
                    logger.error(f"Unexpected API response format: {type(data)}")
                    return

                new_tokens = []
                for pair in recent_pairs:
                    base_mint = pair.get("baseMint")
                    if base_mint and base_mint != SOL_MINT:
                        token_data = {
                            "token_address": base_mint,
                            "symbol": pair.get("baseToken", {}).get("symbol", "") or pair.get("symbol", ""),
                            "creator": "unknown",  # Not available in this API
                            "liquidity_sol": 0.0,  # Not available in this API
                            "timestamp": datetime.now().isoformat(),
                            "source": "raydium_once"
                        }
                        new_tokens.append(token_data)

                if new_tokens:
                    # Add to global pools_data instead of calling save_pools_to_file directly
                    global pools_data
                    pools_data.extend(new_tokens)
                    await save_pools_to_file()
                    logger.info(f"Saved {len(new_tokens)} tokens from Raydium API")
                else:
                    logger.info("No new tokens found")

        except Exception as e:
            logger.error(f"Error in one-time collection: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        asyncio.run(run_once())
    else:
        asyncio.run(run_collector())