#!/usr/bin/env python3
"""
New Pools Pipeline Runner
Combines Raydium and Pump.fun collectors, runs security checks, and aggregates results.
"""

import argparse
import json
import logging
import os
import sys
from typing import List, Dict, Any
from datetime import datetime
import asyncio

# Add collectors to path
sys.path.append(os.path.dirname(__file__))

from collectors.raydium_pool_collector import get_recent_pools as get_raydium_pools
from collectors.pump_fun_collector import get_recent_pools as get_pump_pools
from collectors.security_checker import check_token

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_lookback_hours() -> int:
    """Load lookback hours from environment"""
    return int(os.getenv("NEW_POOLS_LOOKBACK_HOURS", "24"))


async def run_pipeline(dry_run: bool = False) -> None:
    """Run the complete new pools pipeline"""
    logger.info("Starting new pools pipeline...")

    # Ensure directories exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # Collect tokens from both sources
    logger.info("Collecting recent tokens...")
    raydium_tokens = get_raydium_pools(50)
    pump_tokens = get_pump_pools(50)

    # Combine and deduplicate
    all_tokens = list(set(raydium_tokens + pump_tokens))
    logger.info(f"Found {len(all_tokens)} unique tokens ({len(raydium_tokens)} Raydium, {len(pump_tokens)} Pump.fun)")

    if not all_tokens:
        logger.info("No tokens to process")
        return

    # Load pool data to enrich results
    pool_data = {}
    try:
        if os.path.exists("data/new_pools_raw.json"):
            with open("data/new_pools_raw.json", 'r') as f:
                pools = json.load(f)
                for pool in pools:
                    pool_data[pool["token_address"]] = pool
    except Exception as e:
        logger.warning(f"Could not load pool data: {e}")

    # Run security checks
    logger.info("Running security checks...")
    safe_tokens = []

    for i, token in enumerate(all_tokens, 1):
        logger.info(f"Checking token {i}/{len(all_tokens)}: {token}")

        try:
            result = check_token(token)
            if result.get('safe'):
                # Enrich with pool data
                pool_info = pool_data.get(token, {})
                result.update({
                    'liquidity_sol': pool_info.get('liquidity_sol', 0.0),
                    'source': pool_info.get('source', 'unknown'),
                    'timestamp': pool_info.get('timestamp', datetime.now().isoformat())
                })
                safe_tokens.append(result)
                logger.info(f"✓ Token {token} passed security check")
            else:
                logger.info(f"✗ Token {token} failed security check: {result.get('status', 'UNKNOWN')}")

            # Rate limiting to avoid API bans
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Error checking token {token}: {e}")
            continue

    logger.info(f"Security checks complete: {len(safe_tokens)}/{len(all_tokens)} tokens passed")

    if not safe_tokens:
        logger.info("No safe tokens to aggregate")
        return

    # Prepare output data
    output_lines = []
    for result in safe_tokens:
        token_address = result.get('token_address', '')
        score = result.get('risk_score', 0)
        liquidity = result.get('liquidity_sol', 0.0)
        source = result.get('source', 'unknown')
        timestamp = result.get('timestamp', datetime.now().isoformat())

        line = f"{token_address} | {score} | {liquidity} | {source} | {timestamp}"
        output_lines.append(line)

    # Write to file or dry run
    if dry_run:
        logger.info("DRY RUN - Would write the following to data/daily_aggregate.txt:")
        for line in output_lines:
            print(line)
    else:
        output_file = "data/daily_aggregate.txt"
        try:
            with open(output_file, 'w') as f:
                for line in output_lines:
                    f.write(line + '\n')
            logger.info(f"Results written to {output_file}")
        except Exception as e:
            logger.error(f"Error writing to {output_file}: {e}")

    # Summary
    logger.info(f"Pipeline complete: Processed {len(all_tokens)} tokens, {len(safe_tokens)} safe, {len(output_lines)} aggregated")


def main():
    parser = argparse.ArgumentParser(description="Run new pools security pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline but don't write results to file"
    )

    args = parser.parse_args()

    # Run the async pipeline
    asyncio.run(run_pipeline(dry_run=args.dry_run))


if __name__ == "__main__":
    main()