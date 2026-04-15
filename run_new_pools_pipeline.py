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
import csv
from typing import List, Dict, Any
from datetime import datetime
import asyncio
from dotenv import load_dotenv

load_dotenv(override=True)

# Add collectors to path
sys.path.append(os.path.dirname(__file__))

from collectors.raydium_pool_collector import get_raydium_pools
from collectors.pump_fun_collector import get_recent_pools as get_pump_pools
from collectors.security_checker import check_token
from collectors.github_signal import get_github_dev_score

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
    raydium_pools = await get_raydium_pools(50)
    raydium_tokens = [pool['token_address'] for pool in raydium_pools]
    pump_tokens = await get_pump_pools(50)

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

    # Run security checks in parallel
    logger.info("Running security checks...")
    safe_tokens = []

    async def check_single_token(token):
        source = pool_data.get(token, {}).get('source', 'unknown')
        logger.info(f"Checking token: {token} (source: {source})")
        try:
            result = await check_token(token, source=source)
            return token, result
        except Exception as e:
            logger.error(f"Error checking token {token}: {e}")
            return token, None

    # Run all checks in parallel with rate limiting
    semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests to avoid RPC limits

    async def check_with_limit(token):
        async with semaphore:
            return await check_single_token(token)

    tasks = [check_with_limit(token) for token in all_tokens]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Task failed with exception: {result}")
            continue
        token, check_result = result
        if check_result is None:
            continue
        if check_result.get('safe'):
            # Enrich with pool data
            pool_info = pool_data.get(token, {})
            check_result.update({
                'liquidity_sol': pool_info.get('liquidity_sol', 0.0),
                'source': pool_info.get('source', 'unknown'),
                'timestamp': pool_info.get('timestamp', datetime.now().isoformat())
            })
            safe_tokens.append(check_result)
            score = check_result.get('risk_score', 'N/A')
            logger.info(f"✓ Token {token} passed security check (Score: {score})")
        else:
            reasons = check_result.get('reasons', [])
            reason_str = " | ".join(reasons) if reasons else "Unknown"
            score = check_result.get('risk_score', 'N/A')
            logger.info(f"✗ Token {token} failed security check: {check_result.get('status', 'UNKNOWN')} (Score: {score}) (Reasons: {reason_str})")

            # For WARN tokens, get GitHub dev score if symbol available
            if check_result.get('status') == 'WARN':
                pool_info = pool_data.get(token, {})
                symbol = pool_info.get('symbol')
                if symbol:
                    try:
                        github_score = await get_github_dev_score(symbol)
                        check_result['github_dev_score'] = github_score
                        logger.info(f"GitHub dev score for {token} ({symbol}): {github_score}")
                    except Exception as e:
                        logger.error(f"Error getting GitHub score for {token}: {e}")

    logger.info(f"Security checks complete: {len(safe_tokens)}/{len(all_tokens)} tokens passed")

    if not safe_tokens:
        logger.info("No safe tokens to aggregate")
        return

    # Prepare output data
    output_lines = []
    for result in safe_tokens:
        mint = result.get('token_address', '')
        score = result.get('risk_score', 0)
        verdict = result.get('verdict', 'UNKNOWN')
        flags = "|".join(result.get('reasons', []))
        liq_usd = result.get('lp_liquidity_usd', 0.0)
        source = result.get('source', 'unknown')
        timestamp = result.get('timestamp', datetime.now().isoformat())
        symbol = pool_data.get(mint, {}).get("symbol", "") or "???"

        line = f"{timestamp} | {mint} | {symbol} | risk={score:.1f} | verdict={verdict} | flags={flags} | liq=${liq_usd:.0f} | src={source}"
        output_lines.append(line)

    # Prepare data for CSV
    csv_data = []
    for result in safe_tokens:
        csv_data.append({
            'token_address': result.get('token_address', ''),
            'score': result.get('risk_score', 0),
            'liquidity_sol': result.get('liquidity_sol', 0.0),
            'source': result.get('source', 'unknown'),
            'timestamp': result.get('timestamp', datetime.now().isoformat())
        })

    # Write to file or dry run
    if dry_run:
        logger.info("DRY RUN - Would write the following to data/daily_aggregate.txt:")
        for line in output_lines:
            print(line)
        logger.info("DRY RUN - Would export to data/signals.csv")
    else:
        output_file = "data/daily_aggregate.txt"
        try:
            with open(output_file, 'a') as f:
                for line in output_lines:
                    f.write(line + '\n')
            logger.info(f"Results appended to {output_file}")
        except Exception as e:
            logger.error(f"Error writing to {output_file}: {e}")

        # Export to CSV
        csv_file = "data/signals.csv"
        try:
            with open(csv_file, 'w', newline='') as f:
                fieldnames = ['token_address', 'score', 'liquidity_sol', 'source', 'timestamp']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)
            logger.info(f"Data exported to {csv_file}")
        except Exception as e:
            logger.error(f"Error exporting to {csv_file}: {e}")

        # Append to entry candidates
        os.makedirs("data/processed", exist_ok=True)
        entry_file = "data/processed/entry_candidates.json"
        try:
            with open(entry_file, 'a') as f:
                for result in safe_tokens:
                    entry = result.copy()
                    entry["discovered_at"] = datetime.now().isoformat()
                    f.write(json.dumps(entry) + '\n')
            logger.info(f"Entries appended to {entry_file}")
        except Exception as e:
            logger.error(f"Error appending to {entry_file}: {e}")

    # Summary with counts
    all_check_results = []
    for result in results:
        if isinstance(result, Exception):
            continue
        token, check_result = result
        if check_result:
            all_check_results.append(check_result)

    pass_count = sum(1 for r in all_check_results if r.get('verdict') == 'PASS')
    warn_count = sum(1 for r in all_check_results if r.get('verdict') == 'WARN')
    block_count = sum(1 for r in all_check_results if r.get('verdict') == 'BLOCK')
    honeypot_count = sum(1 for r in all_check_results if r['honeypot_data']['is_honeypot'])

    logger.info(f"PASS={pass_count} WARN={warn_count} BLOCK={block_count} HONEYPOT={honeypot_count}")
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