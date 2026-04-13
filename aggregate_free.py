#!/usr/bin/env python3
"""
Free Discovery Aggregator - Level 1-2 Pipeline
Collects data from free sources and builds daily aggregate for LLM analysis.
"""

import asyncio
import argparse
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

from collectors.free_discovery_aggregator import FreeDiscoveryAggregator

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Free Discovery Aggregator")
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=25,
        help="Maximum number of candidates to process (default: 25)",
    )
    parser.add_argument(
        "--save-history",
        action="store_true",
        default=True,
        help="Save results to signals_history/ directory (default: True)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Output directory for aggregate file (default: current directory)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    logger.info(
        f"Starting free discovery aggregation (max_candidates={args.max_candidates})"
    )

    # Create aggregator
    aggregator = FreeDiscoveryAggregator(max_candidates=args.max_candidates)

    try:
        # Collect all data
        collected_data = await aggregator.collect_all()

        # Build aggregate text
        aggregate_text = await aggregator.build_aggregate_text(collected_data)

        # Save to history if requested
        if args.save_history:
            aggregator.save_history(aggregate_text, collected_data)

        # Also save to output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(exist_ok=True)

        timestamp = collected_data.get("timestamp", "unknown")
        dt_str = (
            timestamp.replace("T", "_")
            .replace(":", "")
            .replace("-", "")
            .replace("Z", "")[:15]
        )
        output_file = output_dir / f"daily_aggregate_{dt_str}.txt"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(aggregate_text)

        print(f"\n{'='*80}")
        print("FREE DISCOVERY AGGREGATE COMPLETE")
        print(f"{'='*80}")
        print(f"📄 File saved: {output_file.absolute()}")
        print(f"📊 Processed {len(collected_data.get('new_pools', []))} new pools")
        print(
            f"🌐 Found {len(collected_data.get('cross_chain_pools', []))} cross-chain opportunities"
        )
        print(
            f"🛡️  Security checked: {len(collected_data.get('security_results', []))} tokens"
        )
        print(f"📈 GitHub repos: {len(collected_data.get('github_repos', []))} active")
        print(f"🚀 Pump.Fun graduates: {len(collected_data.get('pump_fun_graduation_tokens', []))} high potential")
        print()
        print("💡 Copy the content from the file above to Claude/Grok/ChatGPT")
        print("🤖 Use the built-in LLM prompt at the end for analysis")
        print(f"{'='*80}\n")

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
