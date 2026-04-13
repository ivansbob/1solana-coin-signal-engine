#!/usr/bin/env python3
"""
Free Discovery Aggregator - Level 1-2 Pipeline (улучшенная версия)
"""

import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from collectors.free_discovery_aggregator import FreeDiscoveryAggregator
from collectors.moon_score_engine import calculate_moon_score   # ← Новый импорт
from analytics.arb_scanner import scan_arb_opportunities, generate_arb_coding_agent_prompt  # ← Arb scanner import

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="Free Discovery Aggregator with Moon Score")
    parser.add_argument("--max-candidates", type=int, default=30, help="Maximum candidates to process")
    parser.add_argument("--save-history", action="store_true", default=True)
    parser.add_argument("--output-dir", type=str, default=".")

    args = parser.parse_args()
    load_dotenv()

    logger.info(f"Starting Free Discovery Aggregation with Moon Score Engine (max={args.max_candidates})")

    aggregator = FreeDiscoveryAggregator(max_candidates=args.max_candidates)

    try:
        collected_data = await aggregator.collect_all()

        # ====================== MOON SCORE ENGINE ======================
        if "new_pools" in collected_data and collected_data["new_pools"]:
            logger.info(f"Applying Moon Score to {len(collected_data['new_pools'])} tokens...")

            scored_pools = []
            for token in collected_data["new_pools"]:
                scored = calculate_moon_score(token)
                scored_pools.append(scored)

            # Сортируем по Moon Score (самые горячие сверху)
            collected_data["new_pools"] = sorted(
                scored_pools,
                key=lambda x: x.get("moon_score", 0),
                reverse=True
            )

            high_moon = [t for t in collected_data["new_pools"] if t.get("moon_score", 0) >= 70]
            logger.info(f"Found {len(high_moon)} high-potential tokens (Moon Score ≥ 70)")

        # ====================== ARB SCANNER ======================
        logger.info("Scanning for arbitrage opportunities...")
        arb_opportunities = await scan_arb_opportunities()
        collected_data["arb_opportunities"] = arb_opportunities
        logger.info(f"Found {len(arb_opportunities)} arbitrage opportunities")

        # ====================== BUILD AGGREGATE TEXT ======================
        aggregate_text = await aggregator.build_aggregate_text(collected_data)

        # Добавляем Moon Score Summary в начало отчёта
        moon_summary = "\n## MOON SCORE SUMMARY (Zero-LLM Heuristic)\n"
        moon_summary += f"Total tokens analyzed: {len(collected_data.get('new_pools', []))}\n"
        moon_summary += f"High Moon Score (≥70): {len([t for t in collected_data.get('new_pools', []) if t.get('moon_score', 0) >= 70])}\n\n"
        aggregate_text = moon_summary + aggregate_text

        # Добавляем Arb Coding Agent Prompt
        if arb_opportunities:
            arb_prompt = generate_arb_coding_agent_prompt(arb_opportunities)
            aggregate_text += "\n\n" + arb_prompt

        # ====================== SAVE HISTORY AND OUTPUT ======================
        if args.save_history:
            aggregator.save_history(aggregate_text, collected_data)

        output_dir = Path(args.output_dir)
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"daily_aggregate_moon_{timestamp}.txt"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(aggregate_text)

        print(f"\n{'='*90}")
        print("FREE DISCOVERY AGGREGATOR + MOON SCORE COMPLETE")
        print(f"{'='*90}")
        print(f"📄 Saved: {output_file.absolute()}")
        print(f"🚀 High Moon Score tokens: {len([t for t in collected_data.get('new_pools', []) if t.get('moon_score', 0) >= 70])}")
        print(f"💡 Ready for LLM analysis or direct feeding to coding agent")
        print(f"{'='*90}\n")

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
