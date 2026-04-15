"""
Free Discovery Aggregator - Level 1-2 Pipeline (улучшенная версия)
"""

import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from collectors.free_discovery_aggregator import FreeDiscoveryAggregator
from collectors.jupiter_arb_scanner import JupiterArbScanner   # ← Новый импорт
from analytics.arb_scanner import scan_arb_opportunities, generate_arb_coding_agent_prompt  # ← Arb scanner import
from config.settings import load_settings

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

    settings = load_settings()

    logger.info(f"Starting Free Discovery Aggregation with Moon Score Engine (max={args.max_candidates})")

    aggregator = FreeDiscoveryAggregator(max_candidates=args.max_candidates)

    try:
        collected_data = await aggregator.collect_all()

        # ====================== JUPITER ARB SCANNER ======================
        scanner = JupiterArbScanner(
            amount_in_lamports=int(settings.JUPITER_ARB_AMOUNT_IN_SOL * 1_000_000_000),  # SOL to lamports
            min_profit_pct=settings.JUPITER_ARB_MIN_PROFIT_PCT,
            max_concurrency=settings.JUPITER_ARB_MAX_CONCURRENCY,
            use_lite_api=settings.JUPITER_ARB_USE_LITE_API,
            slippage_bps=settings.JUPITER_ARB_SLIPPAGE_BPS,
            use_light_prescreening=True  # Pre-screen with LightArbDetector (arb_spread_score >= 4)
        )
        arb_opportunities = await scanner.scan_tokens(collected_data.get("new_pools", []))
        collected_data["arb_opportunities"] = [vars(o) for o in arb_opportunities]

        high_arb = [o for o in arb_opportunities if o.arb_score >= 60]
        logger.info(f"Found {len(high_arb)} high-confidence arbitrage opportunities (Score ≥ 60)")

        # ====================== BUILD AGGREGATE TEXT ======================
        aggregate_text = await aggregator.build_aggregate_text(collected_data)

        # Добавляем Jupiter Arb Scanner Summary в начало отчёта
        arb_section = scanner.to_daily_aggregate_section(arb_opportunities)
        aggregate_text = arb_section + "\n\n" + aggregate_text

        # Добавляем Arb Coding Agent Prompt
        if arb_opportunities:
            from analytics.arb_scanner import from_jupiter_opportunities
            arb_dicts = from_jupiter_opportunities(arb_opportunities)
            arb_prompt = generate_arb_coding_agent_prompt(arb_dicts)
            aggregate_text += "\n\n" + arb_prompt

        # ====================== SAVE HISTORY AND OUTPUT ======================
        if args.save_history:
            aggregator.save_history(aggregate_text, collected_data)

        output_dir = Path(args.output_dir)
        output_dir.mkdir(exist_ok=True)

        # Создаем директории и файлы-заглушки для реестров смарт-кошельков
        registry_dir = Path("data/registry")
        registry_dir.mkdir(parents=True, exist_ok=True)

        raw_wallets_path = Path("data/smart_wallets.raw.json")
        if not raw_wallets_path.exists():
            raw_wallets_path.parent.mkdir(parents=True, exist_ok=True)
            raw_wallets_path.write_text("[]")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"daily_aggregate_moon_{timestamp}.txt"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(aggregate_text)

        # ====================== LLM OPTIMIZER PROMPT (PR-5) ======================
        print("\n" + "="*95)
        print("💡 ГОТОВО ДЛЯ LLM АНАЛИЗА — СКОПИРУЙ ВЕСЬ ФАЙЛ И ВСТАВЬ В ЧАТ")
        print("="*95)
        print("\nСкопируй **всё содержимое** файла и вставь после следующего системного промпта:\n")

        print("=== СИСТЕМНЫЙ ПРОМПТ (вставь первым) ===")
        print("""Ты — Solana Tier-1 Alpha Scout (AI-агент хедж-фонда).
Твоя задача: анализировать ежедневный free discovery aggregate.

ПРАВИЛА (строго соблюдай):
1. Игнорируй любой скам, rug, высокий dev-sell, концентрацию холдеров >75%.
2. Приоритизируй токены с moon_score >= 45 ИЛИ is_graduating=True ИЛИ arb_score >= 60.
3. Анализируй Moon Score, GitHub velocity, buyer velocity, liquidity inflow, cross-chain presence.
4. Выводи ТОЛЬКО чистый JSON-массив. Никакого Markdown, никакого объяснения, никакого дополнительного текста до или после JSON.
5. Если ничего достойного — верни пустой массив [].

Структура каждого объекта:
{
  "symbol": string,
  "token_address": string,
  "moon_score": number,
  "arb_score": number,
  "flash_loan_ready": boolean,
  "curve_progress": number | null,
  "action": "BUY" | "SCALP" | "ARB" | "WATCH" | "IGNORE",
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reason": string (коротко, 8-15 слов),
  "risk_level": "LOW" | "MEDIUM" | "HIGH",
  "suggested_position_pct": number (0-5),
  "tags": array of strings (например ["graduating", "ai-agent", "high-velocity"])
}

Выведи только JSON.""")
        print("====================================\n")

        print(f"📄 Файл готов: {output_file.absolute()}")
        print("✅ Просто скопируй всё содержимое файла и вставь в Claude / ChatGPT / Grok после промпта выше.")
        print("🎯 Модель вернёт чистый JSON, который можно сразу парсить и отправлять в trading engine.\n")
        print("="*95)

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))