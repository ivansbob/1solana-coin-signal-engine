import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import aiohttp

logger = logging.getLogger(__name__)

from utils.io import append_jsonl, write_json, ensure_dir
from utils.rate_limit import async_acquire
from utils.retry import with_retry

PUMP_FUN_API_URL = "https://frontend-api.pump.fun/coins?offset=0&limit=80&sort=last_trade_timestamp&order=DESC"

# Успешные метрики 2026 (на основе реальных данных Pump.fun graduates)
MIN_MARKET_CAP = 35_000          # начинаем смотреть отсюда
GRADUATION_THRESHOLD = 90.0      # % до миграции на Raydium
HIGH_VELOCITY_THRESHOLD = 25.0   # сильный buy pressure

SCAM_FILTERS = ["dev", "team", "tax", "renounced", "locked", "burned"]  # косвенные признаки


async def fetch_pump_fun_coins() -> List[Dict]:
    """Получаем актуальные токены, отсортированные по последней торговле."""
    await async_acquire("dex")  # используем существующий rate limiter

    async with aiohttp.ClientSession() as session:
        async with session.get(PUMP_FUN_API_URL) as resp:
            if resp.status != 200:
                print(f"Pump.fun API error: {resp.status}")
                return []
            return await resp.json()


def calculate_graduation_score(coin: Dict[str, Any]) -> Dict[str, Any]:
    """Улучшенный scoring на основе реальных предикторов успеха после graduation."""
    market_cap = float(coin.get("usd_market_cap") or 0)
    virtual_sol_reserves = float(coin.get("virtual_sol_reserves") or 0)
    reply_count = int(coin.get("reply_count") or 0)
    last_trade_timestamp = coin.get("last_trade_timestamp")

    # Bonding curve progress
    curve_progress_pct = min((market_cap / 69_000) * 100, 100.0)

    # Buy pressure velocity (самая важная метрика успеха)
    buy_pressure = float(coin.get("buy_pressure") or coin.get("v24hUSD") or 0)
    unique_buyers = int(coin.get("unique_buyers_1h") or coin.get("holder_count", 0) - 1)

    # Acceleration proxy (сколько уникальных покупателей за последний час)
    buyer_velocity = unique_buyers / max(1, (datetime.now(timezone.utc).timestamp() - int(last_trade_timestamp or 0)) / 3600)

    # Dev sell pressure (критично низкий = хороший знак)
    dev_sell_pressure = float(coin.get("dev_sell_pressure_5m") or 0)

    # Holder distribution quality
    holder_concentration = float(coin.get("top10_holder_pct") or 0.65)  # чем ниже — тем лучше

    # Основной score
    base_score = curve_progress_pct * 0.4

    if buyer_velocity > HIGH_VELOCITY_THRESHOLD:
        base_score += 35
    if unique_buyers > 40 and curve_progress_pct > 70:
        base_score += 25
    if dev_sell_pressure < 0.03:
        base_score += 20
    if holder_concentration < 0.45:
        base_score += 15

    # Штрафы
    if dev_sell_pressure > 0.15:
        base_score -= 30
    if holder_concentration > 0.75:
        base_score -= 25

    risk_adjusted_score = round(max(10, min(100, base_score)), 2)

    is_graduating = curve_progress_pct >= GRADUATION_THRESHOLD
    is_high_potential = risk_adjusted_score >= 75 and is_graduating

    return {
        "market_cap": round(market_cap, 0),
        "curve_progress_pct": round(curve_progress_pct, 1),
        "buyer_velocity": round(buyer_velocity, 2),
        "unique_buyers_1h": unique_buyers,
        "dev_sell_pressure_5m": round(dev_sell_pressure, 4),
        "holder_concentration": round(holder_concentration, 3),
        "risk_adjusted_graduation_score": risk_adjusted_score,
        "is_graduating": is_graduating,
        "is_high_potential": is_high_potential,
        "graduation_reason_codes": [
            "strong_buyer_velocity" if buyer_velocity > HIGH_VELOCITY_THRESHOLD else "",
            "low_dev_sell" if dev_sell_pressure < 0.05 else "",
            "healthy_distribution" if holder_concentration < 0.50 else ""
        ],
        "provenance": "pump_fun_graduation_tracker_2026"
    }


async def get_recent_pools(limit: int = 50) -> List[str]:
    """Get list of recent pump.fun token addresses"""
    high_potential = await run_pump_fun_graduation_tracker(limit)
    return [token['token_address'] for token in high_potential]


async def run_pump_fun_graduation_tracker(max_tokens: int = 50) -> List[Dict]:
    """Основная функция трекера."""
    coins = await fetch_pump_fun_coins()
    seen_tokens = set()
    high_potential_tokens = []

    for coin in coins:
        mint = coin.get("mint")
        if not mint or mint in seen_tokens:
            continue
        seen_tokens.add(mint)

        symbol = coin.get("symbol", "UNKNOWN")
        market_cap = float(coin.get("usd_market_cap") or 0)

        if market_cap < MIN_MARKET_CAP:
            continue

        score_data = calculate_graduation_score(coin)

        token_data = {
            "token_address": mint,
            "symbol": symbol,
            "name": coin.get("name", ""),
            **score_data,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": "pump_fun",
            "raydium_migration_expected": score_data["is_graduating"]
        }

        if score_data["is_high_potential"]:
            print(f"🚀 HIGH POTENTIAL GRADUATE: {symbol} | Score: {score_data['risk_adjusted_graduation_score']} | MC: ${market_cap:,.0f}")
            high_potential_tokens.append(token_data)

        # Сохраняем все graduating токены
        if score_data["is_graduating"]:
            append_jsonl(Path("data/processed/pump_fun_graduating.jsonl"), token_data)

    # Сохраняем топ для агрегатора и coding agent
    write_json(Path("data/processed/pump_fun_high_potential.json"), {
        "as_of": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "high_potential_count": len(high_potential_tokens),
        "tokens": sorted(high_potential_tokens, key=lambda x: x["risk_adjusted_graduation_score"], reverse=True)[:20]
    })

    return high_potential_tokens


def generate_pump_fun_coding_agent_prompt(high_potential: List[Dict]) -> str:
    """Максимально полезный промпт для coding agent."""
    lines = [
        "# Pump.Fun Graduation → Autonomous Sniper / DLMM Agent Prompt 2026",
        "Ты — Solana AI Trading Agent. Используй данные ниже для создания/улучшения автономного агента.\n",
        "Фокус: Ловить токены за 5–15 минут до или сразу после миграции на Raydium с высоким buyer velocity.\n"
    ]

    for token in high_potential[:10]:
        score = token["risk_adjusted_graduation_score"]
        lines.append(f"## HIGH POTENTIAL: {token['symbol']} ({token['token_address']})")
        lines.append(f"Graduation Score: {score}/100 | Curve: {token['curve_progress_pct']}% | Buyer Velocity: {token.get('buyer_velocity', 0)}")
        lines.append(f"Dev Sell: {token.get('dev_sell_pressure_5m', 0)} | Holders Concentration: {token.get('holder_concentration', 0)}")
        lines.append("Рекомендации для агента:")
        lines.append("1. Мониторить этот mint через Helius WebSocket")
        lines.append("2. При curve_progress > 92% и buyer_velocity > 25 → готовить Jito bundle на покупку")
        lines.append("3. После миграции на Raydium: проверить liquidity depth и открыть позицию с evidence-weighted sizing")
        lines.append("4. Exit rules: trailing stop + dev-sell monitor + MFE capture\n")

    lines.append("\nЗадача: Напиши код агента, который автоматически:")
    lines.append("- Подключается к Pump.fun + Raydium")
    lines.append("- Использует velocity scoring из PR-2")
    lines.append("- Интегрируется с GitHub Velocity Tracker (PR-3)")
    lines.append("- Применяет continuation_enricher из основной системы")
    lines.append("- Логирует все решения с provenance")

    return "\n".join(lines)