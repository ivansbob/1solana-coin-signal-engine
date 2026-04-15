import asyncio
import httpx
from typing import Dict, Any, List
from datetime import datetime, timezone
from pathlib import Path

from utils.cache import cache_get, cache_set
from utils.rate_limit import async_acquire
from utils.io import append_jsonl, write_json
from utils.logger import log_info, log_warning
from config.settings import Settings

COINGECKO_SEARCH_URL = "https://api.coingecko.com/api/v3/search"
COINGECKO_COIN_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}"

# Приоритетные сети для кросс-чейн арбитража 2026
TARGET_CHAINS = ["base", "arbitrum", "ethereum", "solana"]


async def fetch_coingecko_crosschain(symbol: str, cache_ttl: int = 300) -> Dict[str, Any]:
    """Улучшенный бесплатный поиск токена на CoinGecko + кэш."""
    cache_key = f"cg_search_{symbol.lower()}"
    cached = cache_get("dex", cache_key)  # используем существующий dex cache
    if cached:
        return cached

    await async_acquire("dex")  # rate limit (CoinGecko ~30/min)

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(COINGECKO_SEARCH_URL, params={"query": symbol})
            if resp.status_code != 200:
                log_warning("coingecko_search_failed", symbol=symbol, status=resp.status_code)
                return {"found": False, "error": "http_error"}

            data = resp.json()
            coins = data.get("coins", [])

            for coin in coins:
                if coin.get("symbol", "").lower() == symbol.lower():
                    result = {
                        "found": True,
                        "coin_id": coin.get("id"),
                        "name": coin.get("name"),
                        "symbol": coin.get("symbol"),
                        "market_cap_rank": coin.get("market_cap_rank"),
                        "thumb": coin.get("thumb"),
                        "provenance": "coingecko_search"
                    }
                    cache_set("dex", cache_key, result, ttl_sec=cache_ttl)
                    return result

    except Exception as e:
        log_warning("coingecko_exception", symbol=symbol, error=str(e))

    result = {"found": False}
    cache_set("dex", cache_key, result, ttl_sec=60)
    return result


async def get_crosschain_prices(coin_id: str) -> Dict[str, float]:
    """Получаем цены на приоритетных цепях (упрощённо через market data)."""
    prices = {}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # CoinGecko позволяет получать market data с платформами
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=false&developer_data=false"
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                market_data = data.get("market_data", {})
                # Можно расширить на конкретные платформы, но для бесплатного tier используем общие метрики
                prices["usd"] = market_data.get("current_price", {}).get("usd")
                # Для настоящих кросс-чейн цен лучше использовать GeckoTerminal onchain endpoints (если доступны)
    except Exception:
        pass
    return prices


def calculate_arb_opportunity_score(opp: Dict[str, Any], cross_chain: Dict[str, Any]) -> Dict[str, Any]:
    """Evidence-weighted Arbitrage Score (0-100)."""
    score = 0
    reasons = []

    # Solana-side spread (из твоего существующего сканера)
    sol_spread = float(opp.get("spread_pct", 0))
    if sol_spread > 1.5:
        score += 35
        reasons.append(f"Solana DEX spread {sol_spread:.2f}%")
    elif sol_spread > 0.8:
        score += 18
        reasons.append(f"Moderate Solana spread {sol_spread:.2f}%")

    # Cross-chain listing boost
    if cross_chain.get("found"):
        score += 25
        reasons.append(f"Listed on CEX/L2: {cross_chain.get('name')}")

    # Volume & liquidity filter
    volume = float(opp.get("volume_24h", 0))
    if volume > 500_000:
        score += 20
        reasons.append("High 24h volume")
    elif volume > 100_000:
        score += 10

    # Risk adjustment
    if float(opp.get("liquidity_usd", 0)) < 50_000:
        score -= 15
        reasons.append("Low liquidity warning")

    final_score = round(max(0, min(100, score)), 1)

    opp.update({
        "arb_score": final_score,
        "arb_reasons": reasons,
        "cross_chain_listed": cross_chain.get("found", False),
        "cross_chain_name": cross_chain.get("name"),
        "scored_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "provenance": "multi_chain_arb_scanner_2026"
    })

    return opp


async def scan_arb_opportunities(base_opportunities: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Основная функция сканирования арбитража + кросс-чейн."""
    opportunities = base_opportunities or []
    enriched = []

    # Если на вход ничего не дали, возвращаем пустой список, чтобы не крашнуться
    if not opportunities:
        return enriched

    for opp in opportunities[:40]:  # лимит для бесплатного tier
        symbol = opp.get("symbol", "").upper()
        if not symbol:
            continue

        # Параллельно проверяем кросс-чейн presence
        cross_chain = await fetch_coingecko_crosschain(symbol)
        scored = calculate_arb_opportunity_score(opp, cross_chain)
        enriched.append(scored)

        if scored.get("arb_score", 0) >= 65:
            log_info("high_arb_opportunity", symbol=symbol, score=scored["arb_score"], cross_chain=cross_chain.get("found", False))

    if enriched:
        write_json(Path("data/processed/arb_opportunities.json"), {
            "as_of": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "opportunities": sorted(enriched, key=lambda x: x.get("arb_score", 0), reverse=True)
        })

    return enriched


def generate_arb_coding_agent_prompt(opportunities: List[Dict]) -> str:
    """Готовый промпт для coding agent (автономный arb/sniper agent)."""
    lines = [
        "# Multi-Chain Arbitrage Opportunity → Coding Agent Prompt 2026",
        "Ты — Autonomous Solana + L2 Arbitrage Agent.",
        "Используй данные ниже для создания/улучшения бота, который ловит кросс-чейн и intra-DEX спреды.\n"
    ]

    for opp in sorted(opportunities, key=lambda x: x.get("arb_score", 0), reverse=True)[:8]:
        if opp.get("arb_score", 0) < 60:
            continue
        lines.append(f"## OPPORTUNITY: {opp.get('symbol')} | Score: {opp.get('arb_score')}")
        lines.append(f"Spread: {opp.get('spread_pct', 0):.2f}% | Cross-chain: {opp.get('cross_chain_listed')}")
        lines.append("Execution plan:")
        lines.append("1. Monitor Solana DEXes via Jupiter + Helius WS")
        lines.append("2. При спреде > 0.8% и cross-chain listing → подготовить Jito bundle")
        lines.append("3. Для L2 (Base/Arbitrum) использовать bridge (LayerZero / Across) если спред оправдывает газ")
        lines.append("4. Применять evidence-weighted sizing из основной системы")
        lines.append("5. Exit: trailing stop + MFE capture\n")

    lines.append("\nЗадача: Напиши полный код агента с:")
    lines.append("- Rate limiting + caching")
    lines.append("- Jito bundle integration")
    lines.append("- Integration с Moon Score (PR-3) и GitHub Velocity (PR-3)")
    lines.append("- Logging всех решений с provenance")

    return "\n".join(lines)