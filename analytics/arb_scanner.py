import asyncio
import httpx
from typing import Dict, Any, List
from datetime import datetime, timezone
from pathlib import Path

from utils.cache import cache_get, cache_set
from utils.rate_limit import acquire
from utils.io import write_json
from utils.logger import log_info, log_warning
from config.settings import Settings

COINGECKO_SEARCH_URL = "https://api.coingecko.com/api/v3/search"
COINGECKO_COIN_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}"

TARGET_CHAINS = ["base", "arbitrum", "ethereum", "solana"]

async def fetch_coingecko_crosschain(symbol: str, cache_ttl: int = 300) -> Dict[str, Any]:
    cache_key = f"cg_search_{symbol.lower()}"
    cached = cache_get("dex", cache_key)
    if cached:
        return cached
    acquire("dex")
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(COINGECKO_SEARCH_URL, params={"query": symbol})
            if resp.status_code != 200:
                return {"found": False, "error": "http_error"}
            data = resp.json()
            for coin in data.get("coins", []):
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

def calculate_arb_opportunity_score(opp: Dict[str, Any], cross_chain: Dict[str, Any]) -> Dict[str, Any]:
    score = 0
    reasons = []
    sol_spread = float(opp.get("spread_pct", 0))
    if sol_spread > 1.5:
        score += 35
        reasons.append(f"Solana DEX spread {sol_spread:.2f}%")
    elif sol_spread > 0.8:
        score += 18
        reasons.append(f"Moderate Solana spread {sol_spread:.2f}%")
    if cross_chain.get("found"):
        score += 25
        reasons.append(f"Listed on CEX/L2: {cross_chain.get('name')}")
    volume = float(opp.get("volume_24h", 0))
    if volume > 500_000:
        score += 20
        reasons.append("High 24h volume")
    elif volume > 100_000:
        score += 10
    if float(opp.get("liquidity_usd", 0)) < 50_000:
        score -= 15
        reasons.append("Low liquidity warning")
    final_score = round(max(0, min(100, score)), 1)
    opp.update({
        "arb_score": final_score,
        "arb_reasons": reasons,
        "cross_chain_listed": cross_chain.get("found", False),
        "cross_chain_name": cross_chain.get("name"),
        "flash_loan_ready": final_score >= 60,
        "action": "ARB" if final_score >= 60 else "WATCH",
        "scored_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "provenance": "multi_chain_arb_scanner_2026"
    })
    return opp

async def scan_arb_opportunities() -> List[Dict[str, Any]]:
    """Основной сканер арбитража (Solana DEX + Cross-chain)"""
    opportunities = []  # ← Здесь будет твой реальный Solana DEX scan (Jupiter, Raydium и т.д.)
    # Пример: opportunities = await scan_solana_dex_spreads()  # замени на свой код

    enriched = []
    for opp in opportunities[:50]:
        symbol = opp.get("symbol", "").upper()
        cross_chain = await fetch_coingecko_crosschain(symbol)
        scored = calculate_arb_opportunity_score(opp, cross_chain)
        enriched.append(scored)
        if scored["arb_score"] >= 65:
            log_info("high_arb_opportunity", symbol=symbol, score=scored["arb_score"], cross_chain=cross_chain.get("found"))
    write_json(Path("data/processed/arb_opportunities.json"), {
        "as_of": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "opportunities": sorted(enriched, key=lambda x: x.get("arb_score", 0), reverse=True)
    })
    return enriched

def from_jupiter_opportunities(opportunities: List) -> List[Dict]:
    """
    Convert ArbOpportunity objects to format compatible with generate_arb_coding_agent_prompt()
    """
    result = []
    for opp in opportunities:
        # Convert ArbOpportunity dataclass to dict
        if hasattr(opp, '__dict__'):
            opp_dict = vars(opp)
        else:
            opp_dict = opp

        # Map fields to expected format
        converted = {
            "symbol": opp_dict.get("symbol", "UNKNOWN"),
            "token_address": opp_dict.get("token_address", ""),
            "arb_score": opp_dict.get("arb_score", 0),
            "spread_pct": opp_dict.get("profit_pct", 0),  # Map profit_pct to spread_pct
            "cross_chain_listed": False,  # Jupiter arb is on-chain only
            "route_label": opp_dict.get("route_label", ""),
            "price_impact_pct": opp_dict.get("price_impact_pct", 0),
            "profit_lamports": opp_dict.get("profit_lamports", 0),
            "flash_loan_ready": opp_dict.get("arb_score", 0) >= 60,
            "action": "ARB" if opp_dict.get("arb_score", 0) >= 60 else "WATCH",
            "scanned_at": opp_dict.get("scanned_at", ""),
            "provenance": "jupiter_arb_scanner_2026"
        }
        result.append(converted)
    return result


def generate_arb_coding_agent_prompt(opportunities: List[Dict]) -> str:
    """Промпт для coding-агента (автономный arb/sniper)"""
    lines = ["# Multi-Chain Arbitrage Opportunity → Coding Agent Prompt 2026",
              "Ты — Autonomous Solana + L2 Arbitrage Agent.\n"]
    for opp in sorted(opportunities, key=lambda x: x.get("arb_score", 0), reverse=True)[:8]:
        if opp.get("arb_score", 0) < 60:
            continue
        lines.append(f"## OPPORTUNITY: {opp.get('symbol')} | Score: {opp.get('arb_score')}")
        lines.append(f"Spread: {opp.get('spread_pct', 0):.2f}% | Cross-chain: {opp.get('cross_chain_listed')}")
        lines.append("Execution: flash-loan + Jupiter swap + repay\n")
    lines.append("\nЗадача: Напиши полный код агента с Jito bundle, rate limiting, Moon Score и provenance.")
    return "\n".join(lines)