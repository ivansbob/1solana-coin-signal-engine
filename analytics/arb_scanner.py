import asyncio
import httpx
import json
import os
from typing import List, Dict, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Constants
DEXSCREENER_BASE = "https://api.dexscreener.com"
DEX_FEES = {
    "raydium": 0.0025,  # 0.25%
    "orca": 0.003,      # 0.3%
    "jupiter": 0.0,     # aggregator, assume 0 for now
    "meteora": 0.0004,  # 0.04% min
    "pump": 0.0         # assume 0
}

@dataclass
class ArbOpportunity:
    token_mint: str
    symbol: str
    buy_dex: str
    buy_price: float
    sell_dex: str
    sell_price: float
    gross_spread_pct: float
    estimated_fees_pct: float
    net_spread_pct: float
    liquidity_buy_side: float
    liquidity_sell_side: float
    max_position_sol: float
    viable: bool
    flash_loan_viable: bool
    mev_risk: str

async def fetch_dex_price(token_mint: str, dex: str) -> Dict[str, Any]:
    """Fetch price and liquidity for token on specific DEX via DexScreener"""
    url = f"{DEXSCREENER_BASE}/latest/dex/search?q={token_mint}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            pairs = data.get("pairs", [])

            # Find pair on specific DEX
            for pair in pairs:
                if pair.get("dexId") == dex and pair.get("chainId") == "solana":
                    price = pair.get("priceUsd", 0)
                    liquidity = pair.get("liquidity", {}).get("usd", 0)
                    return {"price": float(price), "liquidity": float(liquidity), "dex": dex}
    except Exception as e:
        logger.error(f"Price fetch failed for {token_mint} on {dex}: {e}")
    return {"price": 0, "liquidity": 0, "dex": dex}

async def scan_arb_opportunities(token_mints: List[str]) -> List[ArbOpportunity]:
    """
    Scan for arbitrage opportunities across DEXes on Solana.
    """
    opportunities = []

    for token_mint in token_mints:
        # Fetch prices from all DEXes in parallel
        dexes = list(DEX_FEES.keys())
        tasks = [fetch_dex_price(token_mint, dex) for dex in dexes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        prices = {}
        for dex, result in zip(dexes, results):
            if isinstance(result, dict) and result["price"] > 0:
                prices[dex] = result

        if len(prices) < 2:
            continue  # Need at least 2 prices for arb

        # Find best buy/sell
        sorted_prices = sorted(prices.items(), key=lambda x: x[1]["price"])
        buy_dex, buy_data = sorted_prices[0]
        sell_dex, sell_data = sorted_prices[-1]

        buy_price = buy_data["price"]
        sell_price = sell_data["price"]
        gross_spread = (sell_price - buy_price) / buy_price * 100

        # Estimate fees (simplified: 2 swaps)
        buy_fee = DEX_FEES.get(buy_dex, 0.01)
        sell_fee = DEX_FEES.get(sell_dex, 0.01)
        total_fees_pct = (buy_fee + sell_fee) * 100
        net_spread = gross_spread - total_fees_pct

        if net_spread <= 0:
            continue

        # Liquidity
        buy_liq = buy_data["liquidity"]
        sell_liq = sell_data["liquidity"]

        # Max position (0.5% of smaller liquidity)
        min_liq = min(buy_liq, sell_liq)
        max_position_usd = min_liq * 0.005
        max_position_sol = max_position_usd / sell_price  # approx

        viable = net_spread > 0.1 and max_position_sol > 0.1
        flash_loan_viable = max_position_sol > 5

        # MEV risk
        if net_spread > 1.0:
            mev_risk = "high"
        elif net_spread > 0.5:
            mev_risk = "medium"
        else:
            mev_risk = "low"

        # Get symbol (simplified, assume from buy_data or something)
        symbol = "UNKNOWN"  # In real, fetch from pool data

        opp = ArbOpportunity(
            token_mint=token_mint,
            symbol=symbol,
            buy_dex=buy_dex,
            buy_price=buy_price,
            sell_dex=sell_dex,
            sell_price=sell_price,
            gross_spread_pct=round(gross_spread, 2),
            estimated_fees_pct=round(total_fees_pct, 2),
            net_spread_pct=round(net_spread, 2),
            liquidity_buy_side=buy_liq,
            liquidity_sell_side=sell_liq,
            max_position_sol=round(max_position_sol, 2),
            viable=viable,
            flash_loan_viable=flash_loan_viable,
            mev_risk=mev_risk
        )
        opportunities.append(opp)

    logger.info(f"Found {len(opportunities)} arb opportunities")
    return opportunities

def save_arb_opportunities(opps: List[ArbOpportunity], filename: str = "data/arb_opportunities.json"):
    """Save arb opportunities to JSON"""
    os.makedirs("data", exist_ok=True)
    data = [opp.__dict__ for opp in opps]
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved {len(opps)} arb opportunities to {filename}")

if __name__ == "__main__":
    # Test with sample token
    asyncio.run(scan_arb_opportunities(["So11111111111111111111111111111111111111112"]))