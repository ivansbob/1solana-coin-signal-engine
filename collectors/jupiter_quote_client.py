from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import asyncio
import httpx
from datetime import datetime, timezone

from utils.cache import cache_get, cache_set


@dataclass
class ArbQuoteResult:
    quote1: dict
    quote2: dict
    amount_in: int            # lamports
    amount_out: int           # quote2["outAmount"] as int
    profit_lamports: int      # amount_out - amount_in
    profit_pct: float
    route_label: str          # "Raydium -> Orca -> Meteora"
    is_profitable: bool
    price_impact_pct: float   # max from both quotes


class JupiterQuoteClient:
    BASE_URL = "https://quote-api.jup.ag/v6/quote"
    LITE_URL = "https://lite-api.jup.ag/swap/v1/quote"

    def __init__(self, timeout_sec: int = 5):
        self.timeout_sec = timeout_sec

    def _build_route_label(self, route_plan: List[Dict[str, Any]]) -> str:
        """Build route label from routePlan[].swapInfo.label"""
        labels = []
        for route in route_plan:
            swap_info = route.get("swapInfo")
            if isinstance(swap_info, list):
                # Handle list of swapInfo
                for info in swap_info:
                    label = info.get("label", "")
                    if label:
                        labels.append(label)
            elif isinstance(swap_info, dict):
                # Handle single swapInfo dict
                label = swap_info.get("label", "")
                if label:
                    labels.append(label)
        return " -> ".join(labels) if labels else "Unknown"

    async def get_arb_quotes(
        self, base_mint: str, quote_mint: str, amount_in: int, slippage_bps: int = 0, use_lite: bool = False
    ) -> ArbQuoteResult:
        """
        Get arbitrage quotes: quote1 (WSOL -> TOKEN) and quote2 (TOKEN -> WSOL)
        """
        # Check cache first
        cache_key = f"jup_arb_{base_mint}_{quote_mint}_{amount_in}_{slippage_bps}"
        cached_result = cache_get("dex", cache_key)
        if cached_result:
            # Convert cached dict back to ArbQuoteResult
            return ArbQuoteResult(**cached_result)

        wsol_mint = "So11111111111111111111111111111111111111112"

        # Quote 1: WSOL -> TOKEN
        params1 = {
            "inputMint": wsol_mint,
            "outputMint": base_mint,  # This is the token we want to arbitrage
            "amount": amount_in,
            "slippageBps": slippage_bps,
        }

        url = self.LITE_URL if use_lite else self.BASE_URL

        async with httpx.AsyncClient(timeout=self.timeout_sec) as client:
            try:
                # First quote: WSOL -> TOKEN
                resp1 = await client.get(url, params=params1)
                if resp1.status_code != 200:
                    raise Exception(f"Jupiter API error for quote1: {resp1.status_code} - {resp1.text}")
                quote1 = resp1.json()
                if "error" in quote1:
                    raise Exception(f"Jupiter quote1 error: {quote1['error']}")

                # Get outAmount from quote1 for quote2
                quote1_out_amount = int(quote1["outAmount"])

                # Quote 2: TOKEN -> WSOL
                params2 = {
                    "inputMint": base_mint,
                    "outputMint": wsol_mint,
                    "amount": quote1_out_amount,
                    "slippageBps": slippage_bps,
                }

                resp2 = await client.get(url, params=params2)
                if resp2.status_code != 200:
                    raise Exception(f"Jupiter API error for quote2: {resp2.status_code} - {resp2.text}")
                quote2 = resp2.json()
                if "error" in quote2:
                    raise Exception(f"Jupiter quote2 error: {quote2['error']}")

                # Calculate profit
                amount_out = int(quote2["outAmount"])
                profit_lamports = amount_out - amount_in
                profit_pct = (profit_lamports / amount_in) * 100 if amount_in > 0 else 0

                # Build route labels
                route_label1 = self._build_route_label(quote1.get("routePlan", []))
                route_label2 = self._build_route_label(quote2.get("routePlan", []))
                combined_route_label = f"{route_label1} -> {route_label2}"

                # Price impact (max from both quotes)
                price_impact_pct = max(
                    float(quote1.get("priceImpactPct", 0)),
                    float(quote2.get("priceImpactPct", 0))
                )

                result = ArbQuoteResult(
                    quote1=quote1,
                    quote2=quote2,
                    amount_in=amount_in,
                    amount_out=amount_out,
                    profit_lamports=profit_lamports,
                    profit_pct=profit_pct,
                    route_label=combined_route_label,
                    is_profitable=profit_lamports > 0,
                    price_impact_pct=price_impact_pct
                )

                # Cache the result
                cache_set("dex", cache_key, result.__dict__, ttl_sec=30)

                return result

            except Exception as e:
                raise Exception(f"Failed to get Jupiter quotes: {str(e)}")