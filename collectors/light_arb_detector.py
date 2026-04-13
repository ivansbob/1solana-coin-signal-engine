# collectors/light_arb_detector.py
import asyncio
from typing import List, Dict, Any

from utils.cache import cache_get, cache_set
from utils.rate_limit import acquire
from utils.retry import with_retry
from utils.clock import utc_now_iso

from collectors.dexscreener_client import DexScreenerClient


class LightArbDetector:
    def __init__(self):
        self.dex_client = DexScreenerClient()
        self.cache_ttl_sec = 600  # 10 минут (арбитраж живёт недолго)

    @with_retry
    async def _get_dex_prices(self, token_address: str) -> Dict[str, float]:
        """Получаем цену на основных DEX через DexScreener"""
        await acquire("dex")
        
        pairs = await self.dex_client.get_pairs_by_token(token_address, limit=10)
        
        prices = {}
        for pair in pairs:
            dex_name = str(pair.get("dexId", "")).lower()
            price_usd = float(pair.get("priceUsd") or pair.get("price_usd") or 0)
            if price_usd > 0 and dex_name in {"raydium", "orca", "jupiter", "meteora", "pump"}:
                prices[dex_name] = price_usd
        
        return prices

    def _calculate_spread(self, prices: Dict[str, float]) -> Dict[str, Any]:
        if len(prices) < 2:
            return {"cross_dex_spread_pct": 0.0, "arb_spread_score": 0.0}
        
        sorted_prices = sorted(prices.values())
        min_price = sorted_prices[0]
        max_price = sorted_prices[-1]
        
        spread_pct = round(((max_price - min_price) / min_price) * 100, 3)
        
        # Простая оценка 0–10
        score = 0
        if spread_pct > 2.0:
            score = 10
        elif spread_pct > 1.0:
            score = 7
        elif spread_pct > 0.5:
            score = 4
        elif spread_pct > 0.3:
            score = 2
        
        return {
            "cross_dex_spread_pct": spread_pct,
            "arb_spread_score": score,
            "arb_opportunity": score >= 4,
            "price_range": f"{min_price:.6f} – {max_price:.6f} USD",
            "dex_count": len(prices)
        }

    async def enrich_with_arb_data(self, pools: List[Dict]) -> List[Dict]:
        """Основная функция — обогащает каждый пул арбитражными данными"""
        cache_key = f"arb_enrich_{len(pools)}"
        cached = cache_get("dex", cache_key)
        if cached:
            return cached

        enriched = []
        for pool in pools:
            token_address = pool.get("token_address") or pool.get("address") or pool.get("base_token", {}).get("address")
            if not token_address:
                enriched.append(pool)
                continue

            prices = await self._get_dex_prices(token_address)
            arb_data = self._calculate_spread(prices)

            pool.update(arb_data)
            enriched.append(pool)

        cache_set("dex", cache_key, enriched, ttl_sec=self.cache_ttl_sec)
        return enriched

    def build_arb_text_section(self, pools: List[Dict]) -> str:
        """Красивый блок для daily_aggregate.txt"""
        lines = ["=== LIGHT ARBITRAGE OPPORTUNITIES (cross-DEX spread) ==="]
        for p in pools:
            if p.get("arb_spread_score", 0) >= 2:
                lines.append(
                    f"Token: {p.get('symbol','?')} | "
                    f"Spread: {p.get('cross_dex_spread_pct',0):.2f}% | "
                    f"Score: {p.get('arb_spread_score',0)} | "
                    f"DEXs: {p.get('dex_count',0)} | "
                    f"Opportunity: {'YES' if p.get('arb_opportunity') else 'no'}"
                )
        return "\n".join(lines) + "\n" if len(lines) > 1 else "=== NO ARBITRAGE SIGNALS ===\n"