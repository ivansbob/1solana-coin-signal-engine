# collectors/onchain_liquidity_collector.py
import asyncio
from datetime import datetime
from typing import List, Dict, Any

from utils.cache import cache_get, cache_set
from utils.rate_limit import async_acquire, acquire
from utils.retry import with_retry
from utils.clock import utc_now_iso

# Импортируем существующие коллекторы (они уже в твоём репо)
from collectors.new_pools import get_new_pools
from collectors.dexscreener_client import DexScreenerClient
from collectors.cross_chain_collector import get_cross_chain_pools
from collectors.raydium_pool_collector import get_raydium_pools  # если есть


class OnchainLiquidityCollector:
    def __init__(self):
        self.dex_client = DexScreenerClient()

    async def _calculate_metrics(self, pool: Dict) -> Dict:
        """Добавляем все метрики 2026 года"""
        liq = float(pool.get("liquidity_usd") or pool.get("liquidity") or 0)
        vol_1h = float(pool.get("volume_1h") or pool.get("volume_h1") or 0)
        age_min = int(pool.get("age_minutes") or pool.get("pool_age") or 9999)

        liquidity_velocity_1h = round((vol_1h / liq * 100), 2) if liq > 5000 else 0.0
        volume_to_liq_ratio = round(vol_1h / liq, 2) if liq > 0 else 0.0

        return {
            **pool,
            "liquidity_velocity_1h": liquidity_velocity_1h,
            "volume_to_liq_ratio": volume_to_liq_ratio,
            "age_of_pool_minutes": age_min,
            "fast_rise_flag": liquidity_velocity_1h > 70 and age_min < 120,
            "mev_resistance": age_min > 5,
        }

    async def get_onchain_liquidity_candidates(
        self, max_candidates: int = 25
    ) -> List[Dict]:
        cache_key = "onchain_liquidity_candidates"
        cached = cache_get("dex", cache_key)
        if cached:
            return cached

        await async_acquire("dex")

        # 1. Solana новые пулы (твои существующие коллекторы)
        solana_pools = await get_new_pools() or []
        raydium_pools = (
            await get_raydium_pools() if hasattr(get_raydium_pools, "__call__") else []
        )

        # 2. DexScreener trending / new
        dexscreener_pools = await self.dex_client.get_trending_pairs(limit=15)

        # 3. Cross-chain
        cross_pools = await get_cross_chain_pools()

        all_pools = solana_pools + raydium_pools + dexscreener_pools + cross_pools

        # 4. Обогащаем метриками
        enriched = []
        for pool in all_pools[: max_candidates * 2]:
            enriched_pool = await self._calculate_metrics(pool)
            # Cross-chain delta (если есть данные по нескольким цепям)
            if "chain" in enriched_pool and enriched_pool.get("price_usd"):
                enriched_pool["cross_chain_delta"] = (
                    await self._calculate_cross_chain_delta(enriched_pool)
                )
            enriched.append(enriched_pool)

        # Сортируем по силе сигнала
        enriched.sort(
            key=lambda p: (
                p.get("liquidity_velocity_1h", 0),
                p.get("volume_to_liq_ratio", 0),
                -p.get("age_of_pool_minutes", 9999),
            ),
            reverse=True,
        )

        result = enriched[:max_candidates]

        cache_set("dex", cache_key, result, ttl_sec=900)  # 15 минут
        return result

    async def _calculate_cross_chain_delta(self, pool: Dict) -> float:
        """Простой cross-chain delta (Solana vs Base/Arbitrum)"""
        # Здесь можно расширить позже через CoinGecko price compare
        return 0.0  # заглушка — потом реализуем реальный delta

    def build_liquidity_text_section(self, candidates: List[Dict]) -> str:
        """Красивый блок для daily_aggregate.txt"""
        lines = ["=== ON-CHAIN LIQUIDITY & FAST FLOW (last 24h) ==="]
        for p in candidates:
            lines.append(
                f"Token: {p.get('symbol','?')} | "
                f"Liquidity: ${p.get('liquidity_usd',0):,.0f} | "
                f"Velocity 1h: {p.get('liquidity_velocity_1h',0):.1f}% | "
                f"Age: {p.get('age_of_pool_minutes',0)} мин | "
                f"Vol/Liq: {p.get('volume_to_liq_ratio',0):.2f}x | "
                f"Fast Rise: {'YES' if p.get('fast_rise_flag') else 'no'}"
            )
        return "\n".join(lines) + "\n"
