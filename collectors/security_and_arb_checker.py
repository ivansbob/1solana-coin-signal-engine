# collectors/security_and_arb_checker.py
import asyncio
from typing import Dict, Any, List

from utils.cache import cache_get, cache_set
from utils.rate_limit import async_acquire
from utils.retry import async_with_retry
from utils.clock import utc_now_iso

# Импортируем всё, что уже есть в твоём репо
from collectors.honeypot_simulator import check_honeypot
from collectors.rug_engine import assess_rug_risk
from collectors.authority_checks import check_solana_authorities
from collectors.dev_risk_checks import check_dev_risk
from collectors.dexscreener_client import DexScreenerClient


class SecurityAndArbChecker:
    def __init__(self):
        self.dex_client = DexScreenerClient()
        self.cache_ttl_sec = 900  # 15 минут

    @async_with_retry
    async def check_token(self, token_address: str, chain: str = "solana") -> Dict[str, Any]:
        cache_key = f"sec_arb_{chain}_{token_address}"
        cached = cache_get("dex", cache_key)
        if cached:
            return cached

        await async_acquire("dex")

        # === 1. SECURITY BLOCK ===
        honeypot_result = await check_honeypot(token_address, chain)
        rug_result = assess_rug_risk(token_address, chain)
        authority_result = await check_solana_authorities(token_address) if chain == "solana" else {}
        dev_result = check_dev_risk(token_address)

        honeypot = honeypot_result.get("is_honeypot", False)
        rug_score = float(rug_result.get("rug_score", 0.0))
        authority_risk = authority_result.get("risk_score", 0.0)

        final_rug_score = round((rug_score + authority_risk + (10.0 if honeypot else 0.0)) / 3, 2)

        if final_rug_score >= 7.5 or honeypot:
            risk_level = "HIGH"
            verdict = "BLOCK"
        elif final_rug_score >= 4.0:
            risk_level = "MEDIUM"
            verdict = "WARN"
        else:
            risk_level = "LOW"
            verdict = "PASS"

        security = {
            "honeypot": honeypot,
            "rug_score": final_rug_score,
            "risk_level": risk_level,
            "verdict": verdict,
            "reasons": self._build_security_reasons(honeypot_result, rug_result, authority_result, dev_result),
            "solana_specific": authority_result,
        }

        # === 2. LIGHT ARBITRAGE BLOCK ===
        prices = await self._get_dex_prices(token_address)
        arb_data = self._calculate_spread(prices)

        result = {
            "token_address": token_address,
            **security,
            **arb_data,
            "checked_at": utc_now_iso(),
        }

        cache_set("dex", cache_key, result, ttl_sec=self.cache_ttl_sec)
        return result

    async def _get_dex_prices(self, token_address: str) -> Dict[str, float]:
        pairs = await self.dex_client.get_pairs_by_token(token_address, limit=8)
        prices = {}
        for p in pairs:
            dex = str(p.get("dexId", "")).lower()
            price = float(p.get("priceUsd") or p.get("price_usd") or 0)
            if price > 0 and dex in {"raydium", "orca", "jupiter", "meteora", "pump"}:
                prices[dex] = price
        return prices

    def _calculate_spread(self, prices: Dict[str, float]) -> Dict[str, Any]:
        if len(prices) < 2:
            return {"cross_dex_spread_pct": 0.0, "arb_spread_score": 0, "arb_opportunity": False}
        sorted_p = sorted(prices.values())
        spread_pct = round(((sorted_p[-1] - sorted_p[0]) / sorted_p[0]) * 100, 3)
        score = 10 if spread_pct > 2.0 else 7 if spread_pct > 1.0 else 4 if spread_pct > 0.5 else 2 if spread_pct > 0.3 else 0
        return {
            "cross_dex_spread_pct": spread_pct,
            "arb_spread_score": score,
            "arb_opportunity": score >= 4,
            "dex_count": len(prices)
        }

    def _build_security_reasons(self, *results) -> List[str]:
        reasons = []
        for r in results:
            if isinstance(r, dict):
                if r.get("is_honeypot"):
                    reasons.append("honeypot")
                if r.get("lp_locked") is False:
                    reasons.append("lp_not_locked")
                if r.get("freeze_authority_active"):
                    reasons.append("freeze_active")
        return reasons[:6]

    async def check_batch(self, addresses: List[str], chain: str = "solana") -> List[Dict[str, Any]]:
        tasks = [self.check_token(addr, chain) for addr in addresses]
        return await asyncio.gather(*tasks)

    def build_combined_text_section(self, results: List[Dict[str, Any]]) -> str:
        lines = ["=== SECURITY + LIGHT ARB CHECK ==="]
        for r in results:
            sec = "✅" if r["verdict"] == "PASS" else "⚠️" if r["verdict"] == "WARN" else "❌"
            arb = f"ARB:{r.get('arb_spread_score',0)}" if r.get("arb_opportunity") else ""
            lines.append(
                f"{sec} {r['token_address'][:8]}... | "
                f"Rug:{r['rug_score']:.1f} | Honeypot:{'YES' if r['honeypot'] else 'no'} | "
                f"Spread:{r.get('cross_dex_spread_pct',0):.2f}% {arb}"
            )
        return "\n".join(lines) + "\n"