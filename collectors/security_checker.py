# collectors/security_checker.py
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List

from utils.cache import cache_get, cache_set
from utils.rate_limit import acquire
from utils.retry import with_retry
from utils.clock import utc_now_iso

# Импортируем существующие модули из твоего репо
from collectors.honeypot_simulator import check_honeypot  # или как у тебя называется
from collectors.rug_engine import assess_rug_risk
from collectors.authority_checks import check_solana_authorities
from collectors.dev_risk_checks import check_dev_risk


class SecurityChecker:
    def __init__(self):
        self.cache_ttl_sec = 1800  # 30 минут

    async def check_token(self, token_address: str, chain: str = "solana") -> Dict[str, Any]:
        cache_key = f"security_{chain}_{token_address}"
        cached = cache_get("dex", cache_key)
        if cached:
            return cached

        acquire("dex")

        # 1. Honeypot check (Teycir-style)
        honeypot_result = await check_honeypot(token_address, chain)

        # 2. Rug assessment (RugWatch-style)
        rug_result = assess_rug_risk(token_address, chain)

        # 3. Solana-specific authority checks
        authority_result = {}
        if chain == "solana":
            authority_result = await check_solana_authorities(token_address)

        # 4. Dev risk
        dev_result = check_dev_risk(token_address)

        # === Объединяем в единый вердикт ===
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

        result = {
            "token_address": token_address,
            "honeypot": honeypot,
            "rug_score": final_rug_score,
            "risk_level": risk_level,
            "verdict": verdict,
            "reasons": self._build_reasons(honeypot_result, rug_result, authority_result, dev_result),
            "solana_specific": authority_result,
            "checked_at": utc_now_iso(),
        }

        cache_set("dex", cache_key, result, ttl_sec=self.cache_ttl_sec)
        return result

    def _build_reasons(self, honeypot_res, rug_res, authority_res, dev_res) -> List[str]:
        reasons = []
        if honeypot_res.get("is_honeypot"):
            reasons.append("honeypot_patterns_detected")
        if rug_res.get("lp_locked") is False:
            reasons.append("liquidity_not_locked")
        if authority_res.get("freeze_authority_active"):
            reasons.append("freeze_authority_active")
        if authority_res.get("mint_authority_active"):
            reasons.append("mint_authority_active")
        if dev_res.get("dev_sell_pressure_high"):
            reasons.append("high_dev_sell_pressure")
        return reasons[:6]  # не больше 6 причин

    async def check_batch(self, token_addresses: List[str], chain: str = "solana") -> List[Dict[str, Any]]:
        tasks = [self.check_token(addr, chain) for addr in token_addresses]
        return await asyncio.gather(*tasks)

    def build_security_text_section(self, results: List[Dict[str, Any]]) -> str:
        lines = ["=== SECURITY CHECK (Honeypot + Rug + Authority) ==="]
        for r in results:
            status = "✅ PASS" if r["verdict"] == "PASS" else "⚠️ WARN" if r["verdict"] == "WARN" else "❌ BLOCK"
            lines.append(
                f"{status} {r['token_address'][:8]}... | "
                f"Rug: {r['rug_score']:.1f} | "
                f"Honeypot: {'YES' if r['honeypot'] else 'no'} | "
                f"Risk: {r['risk_level']}"
            )
        return "\n".join(lines) + "\n"
