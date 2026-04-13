import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
import logging

from utils.cache import SimpleTTLCache
from utils.rate_limit import acquire
from utils.retry import with_retry
from utils.clock import utc_now_iso
from utils.io import write_json

# Import existing collectors
from .github_signal import get_github_candidates, build_github_text_section
from .new_pools import get_new_pools
from .cross_chain_collector import get_cross_chain_pools
from .security_checker import SecurityChecker

logger = logging.getLogger(__name__)


class FreeDiscoveryAggregator:
    """Free discovery aggregator for level 1-2 signals"""

    def __init__(self, max_candidates: int = 25):
        self.max_candidates = max_candidates
        self.cache = SimpleTTLCache(ttl=900)  # 15 min cache

    async def collect_all(self) -> Dict[str, Any]:
        """
        Collect data from all free sources:
        - github_repos: GitHub dev activity
        - new_pools: New pools from DexScreener (Solana)
        - cross_chain_pools: Cross-chain opportunities
        - security_results: Basic security checks
        """
        logger.info("Starting free discovery collection...")

        # Collect in parallel
        tasks = [
            self._collect_github_activity(),
            self._collect_new_pools(),
            self._collect_cross_chain_pools(),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        github_data = results[0] if not isinstance(results[0], Exception) else []
        new_pools_data = results[1] if not isinstance(results[1], Exception) else []
        cross_chain_data = results[2] if not isinstance(results[2], Exception) else []

        # Basic security checks for top candidates
        security_data = await self._collect_security_checks(
            new_pools_data[: self.max_candidates]
        )

        collected_data = {
            "timestamp": utc_now_iso(),
            "github_repos": github_data,
            "new_pools": new_pools_data[: self.max_candidates],
            "cross_chain_pools": cross_chain_data[: self.max_candidates],
            "security_results": security_data,
        }

        logger.info(
            f"Collection complete: {len(new_pools_data)} new pools, {len(cross_chain_data)} cross-chain, {len(security_data)} security checks"
        )
        return collected_data

    async def _collect_github_activity(self) -> List[Dict[str, Any]]:
        """Collect new GitHub repos with dev activity scores"""
        try:
            candidates = await get_github_candidates()
            # Return the candidates directly, already sorted by pushed_at desc
            return candidates
        except Exception as e:
            logger.error(f"GitHub collection failed: {e}")
            return []

    async def _collect_new_pools(self) -> List[Dict[str, Any]]:
        """Collect new pools from free sources"""
        try:
            # Get from DexScreener (Solana) - main free source
            solana_pools = await get_new_pools()

            # Deduplicate by token_address
            seen = set()
            unique_pools = []
            for pool in solana_pools:
                token_addr = pool.get("token_address", "")
                if token_addr and token_addr not in seen:
                    seen.add(token_addr)
                    unique_pools.append(pool)

            # Sort by liquidity and recency
            unique_pools.sort(
                key=lambda x: (x.get("liquidity_usd", 0), -x.get("age_minutes", 0)),
                reverse=True,
            )
            return unique_pools

        except Exception as e:
            logger.error(f"New pools collection failed: {e}")
            return []

    async def _collect_cross_chain_pools(self) -> List[Dict[str, Any]]:
        """Collect cross-chain opportunities"""
        try:
            return await get_cross_chain_pools(min_liquidity=5000)
        except Exception as e:
            logger.error(f"Cross-chain collection failed: {e}")
            return []

    async def _collect_security_checks(
        self, pools: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Basic security checks for top pools"""
        security_checker = SecurityChecker()
        token_addresses = [pool.get("token_address", "") for pool in pools if pool.get("token_address")]

        if not token_addresses:
            return []

        try:
            security_results = await security_checker.check_batch(token_addresses, chain="solana")
            return security_results
        except Exception as e:
            logger.warning(f"Batch security check failed: {e}")
            # Return error results for all tokens
            return [
                {
                    "token_address": addr,
                    "symbol": next((pool.get("symbol", "UNKNOWN") for pool in pools if pool.get("token_address") == addr), "UNKNOWN"),
                    "honeypot": False,
                    "rug_score": 10.0,
                    "risk_level": "HIGH",
                    "verdict": "BLOCK",
                    "reasons": [f"Security check error: {str(e)[:50]}"],
                    "solana_specific": {},
                    "checked_at": utc_now_iso(),
                }
                for addr in token_addresses
            ]

    def build_aggregate_text(self, collected_data: Dict[str, Any]) -> str:
        """Build the beautiful daily aggregate text"""
        timestamp = collected_data.get("timestamp", utc_now_iso())
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        lines = []
        lines.append(
            f"=== WEB3 FREE ARBITRAGE SCAN {dt.strftime('%Y-%m-%d %H:%M')} ==="
        )
        lines.append("")

        # GitHub Repos
        github_text = build_github_text_section(collected_data.get("github_repos", []))
        lines.append(github_text)
        lines.append("")

        # Fast Liquidity Flow
        lines.append("💰 FAST LIQUIDITY FLOW (SOLANA)")
        lines.append("-" * 50)
        new_pools = collected_data.get("new_pools", [])
        if new_pools:
            for pool in new_pools[:15]:  # Top 15
                symbol = pool.get("symbol", "UNKNOWN")[:8]
                liquidity = pool.get("liquidity_usd", 0)
                volume_h1 = pool.get("volume_h1", 0)
                age_min = pool.get("age_minutes", 0)
                price_change = pool.get("price_change_h1", 0)

                liq_str = (
                    f"${liquidity:,.0f}" if liquidity > 1000 else f"${liquidity:.0f}"
                )
                vol_str = (
                    f"${volume_h1:,.0f}" if volume_h1 > 1000 else f"${volume_h1:.0f}"
                )
                age_str = f"{age_min}m" if age_min < 60 else f"{age_min//60}h"

                lines.append(
                    f"• {symbol:<8} | 💧 {liq_str:<8} | 📈 {vol_str:<8} | 🕒 {age_str:<4} | {price_change:+.1f}%"
                )
        else:
            lines.append("No new liquidity flows detected")
        lines.append("")

        # Cross-chain Opportunities
        lines.append("🌐 CROSS-CHAIN OPPORTUNITIES")
        lines.append("-" * 50)
        cross_chain = collected_data.get("cross_chain_pools", [])
        if cross_chain:
            for pool in cross_chain[:10]:  # Top 10
                symbol = pool.get("attributes", {}).get("name", "UNKNOWN")[:8]
                chain = (
                    pool.get("relationships", {})
                    .get("dex", {})
                    .get("data", {})
                    .get("id", "unknown")
                )
                liquidity = pool.get("attributes", {}).get("reserve_in_usd", "0")
                try:
                    liq_float = float(liquidity)
                    liq_str = (
                        f"${liq_float:,.0f}"
                        if liq_float > 1000
                        else f"${liq_float:.0f}"
                    )
                except:
                    liq_str = liquidity

                lines.append(f"• {symbol:<8} | {chain:<10} | 💧 {liq_str}")
        else:
            lines.append("No cross-chain opportunities detected")
        lines.append("")

        # Security Summary
        lines.append("🛡️  SECURITY SUMMARY")
        lines.append("-" * 50)
        security_results = collected_data.get("security_results", [])
        if security_results:
            safe_count = sum(
                1 for s in security_results if s.get("risk_score", 100) < 30
            )
            risky_count = len(security_results) - safe_count
            lines.append(
                f"Total checked: {len(security_results)} | Safe: {safe_count} | Risky: {risky_count}"
            )

            # Show top risky ones
            risky_pools = [s for s in security_results if s.get("risk_score", 0) >= 30][
                :5
            ]
            if risky_pools:
                lines.append("")
                lines.append("⚠️  HIGH RISK TOKENS:")
                for pool in risky_pools:
                    symbol = pool.get("symbol", "UNKNOWN")[:8]
                    risk = pool.get("risk_score", 0)
                    reasons = pool.get("reasons", [])[:2]  # Top 2 reasons
                    reason_str = " | ".join(reasons) if reasons else "Unknown risk"
                    lines.append(f"• {symbol:<8} | Risk: {risk}/100 | {reason_str}")
        else:
            lines.append("No security checks performed")
        lines.append("")

        # Metrics Summary
        lines.append("📈 KEY METRICS SUMMARY")
        lines.append("-" * 50)
        if new_pools:
            total_liq = sum(p.get("liquidity_usd", 0) for p in new_pools)
            total_vol = sum(p.get("volume_h1", 0) for p in new_pools)
            avg_age = sum(p.get("age_minutes", 0) for p in new_pools) / len(new_pools)

            lines.append(f"• Total New Liquidity: ${total_liq:,.0f}")
            lines.append(f"• Total 1H Volume: ${total_vol:,.0f}")
            lines.append(f"• Average Pool Age: {avg_age:.0f} minutes")
            lines.append(
                f"• Liquidity/Velocity Ratio: {(total_liq/total_vol):.2f}"
                if total_vol > 0
                else "• Liquidity/Velocity Ratio: N/A"
            )
        lines.append("")

        # LLM Prompt
        lines.append("🤖 LLM ANALYSIS PROMPT")
        lines.append("-" * 50)
        lines.append("Analyze this Web3 arbitrage scan data for trading opportunities:")
        lines.append(
            "1. Identify tokens with high liquidity velocity (volume/liq > 0.5)"
        )
        lines.append("2. Flag cross-chain price discrepancies for arb opportunities")
        lines.append("3. Highlight safe tokens with revoked authorities and locked LP")
        lines.append("4. Suggest entry points for new pools with < 60min age")
        lines.append(
            "5. Calculate potential profit ranges based on volume and price changes"
        )
        lines.append("")
        lines.append("Focus on Solana ecosystem opportunities with free RPC access.")

        return "\n".join(lines)

    def save_history(self, text: str, collected_data: Dict[str, Any]) -> None:
        """Save the aggregate text and raw data to history"""
        timestamp = collected_data.get("timestamp", utc_now_iso())
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        # Create signals_history directory
        history_dir = Path("signals_history")
        history_dir.mkdir(exist_ok=True)

        # Save text file
        timestamp_str = dt.strftime("%Y%m%d_%H%M")
        txt_filename = f"daily_aggregate_{timestamp_str}.txt"
        txt_path = history_dir / txt_filename

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)

        # Save raw JSON data
        json_filename = f"raw_{timestamp_str}.json"
        json_path = history_dir / json_filename
        write_json(json_path, collected_data)

        logger.info(f"Saved aggregate to {txt_path}")
        logger.info(f"Saved raw data to {json_path}")

        print(f"✅ Aggregate saved: {txt_path}")
        print("📋 Copy the content above to Claude/Grok/ChatGPT for analysis")
