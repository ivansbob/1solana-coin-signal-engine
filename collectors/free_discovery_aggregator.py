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
from .github_signal import (
    collect_enhanced_github_candidates,
    build_github_text_section,
    generate_coding_agent_prompt,
    generate_coding_agent_prompt_v2,
)
from .github_velocity_tracker import run_github_velocity_tracker
from .new_pools import get_new_pools
from .cross_chain_collector import get_cross_chain_pools
from .pump_fun_collector import run_pump_fun_graduation_tracker, generate_pump_fun_coding_agent_prompt
from .security_checker import SecurityChecker
from .security_and_arb_checker import SecurityAndArbChecker
from .onchain_liquidity_collector import OnchainLiquidityCollector
from .light_arb_detector import LightArbDetector

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
            self._collect_onchain_liquidity(),
            self._collect_github_velocity(),
            self._collect_pump_fun_graduation(),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        github_data = results[0] if not isinstance(results[0], Exception) else []
        new_pools_data = results[1] if not isinstance(results[1], Exception) else []
        cross_chain_data = results[2] if not isinstance(results[2], Exception) else []
        onchain_liquidity_data = (
            results[3] if not isinstance(results[3], Exception) else []
        )
        velocity_data = results[4] if not isinstance(results[4], Exception) else []
        pump_fun_data = results[5] if not isinstance(results[5], Exception) else []

        # Basic security checks for top candidates
        security_data = await self._collect_security_checks(
            new_pools_data[: self.max_candidates]
        )

        # Security and arb checks for onchain liquidity candidates
        security_arb_results = await self._collect_security_arb_checks(
            onchain_liquidity_data
        )

        # Generate coding agent prompt for GitHub repos
        coding_agent_prompt = (
            generate_coding_agent_prompt(github_data) if github_data else ""
        )

        # Generate velocity-based coding agent prompt
        velocity_coding_prompt = (
            generate_coding_agent_prompt_v2(velocity_data) if velocity_data else ""
        )

        # Generate Pump.Fun graduation coding agent prompt
        pump_fun_coding_prompt = (
            generate_pump_fun_coding_agent_prompt(pump_fun_data) if pump_fun_data else ""
        )

        collected_data = {
            "timestamp": utc_now_iso(),
            "github_repos": github_data,
            "github_velocity_repos": velocity_data,
            "pump_fun_graduation_tokens": pump_fun_data,
            "new_pools": new_pools_data[: self.max_candidates],
            "cross_chain_pools": cross_chain_data[: self.max_candidates],
            "onchain_liquidity_candidates": onchain_liquidity_data,
            "security_results": security_data,
            "security_arb_results": security_arb_results,
            "coding_agent_prompt": coding_agent_prompt,
            "velocity_coding_agent_prompt": velocity_coding_prompt,
            "pump_fun_coding_agent_prompt": pump_fun_coding_prompt,
        }

        # Save coding agent payload separately
        self.save_coding_agent_payload(github_data, velocity_data, pump_fun_data)

        logger.info(
            f"Collection complete: {len(github_data)} github repos, {len(velocity_data)} velocity repos, {len(pump_fun_data)} pump_fun graduates, {len(new_pools_data)} new pools, {len(cross_chain_data)} cross-chain, {len(onchain_liquidity_data)} onchain liquidity, {len(security_data)} security checks, {len(security_arb_results)} security+arb checks"
        )
        return collected_data

    async def _collect_github_activity(self) -> List[Dict[str, Any]]:
        """Collect enhanced GitHub repos with 2026 metrics and X signals"""
        try:
            candidates = await collect_enhanced_github_candidates()
            # Return the candidates directly, sorted by risk_adjusted_score desc
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

    async def _collect_onchain_liquidity(self) -> List[Dict[str, Any]]:
        """Collect onchain liquidity candidates"""
        try:
            collector = OnchainLiquidityCollector()
            return await collector.get_onchain_liquidity_candidates(
                max_candidates=self.max_candidates
            )
        except Exception as e:
            logger.error(f"Onchain liquidity collection failed: {e}")
            return []

    async def _collect_github_velocity(self) -> List[Dict[str, Any]]:
        """Collect GitHub velocity data with delta metrics"""
        try:
            velocity_data = await run_github_velocity_tracker(max_repos=25)
            return velocity_data
        except Exception as e:
            logger.error(f"GitHub velocity collection failed: {e}")
            return []

    async def _collect_pump_fun_graduation(self) -> List[Dict[str, Any]]:
        """Collect Pump.Fun graduation tracker data"""
        try:
            pump_fun_data = await run_pump_fun_graduation_tracker()
            return pump_fun_data
        except Exception as e:
            logger.error(f"Pump.Fun graduation collection failed: {e}")
            return []

    async def _collect_security_checks(
        self, pools: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Basic security checks for top pools"""
        security_checker = SecurityChecker()
        token_addresses = [
            pool.get("token_address", "") for pool in pools if pool.get("token_address")
        ]

        if not token_addresses:
            return []

        try:
            security_results = await security_checker.check_batch(
                token_addresses, chain="solana"
            )
            return security_results
        except Exception as e:
            logger.warning(f"Batch security check failed: {e}")
            # Return error results for all tokens
            return [
                {
                    "token_address": addr,
                    "symbol": next(
                        (
                            pool.get("symbol", "UNKNOWN")
                            for pool in pools
                            if pool.get("token_address") == addr
                        ),
                        "UNKNOWN",
                    ),
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

    async def _collect_security_arb_checks(
        self, pools: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Security and light arb checks for onchain pools"""
        checker = SecurityAndArbChecker()
        token_addresses = [
            pool.get("token_address", "") for pool in pools if pool.get("token_address")
        ]

        if not token_addresses:
            return []

        try:
            security_arb_results = await checker.check_batch(
                token_addresses, chain="solana"
            )
            return security_arb_results
        except Exception as e:
            logger.warning(f"Batch security and arb check failed: {e}")
            # Return error results for all tokens
            return [
                {
                    "token_address": addr,
                    "symbol": next(
                        (
                            pool.get("symbol", "UNKNOWN")
                            for pool in pools
                            if pool.get("token_address") == addr
                        ),
                        "UNKNOWN",
                    ),
                    "honeypot": False,
                    "rug_score": 10.0,
                    "risk_level": "HIGH",
                    "verdict": "BLOCK",
                    "reasons": [f"Security check error: {str(e)[:50]}"],
                    "solana_specific": {},
                    "cross_dex_spread_pct": 0.0,
                    "arb_spread_score": 0,
                    "arb_opportunity": False,
                    "checked_at": utc_now_iso(),
                }
                for addr in token_addresses
            ]

    async def build_aggregate_text(self, collected_data: Dict[str, Any]) -> str:
        """Build the beautiful daily aggregate text"""
        timestamp = collected_data.get("timestamp", utc_now_iso())
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        lines = []
        lines.append(
            f"=== WEB3 FREE ARBITRAGE SCAN {dt.strftime('%Y-%m-%d %H:%M')} ==="
        )
        lines.append("")

        # Add header for cleaner LLM input
        lines.append("# Solana Free Discovery Aggregate + Moon Score")
        lines.append(f"Generated at: {datetime.now(timezone.utc).isoformat()}")
        lines.append("")
        lines.append("Top tokens sorted by Moon Score descending.")
        lines.append("Only tokens with Moon Score > 30 or graduating are worth attention.")
        lines.append("")

        # Coding Agent Prompt first for high-priority repos
        coding_prompt = collected_data.get("coding_agent_prompt", "")
        if coding_prompt:
            lines.append(coding_prompt)
            lines.append("")

        # GitHub Repos
        github_text = build_github_text_section(collected_data.get("github_repos", []))
        lines.append(github_text)
        lines.append("")

        # Multi-Velocity GitHub Signals
        velocity_prompt = collected_data.get("velocity_coding_agent_prompt", "")
        if velocity_prompt:
            lines.append("## MULTI-VELOCITY GITHUB SIGNALS (PR-3)")
            lines.append("-" * 50)
            lines.append(velocity_prompt)
            lines.append("")

        # Pump.Fun Graduation Alpha
        pump_fun_prompt = collected_data.get("pump_fun_coding_agent_prompt", "")
        if pump_fun_prompt:
            lines.append("## PUMP.FUN GRADUATION ALPHA (PR-2 2026)")
            lines.append("-" * 50)
            lines.append(pump_fun_prompt)
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

        # Onchain Liquidity Candidates
        lines.append("💰 ON-CHAIN LIQUIDITY CANDIDATES")
        lines.append("-" * 50)
        onchain_candidates = collected_data.get("onchain_liquidity_candidates", [])
        if onchain_candidates:
            collector = OnchainLiquidityCollector()
            liquidity_text = collector.build_liquidity_text_section(onchain_candidates)
            lines.append(liquidity_text)
        else:
            lines.append("No onchain liquidity candidates detected")
        lines.append("")

        # Light Arbitrage Opportunities
        lines.append("🔄 LIGHT ARBITRAGE OPPORTUNITIES")
        lines.append("-" * 50)
        if onchain_candidates:
            arb_detector = LightArbDetector()
            enriched_pools = await arb_detector.enrich_with_arb_data(onchain_candidates)
            arb_text = arb_detector.build_arb_text_section(enriched_pools)
            lines.append(arb_text)
        else:
            lines.append("No arbitrage data available")
        lines.append("")

        # Security + Light Arb Check
        lines.append("🛡️ SECURITY + LIGHT ARB CHECK")
        lines.append("-" * 50)
        security_arb_results = collected_data.get("security_arb_results", [])
        if security_arb_results:
            checker = SecurityAndArbChecker()
            combined_text = checker.build_combined_text_section(security_arb_results)
            lines.append(combined_text)
        else:
            lines.append("No security and arb data available")
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

        # Arbitrage Opportunities
        lines.append("🔄 ARBITRAGE OPPORTUNITIES")
        lines.append("-" * 50)
        arb_opps = collected_data.get("arb_opportunities", [])
        if arb_opps:
            for opp in sorted(arb_opps, key=lambda x: x.get("arb_score", 0), reverse=True)[:10]:  # Top 10
                symbol = opp.get("symbol", "UNKNOWN")[:8]
                score = opp.get("arb_score", 0)
                spread = opp.get("spread_pct", 0)
                cross_chain = "Yes" if opp.get("cross_chain_listed") else "No"
                lines.append(f"• {symbol:<8} | Score: {score:.1f} | Spread: {spread:.2f}% | Cross-chain: {cross_chain}")
        else:
            lines.append("No arbitrage opportunities detected")
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

    def save_coding_agent_payload(
        self, github_enriched: List[Dict[str, Any]], velocity_data: List[Dict[str, Any]], pump_fun_data: List[Dict[str, Any]]
    ) -> None:
        """Save coding agent payload for separate processing"""
        if not github_enriched and not velocity_data and not pump_fun_data:
            return

        timestamp = utc_now_iso()
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        # Create signals_history directory
        history_dir = Path("signals_history")
        history_dir.mkdir(exist_ok=True)

        # Save coding agent payload
        timestamp_str = dt.strftime("%Y%m%d_%H%M")
        payload_filename = f"coding_agent_payload_{timestamp_str}.json"
        payload_path = history_dir / payload_filename
        write_json(
            payload_path,
            {
                "github_enriched": github_enriched,
                "velocity_candidates": velocity_data,
                "pump_fun_candidates": pump_fun_data,
                "timestamp": timestamp,
            },
        )

        logger.info(f"Saved coding agent payload to {payload_path}")

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
