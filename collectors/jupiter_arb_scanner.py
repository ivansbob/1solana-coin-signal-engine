from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import asyncio
from datetime import datetime, timezone

from .jupiter_quote_client import JupiterQuoteClient, ArbQuoteResult
from .jupiter_route_filter import is_viable
from utils.logger import log_info, log_warning


@dataclass
class ArbOpportunity:
    token_address: str
    symbol: str
    profit_lamports: int
    profit_pct: float
    route_label: str
    price_impact_pct: float
    arb_score: float  # 0-100
    scanned_at: str  # ISO UTC
    raw_quote1: dict
    raw_quote2: dict


class JupiterArbScanner:
    def __init__(self, amount_in_lamports: int = 1_000_000_000, min_profit_pct: float = 0.15,
                 max_concurrency: int = 5, use_lite_api: bool = False, slippage_bps: int = 0):
        self.amount_in_lamports = amount_in_lamports
        self.min_profit_pct = min_profit_pct
        self.max_concurrency = max_concurrency
        self.use_lite_api = use_lite_api
        self.slippage_bps = slippage_bps
        self.client = JupiterQuoteClient()

    async def _scan_single_token(self, token: Dict[str, Any]) -> Optional[ArbOpportunity]:
        """Scan a single token for arbitrage opportunities"""
        try:
            token_address = token.get("address") or token.get("mint") or token.get("token_address")
            if not token_address:
                return None

            symbol = token.get("symbol", "UNKNOWN")

            # Get arbitrage quotes
            result = await self.client.get_arb_quotes(
                base_mint=token_address,
                quote_mint="",  # Will use WSOL as quote
                amount_in=self.amount_in_lamports,
                slippage_bps=self.slippage_bps,
                use_lite=self.use_lite_api
            )

            # Check if viable
            if not is_viable(result, min_profit_pct=self.min_profit_pct):
                return None

            # Calculate arb score (0-100, higher is better)
            arb_score = min(100.0, result.profit_pct * 20)  # Scale profit % to score

            scanned_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            opportunity = ArbOpportunity(
                token_address=token_address,
                symbol=symbol,
                profit_lamports=result.profit_lamports,
                profit_pct=result.profit_pct,
                route_label=result.route_label,
                price_impact_pct=result.price_impact_pct,
                arb_score=arb_score,
                scanned_at=scanned_at,
                raw_quote1=result.quote1,
                raw_quote2=result.quote2
            )

            return opportunity

        except Exception as e:
            log_warning("arb_scan_error", token=token.get("symbol", "UNKNOWN"), error=str(e))
            return None

    async def scan_tokens(self, tokens: List[Dict[str, Any]], wsol_mint: str = "So11111111111111111111111111111111111111112") -> List[ArbOpportunity]:
        """Scan multiple tokens concurrently"""
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def scan_with_semaphore(token: Dict[str, Any]) -> Optional[ArbOpportunity]:
            async with semaphore:
                return await self._scan_single_token(token)

        # Scan all tokens concurrently
        tasks = [scan_with_semaphore(token) for token in tokens]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and None results
        opportunities = []
        for result in results:
            if isinstance(result, ArbOpportunity):
                opportunities.append(result)
            elif isinstance(result, Exception):
                log_warning("arb_scan_task_error", error=str(result))

        # Sort by arb_score descending
        opportunities.sort(key=lambda x: x.arb_score, reverse=True)

        log_info("arb_scan_complete", total_tokens=len(tokens), viable_opportunities=len(opportunities))

        return opportunities

    def to_daily_aggregate_section(self, opportunities: List[ArbOpportunity]) -> str:
        """Generate the arbitrage scanner section for daily aggregate"""
        if not opportunities:
            return "\n## JUPITER ARB SCANNER SUMMARY\nNo viable arbitrage opportunities found.\n"

        high_confidence = [o for o in opportunities if o.arb_score >= 60]
        viable = len(opportunities)

        section = "\n## JUPITER ARB SCANNER SUMMARY\n"
        section += f"Total viable opportunities: {viable}\n"
        section += f"High-confidence opportunities (score ≥60): {len(high_confidence)}\n\n"

        if opportunities:
            section += "### Top Opportunities:\n"
            for i, opp in enumerate(opportunities[:5], 1):  # Top 5
                section += f"{i}. **{opp.symbol}** ({opp.token_address[:8]}...)\n"
                section += f"   - Profit: {opp.profit_pct:.2f}% ({opp.profit_lamports:,} lamports)\n"
                section += f"   - Route: {opp.route_label}\n"
                section += f"   - Impact: {opp.price_impact_pct:.2f}%\n"
                section += f"   - Score: {opp.arb_score:.1f}/100\n\n"

        return section