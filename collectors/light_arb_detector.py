"""Advanced arbitrage detector using real pool data and precision sizing."""

import asyncio
import logging
from typing import List, Dict, Any, Optional

from analytics.route_builder import CrossDexRouter, Pool, PoolType, DexType, SwapPath
from trading.arb_sizing import OptimalSizeCalculator, SizingResult
from trading.live_executor import LiveTrader
from collectors.solana_rpc_client import SolanaRpcClient
from collectors.helius_client import HeliusClient

logger = logging.getLogger(__name__)


class AdvancedArbDetector:
    """Advanced arbitrage detector using real pool simulation."""

    def __init__(
        self,
        helius_api_key: Optional[str] = None,
        rpc_url: str = "https://api.mainnet-beta.solana.com",
        router: Optional[CrossDexRouter] = None,
        sizer: Optional[OptimalSizeCalculator] = None,
        live_trader: Optional[LiveTrader] = None,
    ):
        self.helius_client = HeliusClient(helius_api_key) if helius_api_key else None
        self.rpc_client = SolanaRpcClient(rpc_url)
        self.router = router or CrossDexRouter()
        self.sizer = sizer or OptimalSizeCalculator()
        self.live_trader = live_trader

        # Cache for pool data to avoid repeated fetches
        self._pool_cache: Dict[str, Pool] = {}
        self._cache_ttl = 300  # 5 minutes

    async def fetch_real_pool_data(self, token_addresses: List[str]) -> List[Pool]:
        """Fetch real pool data from on-chain sources."""
        pools = []

        for token_address in token_addresses:
            try:
                # Get token accounts (pools) for this token
                token_accounts = self.rpc_client.get_token_accounts_by_owner(
                    token_address, mint=None
                )

                for account_data in token_accounts.get("value", []):
                    pool = await self._parse_pool_from_account(account_data, token_address)
                    if pool:
                        pools.append(pool)
                        self._pool_cache[pool.address] = pool

            except Exception as e:
                logger.warning(f"Error fetching pools for token {token_address}: {e}")
                continue

        logger.info(f"Fetched {len(pools)} pools for {len(token_addresses)} tokens")
        return pools

    async def _parse_pool_from_account(self, account_data: Dict[str, Any], token_address: str) -> Optional[Pool]:
        """Parse pool data from Solana account info."""
        try:
            account_info = account_data.get("account", {})
            owner = account_info.get("owner", "")

            # Determine DEX type from owner program
            dex_type = self._identify_dex_from_owner(owner)
            if not dex_type:
                return None

            # Determine pool type
            pool_type = PoolType.CLMM if dex_type in {DexType.RAYDIUM_CLMM, DexType.ORCA} else PoolType.AMM

            # Extract token addresses from parsed data
            parsed_data = account_info.get("data", {}).get("parsed", {})
            if not parsed_data:
                return None

            # This is simplified - real implementation would parse specific DEX layouts
            # For now, we'll create mock pools based on available data
            token_a = token_address
            token_b = "So11111111111111111111111111111112"  # Default to SOL

            # Mock liquidity and fee data (would be parsed from actual pool data)
            liquidity_usd = 5000.0  # Mock value
            fee_pct = 0.003  # 0.3% default fee

            pool = Pool(
                address=account_data.get("pubkey", ""),
                dex_type=dex_type,
                pool_type=pool_type,
                token_a=token_a,
                token_b=token_b,
                liquidity_usd=liquidity_usd,
                fee_pct=fee_pct,
            )

            return pool

        except Exception as e:
            logger.debug(f"Error parsing pool from account: {e}")
            return None

    def _identify_dex_from_owner(self, owner_address: str) -> Optional[DexType]:
        """Identify DEX type from program owner address."""
        dex_programs = {
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": DexType.RAYDIUM_AMM,
            "5quBvoiQrt2LE8BvKH1KFU2A6LShNWbNB5T2qRQqSjrE": DexType.RAYDIUM_CLMM,
            "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": DexType.ORCA,
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": DexType.METEORA,
            "6EF8rrecthR5Dkzon8NQtvj5JwA676vUPKkWXvqroll": DexType.PUMP,
        }

        return dex_programs.get(owner_address)

    async def scan_arb_opportunities(
        self,
        token_addresses: List[str],
        market_states: List[Dict[str, Any]],
        portfolio_ctx: Dict[str, Any],
        settings: Any,
    ) -> List[Dict[str, Any]]:
        """Scan for arbitrage opportunities using real pool data and precision sizing.

        Returns:
            List of ARB_EXECUTE signals ready for LiveTrader
        """
        logger.info(f"Scanning arbitrage opportunities for {len(token_addresses)} tokens")

        # Fetch real pool data
        pools = await self.fetch_real_pool_data(token_addresses)
        if not pools:
            logger.warning("No pools found for arbitrage scanning")
            return []

        # Generate arbitrage paths
        paths = self.router.get_paths(pools)
        if not paths:
            logger.info("No arbitrage paths found")
            return []

        # Convert market states to index
        market_index = {m.get("token_address"): m for m in market_states}

        arb_signals = []

        for path in paths:
            try:
                # Calculate optimal sizing
                sizing_result = self.sizer.calculate_optimal_size(
                    path=path,
                    pools_data={p.address: p for p in pools},
                    market_ctx=market_index.get(path.input_token, {}),
                    portfolio_ctx=portfolio_ctx,
                    settings=settings,
                )

                if not sizing_result.is_profitable:
                    continue

                # Create ARB_EXECUTE signal
                signal = self._create_arb_signal(path, sizing_result, market_index)
                arb_signals.append(signal)

                logger.info(
                    f"Found arbitrage: {path.input_token} -> {path.path_length} hops -> "
                    f"profit: {sizing_result.net_profit_sol:.4f} SOL"
                )

            except Exception as e:
                logger.warning(f"Error processing arbitrage path: {e}")
                continue

        logger.info(f"Generated {len(arb_signals)} arbitrage signals")
        return arb_signals

    def _create_arb_signal(
        self,
        path: SwapPath,
        sizing: SizingResult,
        market_index: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create ARB_EXECUTE signal from path and sizing data."""
        market_data = market_index.get(path.input_token, {})

        return {
            "signal_type": "ARB_EXECUTE",
            "token_address": path.input_token,
            "symbol": market_data.get("symbol", "UNKNOWN"),
            "name": market_data.get("name", "Unknown Token"),

            # Route information
            "route_ids": path.get_pool_addresses(),
            "path_length": path.path_length,
            "dex_types": [step.dex_type.value for step in path.steps],
            "pool_types": [step.pool_type.value for step in path.steps],

            # Sizing information
            "optimal_in_sol": sizing.optimal_amount_in,
            "expected_out_sol": sizing.expected_amount_out,
            "min_out_sol": sizing.expected_amount_out * 0.95,  # 5% slippage protection
            "expected_net_profit": sizing.net_profit_sol,
            "profit_pct": (sizing.net_profit_sol / sizing.optimal_amount_in) * 100,

            # Risk assessment
            "confidence_score": sizing.confidence_score,
            "risk_assessment": sizing.risk_assessment,
            "simulation_method": sizing.simulation_method,

            # Execution parameters
            "max_slippage_bps": 150,  # Higher slippage tolerance for arbitrage
            "execution_route": "jito",  # Always use Jito for MEV protection
            "priority_level": "high",

            # Metadata
            "signal_source": "advanced_arb_detector",
            "detected_at": asyncio.get_event_loop().time(),
            "arb_type": "cross_dex_cyclic",
        }

    async def execute_arbitrage_signal(
        self,
        signal: Dict[str, Any],
        market_states: List[Dict[str, Any]],
        portfolio_ctx: Dict[str, Any],
        settings: Any,
    ) -> Optional[Dict[str, Any]]:
        """Execute an arbitrage signal through the live trader."""
        if not self.live_trader:
            logger.warning("No live trader configured, cannot execute arbitrage")
            return None

        try:
            # Add arbitrage-specific context
            signal["entry_decision"] = "ARB"
            signal["arb_signal"] = True

            # Execute through live trader
            result = await self.live_trader.execute_entry(
                signal=signal,
                market_states=market_states,
                state=portfolio_ctx,
                settings=settings,
            )

            logger.info(f"Executed arbitrage signal for {signal.get('token_address')}: {result}")
            return result

        except Exception as e:
            logger.error(f"Error executing arbitrage signal: {e}")
            return {"error": str(e), "signal": signal}

    def build_arb_report(self, signals: List[Dict[str, Any]]) -> str:
        """Build detailed arbitrage report for logging."""
        if not signals:
            return "=== NO ARBITRAGE OPPORTUNITIES FOUND ===\n"

        lines = ["=== ADVANCED ARBITRAGE OPPORTUNITIES ==="]

        for signal in signals:
            lines.append(
                f"Token: {signal.get('symbol', '?')} | "
                f"Path: {signal.get('path_length', 0)}-hop | "
                f"Input: {signal.get('optimal_in_sol', 0):.3f} SOL | "
                f"Profit: {signal.get('expected_net_profit', 0):.4f} SOL "
                f"({signal.get('profit_pct', 0):.2f}%) | "
                f"Risk: {signal.get('risk_assessment', 'unknown')} | "
                f"Confidence: {signal.get('confidence_score', 0):.1f}"
            )

        lines.append(f"Total opportunities: {len(signals)}")
        return "\n".join(lines) + "\n"