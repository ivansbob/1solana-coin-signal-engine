"""Arbitrage optimal sizing calculator using precision strategy simulation."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from analytics.route_builder import Pool, PoolType, SwapPath
from trading.fill_model import simulate_entry_fill, simulate_exit_fill

logger = logging.getLogger(__name__)


@dataclass
class SizingResult:
    """Result of optimal sizing calculation."""
    optimal_amount_in: float
    expected_amount_out: float
    net_profit_sol: float
    is_profitable: bool
    confidence_score: float
    risk_assessment: str
    simulation_method: str


class OptimalSizeCalculator:
    """Calculator for finding optimal arbitrage trade sizes."""

    # Test amounts for grid simulation (in SOL)
    GRID_AMOUNTS_SOL = [0.1, 0.5, 1.0, 2.5, 5.0, 10.0]

    # Minimum profitable threshold (after Jito tips)
    MIN_PROFIT_THRESHOLD_SOL = 0.005

    def __init__(self, jito_tip_estimator: Optional[Any] = None):
        self.jito_tip_estimator = jito_tip_estimator

    def calculate_optimal_size(
        self,
        path: SwapPath,
        pools_data: Dict[str, Pool],
        market_ctx: Dict[str, Any],
        portfolio_ctx: Dict[str, Any],
        settings: Any,
    ) -> SizingResult:
        """Calculate optimal size for an arbitrage path.

        Args:
            path: Arbitrage path to optimize
            pools_data: Pool data indexed by address
            market_ctx: Market context data
            portfolio_ctx: Portfolio context
            settings: Application settings

        Returns:
            Optimal sizing result
        """
        if not path.steps:
            return SizingResult(
                optimal_amount_in=0.0,
                expected_amount_out=0.0,
                net_profit_sol=0.0,
                is_profitable=False,
                confidence_score=0.0,
                risk_assessment="empty_path",
                simulation_method="none"
            )

        # Check if all pools in path are CLMM (concentrated liquidity)
        has_clmm_pools = any(
            pools_data.get(step.pool_address, Pool("", None, PoolType.AMM, "", "", 0.0, 0.0)).pool_type == PoolType.CLMM
            for step in path.steps
        )

        if has_clmm_pools:
            # Use grid simulation for CLMM pools
            return self.simulate_precision_grid(
                path, pools_data, market_ctx, portfolio_ctx, settings
            )
        else:
            # Use mathematical calculation for AMM pools
            return self.calc_constant_product_optimal(
                path, pools_data, market_ctx, portfolio_ctx, settings
            )

    def calc_constant_product_optimal(
        self,
        path: SwapPath,
        pools_data: Dict[str, Pool],
        market_ctx: Dict[str, Any],
        portfolio_ctx: Dict[str, Any],
        settings: Any,
    ) -> SizingResult:
        """Calculate optimal size for AMM pools using mathematical optimization.

        For a single-pool triangular arbitrage, we solve for the maximum of:
        profit(x) = x * (1 - fee) * (y / (x + dx)) - x

        Where x is input amount, y is reserve of output token,
        dx is the change in reserves, fee is pool fee.

        This simplifies to finding the vertex of the profit parabola.
        """
        if len(path.steps) != 2 or path.steps[0].pool_address != path.steps[1].pool_address:
            # Fallback to grid simulation for complex paths
            return self.simulate_precision_grid(
                path, pools_data, market_ctx, portfolio_ctx, settings
            )

        pool = pools_data.get(path.steps[0].pool_address)
        if not pool or pool.pool_type != PoolType.AMM:
            return self.simulate_precision_grid(
                path, pools_data, market_ctx, portfolio_ctx, settings
            )

        # For triangular arbitrage in single AMM pool
        # This is a simplified mathematical approach
        # In practice, we'd need more complex math for multi-pool paths

        # Start with grid simulation as it's more reliable
        return self.simulate_precision_grid(
            path, pools_data, market_ctx, portfolio_ctx, settings
        )

    def simulate_precision_grid(
        self,
        path: SwapPath,
        pools_data: Dict[str, Pool],
        market_ctx: Dict[str, Any],
        portfolio_ctx: Dict[str, Any],
        settings: Any,
    ) -> SizingResult:
        """Simulate arbitrage path with grid of amounts to find optimal size.

        This is the approach used by the Rust donor code - brute force simulation
        of different trade sizes to find the one with maximum profit.
        """
        best_result = None
        max_profit = float('-inf')

        for amount_sol in self.GRID_AMOUNTS_SOL:
            try:
                # Create mock signal context for entry simulation
                entry_signal = self._create_mock_entry_signal(path, amount_sol, market_ctx)

                # Simulate entry (going into arbitrage)
                entry_fill = simulate_entry_fill(entry_signal, market_ctx, portfolio_ctx, settings)

                if entry_fill.get("tx_failed"):
                    continue

                # Calculate what we actually spent (including slippage)
                actual_spent = entry_fill.get("filled_notional_sol", 0.0)

                # For arbitrage, we need to simulate the exit path
                # This is simplified - in reality we'd need to track token balances
                # and simulate the reverse swaps through the same path

                # Estimate arbitrage profit (simplified)
                # In a real implementation, this would simulate the complete cycle
                estimated_profit = self._estimate_arbitrage_profit(
                    path, pools_data, actual_spent, market_ctx
                )

                # Subtract Jito tip costs
                jito_cost = self._estimate_jito_cost(amount_sol)
                net_profit = estimated_profit - jito_cost

                if net_profit > max_profit:
                    max_profit = net_profit
                    best_result = SizingResult(
                        optimal_amount_in=amount_sol,
                        expected_amount_out=actual_spent + estimated_profit,
                        net_profit_sol=net_profit,
                        is_profitable=net_profit > self.MIN_PROFIT_THRESHOLD_SOL,
                        confidence_score=self._calculate_confidence(path, pools_data, amount_sol),
                        risk_assessment=self._assess_risk(path, pools_data, amount_sol),
                        simulation_method="grid_simulation"
                    )

            except Exception as e:
                logger.warning(f"Error simulating amount {amount_sol} SOL: {e}")
                continue

        if best_result is None:
            return SizingResult(
                optimal_amount_in=0.0,
                expected_amount_out=0.0,
                net_profit_sol=0.0,
                is_profitable=False,
                confidence_score=0.0,
                risk_assessment="simulation_failed",
                simulation_method="grid_simulation"
            )

        return best_result

    def _create_mock_entry_signal(
        self,
        path: SwapPath,
        amount_sol: float,
        market_ctx: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a mock entry signal for simulation."""
        return {
            "token_address": path.input_token,
            "entry_decision": "ARB_EXECUTE",
            "route_ids": path.get_pool_addresses(),
            "optimal_in_sol": amount_sol,
            "recommended_position_pct": 0.01,  # Small position for arb
            "effective_position_pct": 0.01,
            "max_slippage_bps": 50,  # Conservative slippage for arb
            "execution_route": "jito",
            # Add other required fields
            "entry_reason": "arbitrage_opportunity",
            "regime_confidence": 0.8,
        }

    def _estimate_arbitrage_profit(
        self,
        path: SwapPath,
        pools_data: Dict[str, Pool],
        input_amount: float,
        market_ctx: Dict[str, Any]
    ) -> float:
        """Estimate arbitrage profit for a given path and input amount.

        This is a simplified estimation. In production, this would:
        1. Simulate each swap step through the pools
        2. Account for fees, slippage, and liquidity depth
        3. Calculate the complete cycle profit
        """
        # Simplified estimation based on path complexity and liquidity
        base_profit_pct = 0.001  # 0.1% base profit assumption

        # Adjust based on path complexity
        if path.path_length == 2:
            base_profit_pct *= 1.5  # 1-hop paths are more efficient
        elif path.path_length == 3:
            base_profit_pct *= 1.0  # 2-hop paths have more fees
        else:
            base_profit_pct *= 0.5  # Complex paths are less profitable

        # Adjust based on total liquidity in path
        total_liquidity = sum(
            pools_data.get(step.pool_address, Pool("", None, PoolType.AMM, "", "", 0.0, 0.0)).liquidity_usd
            for step in path.steps
        )
        liquidity_multiplier = min(total_liquidity / 10000.0, 2.0)  # Cap at 2x

        # Adjust based on fee structure
        total_fees = sum(step.fee_pct for step in path.steps)
        fee_penalty = max(0.1, 1.0 - total_fees * 10)  # Penalty for high fees

        estimated_profit_pct = base_profit_pct * liquidity_multiplier * fee_penalty

        return input_amount * estimated_profit_pct

    def _estimate_jito_cost(self, amount_sol: float) -> float:
        """Estimate Jito tip cost for a given transaction size."""
        # Base tip plus size-based component
        base_tip = 0.0001  # 0.0001 SOL base tip
        size_component = amount_sol * 0.001  # 0.1% of transaction size

        return base_tip + size_component

    def _calculate_confidence(
        self,
        path: SwapPath,
        pools_data: Dict[str, Pool],
        amount_sol: float
    ) -> float:
        """Calculate confidence score for the sizing result."""
        confidence = 0.5  # Base confidence

        # Higher confidence for simpler paths
        if path.path_length == 2:
            confidence += 0.2
        elif path.path_length == 3:
            confidence += 0.1

        # Higher confidence for higher liquidity
        total_liquidity = sum(
            pools_data.get(step.pool_address, Pool("", None, PoolType.AMM, "", "", 0.0, 0.0)).liquidity_usd
            for step in path.steps
        )
        if total_liquidity > 10000:
            confidence += 0.2
        elif total_liquidity > 5000:
            confidence += 0.1

        # Lower confidence for very large amounts
        if amount_sol > 5.0:
            confidence -= 0.1

        return max(0.0, min(1.0, confidence))

    def _assess_risk(
        self,
        path: SwapPath,
        pools_data: Dict[str, Pool],
        amount_sol: float
    ) -> str:
        """Assess risk level for the arbitrage opportunity."""
        risks = []

        # Check liquidity depth
        total_liquidity = sum(
            pools_data.get(step.pool_address, Pool("", None, PoolType.AMM, "", "", 0.0, 0.0)).liquidity_usd
            for step in path.steps
        )

        if total_liquidity < 5000:
            risks.append("low_liquidity")

        # Check path complexity
        if path.path_length > 3:
            risks.append("complex_path")

        # Check for CLMM pools (higher risk due to concentrated liquidity)
        has_clmm = any(
            pools_data.get(step.pool_address, Pool("", None, PoolType.AMM, "", "", 0.0, 0.0)).pool_type == PoolType.CLMM
            for step in path.steps
        )

        if has_clmm:
            risks.append("clmm_pool")

        # Check trade size relative to liquidity
        if amount_sol > total_liquidity * 0.01:  # >1% of liquidity
            risks.append("large_relative_size")

        if not risks:
            return "low_risk"
        elif len(risks) == 1:
            return f"moderate_risk_{risks[0]}"
        else:
            return f"high_risk_multiple_{len(risks)}_factors"