"""Cross-DEX arbitrage route builder for generating cyclic arbitrage paths."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from enum import Enum

logger = logging.getLogger(__name__)


class DexType(Enum):
    """Supported DEX types."""
    RAYDIUM_AMM = "raydium_amm"
    RAYDIUM_CLMM = "raydium_clmm"
    ORCA = "orca"
    METEORA = "meteora"
    PUMP = "pump"


class PoolType(Enum):
    """Pool liquidity types."""
    AMM = "amm"  # Constant product: x*y=k
    CLMM = "clmm"  # Concentrated liquidity


@dataclass
class Pool:
    """Represents a DEX pool."""
    address: str
    dex_type: DexType
    pool_type: PoolType
    token_a: str
    token_b: str
    liquidity_usd: float
    fee_pct: float
    reserves_a: Optional[int] = None
    reserves_b: Optional[int] = None
    tick_spacing: Optional[int] = None  # For CLMM pools
    current_tick: Optional[int] = None  # For CLMM pools
    sqrt_price_x64: Optional[int] = None  # For CLMM pools


@dataclass
class SwapStep:
    """Single step in an arbitrage path."""
    pool_address: str
    input_token: str
    output_token: str
    dex_type: DexType
    pool_type: PoolType
    fee_pct: float


@dataclass
class SwapPath:
    """Complete arbitrage path (cycle)."""
    steps: List[SwapStep]
    expected_profit_pct: Optional[float] = None
    estimated_gas_cost: Optional[float] = None
    risk_score: Optional[float] = None

    @property
    def input_token(self) -> str:
        """Get the input token for the path."""
        return self.steps[0].input_token if self.steps else ""

    @property
    def output_token(self) -> str:
        """Get the output token for the path."""
        return self.steps[-1].output_token if self.steps else ""

    @property
    def is_cyclic(self) -> bool:
        """Check if this is a cyclic arbitrage path."""
        return self.input_token == self.output_token and len(self.steps) > 1

    @property
    def path_length(self) -> int:
        """Get the number of hops in the path."""
        return len(self.steps)

    def get_pool_addresses(self) -> List[str]:
        """Get all pool addresses in the path."""
        return [step.pool_address for step in self.steps]


class CrossDexRouter:
    """Router for generating cross-DEX arbitrage paths."""

    # Base tokens for arbitrage cycles
    BASE_TOKENS = {
        "So11111111111111111111111111111112",  # SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    }

    # Minimum liquidity requirements by DEX
    LIQUIDITY_THRESHOLDS = {
        DexType.RAYDIUM_AMM: 2000.0,
        DexType.RAYDIUM_CLMM: 2000.0,
        DexType.ORCA: 2000.0,
        DexType.METEORA: 2000.0,
        DexType.PUMP: 1000.0,  # Lower threshold for newer tokens
    }

    def __init__(self, min_liquidity_multiplier: float = 1.0):
        self.min_liquidity_multiplier = min_liquidity_multiplier

    def filter_pools_by_liquidity(self, pools: List[Pool]) -> List[Pool]:
        """Filter pools by minimum liquidity requirements."""
        filtered_pools = []

        for pool in pools:
            min_liquidity = self.LIQUIDITY_THRESHOLDS.get(pool.dex_type, 1000.0)
            min_liquidity *= self.min_liquidity_multiplier

            if pool.liquidity_usd >= min_liquidity:
                filtered_pools.append(pool)

        logger.debug(f"Filtered {len(pools)} pools to {len(filtered_pools)} based on liquidity")
        return filtered_pools

    def build_token_graph(self, pools: List[Pool]) -> Dict[str, List[Pool]]:
        """Build a graph of token -> pools mapping."""
        graph: Dict[str, List[Pool]] = {}

        for pool in pools:
            # Add bidirectional edges
            if pool.token_a not in graph:
                graph[pool.token_a] = []
            if pool.token_b not in graph:
                graph[pool.token_b] = []

            graph[pool.token_a].append(pool)
            graph[pool.token_b].append(pool)

        return graph

    def generate_1hop_paths(self, pools: List[Pool]) -> List[SwapPath]:
        """Generate 1-hop arbitrage paths (token -> base -> token)."""
        paths = []
        base_tokens = self.BASE_TOKENS

        for pool in pools:
            token_a, token_b = pool.token_a, pool.token_b

            # Check if one token is base and the other is not
            if token_a in base_tokens and token_b not in base_tokens:
                # Path: token_b -> token_a (base) -> token_b
                path = SwapPath(steps=[
                    SwapStep(
                        pool_address=pool.address,
                        input_token=token_b,
                        output_token=token_a,
                        dex_type=pool.dex_type,
                        pool_type=pool.pool_type,
                        fee_pct=pool.fee_pct,
                    ),
                    SwapStep(
                        pool_address=pool.address,
                        input_token=token_a,
                        output_token=token_b,
                        dex_type=pool.dex_type,
                        pool_type=pool.pool_type,
                        fee_pct=pool.fee_pct,
                    ),
                ])
                paths.append(path)

            elif token_b in base_tokens and token_a not in base_tokens:
                # Path: token_a -> token_b (base) -> token_a
                path = SwapPath(steps=[
                    SwapStep(
                        pool_address=pool.address,
                        input_token=token_a,
                        output_token=token_b,
                        dex_type=pool.dex_type,
                        pool_type=pool.pool_type,
                        fee_pct=pool.fee_pct,
                    ),
                    SwapStep(
                        pool_address=pool.address,
                        input_token=token_b,
                        output_token=token_a,
                        dex_type=pool.dex_type,
                        pool_type=pool.pool_type,
                        fee_pct=pool.fee_pct,
                    ),
                ])
                paths.append(path)

        logger.debug(f"Generated {len(paths)} 1-hop arbitrage paths")
        return paths

    def generate_2hop_paths(self, pools: List[Pool]) -> List[SwapPath]:
        """Generate 2-hop arbitrage paths (base -> token_a -> token_b -> base)."""
        paths = []
        token_graph = self.build_token_graph(pools)
        base_tokens = self.BASE_TOKENS

        # Create pool lookup by address for fast access
        pool_lookup = {pool.address: pool for pool in pools}

        for base_token in base_tokens:
            if base_token not in token_graph:
                continue

            # Get all pools connected to this base token
            base_pools = token_graph[base_token]

            for pool1 in base_pools:
                # Determine the intermediate token
                intermediate_token = pool1.token_b if pool1.token_a == base_token else pool1.token_a

                # Skip if intermediate token is also a base token
                if intermediate_token in base_tokens:
                    continue

                # Find pools that connect intermediate_token back to base_token
                if intermediate_token not in token_graph:
                    continue

                intermediate_pools = token_graph[intermediate_token]

                for pool2 in intermediate_pools:
                    # Check if this pool connects back to base_token
                    if pool2.token_a == base_token or pool2.token_b == base_token:
                        # Make sure pools are different
                        if pool1.address == pool2.address:
                            continue

                        # Determine the direction for pool2
                        if pool2.token_a == intermediate_token and pool2.token_b == base_token:
                            # intermediate -> base
                            path = SwapPath(steps=[
                                SwapStep(
                                    pool_address=pool1.address,
                                    input_token=base_token,
                                    output_token=intermediate_token,
                                    dex_type=pool1.dex_type,
                                    pool_type=pool1.pool_type,
                                    fee_pct=pool1.fee_pct,
                                ),
                                SwapStep(
                                    pool_address=pool2.address,
                                    input_token=intermediate_token,
                                    output_token=base_token,
                                    dex_type=pool2.dex_type,
                                    pool_type=pool2.pool_type,
                                    fee_pct=pool2.fee_pct,
                                ),
                            ])
                            paths.append(path)

                        elif pool2.token_b == intermediate_token and pool2.token_a == base_token:
                            # intermediate -> base
                            path = SwapPath(steps=[
                                SwapStep(
                                    pool_address=pool1.address,
                                    input_token=base_token,
                                    output_token=intermediate_token,
                                    dex_type=pool1.dex_type,
                                    pool_type=pool1.pool_type,
                                    fee_pct=pool1.fee_pct,
                                ),
                                SwapStep(
                                    pool_address=pool2.address,
                                    input_token=intermediate_token,
                                    output_token=base_token,
                                    dex_type=pool2.dex_type,
                                    pool_type=pool2.pool_type,
                                    fee_pct=pool2.fee_pct,
                                ),
                            ])
                            paths.append(path)

        logger.debug(f"Generated {len(paths)} 2-hop arbitrage paths")
        return paths

    def get_paths(self, pools: List[Pool]) -> List[SwapPath]:
        """Generate all possible arbitrage paths from pool data."""
        # Filter pools by liquidity
        filtered_pools = self.filter_pools_by_liquidity(pools)

        if not filtered_pools:
            logger.warning("No pools meet liquidity requirements")
            return []

        # Generate all path types
        paths = []

        # 1-hop paths (triangular arbitrage within single pool)
        paths.extend(self.generate_1hop_paths(filtered_pools))

        # 2-hop paths (cross-pool arbitrage)
        paths.extend(self.generate_2hop_paths(filtered_pools))

        # Filter to only cyclic paths
        cyclic_paths = [path for path in paths if path.is_cyclic]

        logger.info(f"Generated {len(cyclic_paths)} valid cyclic arbitrage paths from {len(filtered_pools)} pools")
        return cyclic_paths

    def deduplicate_paths(self, paths: List[SwapPath]) -> List[SwapPath]:
        """Remove duplicate paths based on pool sequence."""
        seen = set()
        unique_paths = []

        for path in paths:
            # Create a hash based on the sequence of pool addresses
            path_hash = tuple(path.get_pool_addresses())
            if path_hash not in seen:
                seen.add(path_hash)
                unique_paths.append(path)

        logger.debug(f"Deduplicated {len(paths)} paths to {len(unique_paths)} unique paths")
        return unique_paths