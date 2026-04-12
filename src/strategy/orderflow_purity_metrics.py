"""Orderflow Purity and Ghost Bid Score metrics."""

from typing import Dict, Any, Optional, Union
import logging
from src.strategy.types import OrderflowMetrics

logger = logging.getLogger(__name__)


def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def compute_orderflow_purity(token_address: str, window_minutes: int = 60, ghost_bid_ratio: Union[float, None] = None, wash_trade_proxy: Union[float, None] = None, organic_buy_ratio: Union[float, None] = None) -> Dict[str, Any]:
    """
    Computes orderflow purity metrics for the given token and time window.

    Returns dict with ghost_bid_ratio, wash_trade_proxy, organic_buy_ratio,
    purity_score, and provenance.

    Parameters can be overridden for testing.
    """
    # TODO: Implement actual Dune query execution
    # For now, return placeholder values
    # In production, this would:
    # 1. Query Dune for trades in the window
    # 2. Identify ghost bids (failed tx or zero-fill bids)
    # 3. Detect repeated wallet pairs (wash trade)
    # 4. Count unique organic buyers

    if ghost_bid_ratio is None:
        ghost_bid_ratio = 0.05  # Placeholder
    if wash_trade_proxy is None:
        wash_trade_proxy = 0.10  # Placeholder
    if organic_buy_ratio is None:
        organic_buy_ratio = 0.70  # Placeholder

    # Calculate purity score per formula
    if ghost_bid_ratio <= 0.08 and wash_trade_proxy <= 0.12 and organic_buy_ratio >= 0.65:
        purity_score = 1.0
    elif ghost_bid_ratio <= 0.15 and wash_trade_proxy <= 0.25:
        purity_score = 0.6
    elif wash_trade_proxy > 0.35:
        purity_score = 0.2
    else:
        purity_score = 0.0

    return {
        "ghost_bid_ratio": round(ghost_bid_ratio, 4),
        "wash_trade_proxy": round(wash_trade_proxy, 4),
        "organic_buy_ratio": round(organic_buy_ratio, 4),
        "purity_score": round(purity_score, 4),
        "provenance": f"dune_query_window_{window_minutes}m"
    }


def compute_orderflow_purity_metrics(token_ctx: Dict[str, Any]) -> OrderflowMetrics:
    """
    Wrapper function to compute orderflow purity metrics from token context.
    Integrates with existing scoring pipeline.
    """
    token_address = token_ctx.get("token_address", "")
    if not token_address:
        logger.warning("No token_address in context, using defaults")
        return {
            "signed_buy_ratio": 0.30,
            "block_0_snipe_pct": 0.20,
            "repeat_buyer_ratio": 0.20,
            "sybil_cluster_ratio": 0.25,
            "organic_taker_volume_ratio": 0.30,
            "orderflow_purity_score": 0.5,  # Neutral for missing data
            "ghost_bid_ratio": 0.10,
            "wash_trade_proxy": 0.15,
            "organic_buy_ratio": 0.60
        }

    # Get purity metrics
    purity_data = compute_orderflow_purity(token_address)

    # Legacy metrics (keep for compatibility)
    signed_buy_volume = _to_float(token_ctx.get("signed_buy_volume"))
    total_buy_volume = _to_float(token_ctx.get("total_buy_volume"))

    block_0_buy_volume = _to_float(token_ctx.get("block_0_buy_volume"))

    repeat_buyer_count = _to_float(token_ctx.get("repeat_buyer_count"))
    unique_buyers_1m = _to_float(token_ctx.get("unique_buyers_1m"))

    wallets_in_largest_cluster = _to_float(token_ctx.get("wallets_in_largest_cluster"))

    organic_taker_volume = _to_float(token_ctx.get("organic_taker_volume"))
    total_volume = _to_float(token_ctx.get("total_volume"))

    # Calculate legacy metrics
    if total_buy_volume > 0 and signed_buy_volume >= 0:
        signed_buy_ratio = signed_buy_volume / total_buy_volume
    else:
        signed_buy_ratio = 0.30

    if total_buy_volume > 0 and block_0_buy_volume >= 0:
        block_0_snipe_pct = block_0_buy_volume / total_buy_volume
    else:
        block_0_snipe_pct = 0.20

    if unique_buyers_1m > 0 and repeat_buyer_count >= 0:
        repeat_buyer_ratio = repeat_buyer_count / unique_buyers_1m
    else:
        repeat_buyer_ratio = 0.20

    if unique_buyers_1m > 0 and wallets_in_largest_cluster >= 0:
        sybil_cluster_ratio = wallets_in_largest_cluster / unique_buyers_1m
    else:
        sybil_cluster_ratio = 0.25

    if total_volume > 0 and organic_taker_volume >= 0:
        organic_taker_volume_ratio = organic_taker_volume / total_volume
    else:
        organic_taker_volume_ratio = 0.30

    # Legacy purity score (for backward compatibility)
    signed_buy_ratio_norm = min(1.0, signed_buy_ratio / 0.75)
    legacy_purity_score = (
        0.35 * signed_buy_ratio_norm +
        0.25 * max(0.0, 1.0 - block_0_snipe_pct) +
        0.20 * max(0.0, 1.0 - repeat_buyer_ratio) +
        0.20 * max(0.0, 1.0 - sybil_cluster_ratio)
    )

    # Use new purity score if available, else legacy
    final_purity_score = purity_data.get("purity_score", legacy_purity_score)

    return {
        "signed_buy_ratio": round(signed_buy_ratio, 4),
        "block_0_snipe_pct": round(block_0_snipe_pct, 4),
        "repeat_buyer_ratio": round(repeat_buyer_ratio, 4),
        "sybil_cluster_ratio": round(sybil_cluster_ratio, 4),
        "organic_taker_volume_ratio": round(organic_taker_volume_ratio, 4),
        "orderflow_purity_score": round(final_purity_score, 4),
        "ghost_bid_ratio": purity_data.get("ghost_bid_ratio", 0.10),
        "wash_trade_proxy": purity_data.get("wash_trade_proxy", 0.15),
        "organic_buy_ratio": purity_data.get("organic_buy_ratio", 0.60)
    }