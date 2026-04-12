"""Orderflow purity and Sybil cluster detection logic."""

from typing import Dict, Any
from src.strategy.types import OrderflowMetrics


def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def compute_orderflow_purity_metrics(token_ctx: Dict[str, Any]) -> OrderflowMetrics:
    """
    Computes purity metrics isolating organic vs manipulated volumes.
    Implements conservative defaults when underlying data is missing.
    """
    signed_buy_volume = _to_float(token_ctx.get("signed_buy_volume"))
    total_buy_volume = _to_float(token_ctx.get("total_buy_volume"))
    
    block_0_buy_volume = _to_float(token_ctx.get("block_0_buy_volume"))
    
    repeat_buyer_count = _to_float(token_ctx.get("repeat_buyer_count"))
    unique_buyers_1m = _to_float(token_ctx.get("unique_buyers_1m"))
    
    wallets_in_largest_cluster = _to_float(token_ctx.get("wallets_in_largest_cluster"))
    
    organic_taker_volume = _to_float(token_ctx.get("organic_taker_volume"))
    total_volume = _to_float(token_ctx.get("total_volume"))

    # Missing data logic enforcement: 
    # If missing, we fall back to conservative defaults (not clean but not catastrophic).
    if total_buy_volume > 0 and signed_buy_volume >= 0:
        signed_buy_ratio = signed_buy_volume / total_buy_volume
    else:
        signed_buy_ratio = 0.30  # Heavy penalty for missing validation
        
    if total_buy_volume > 0 and block_0_buy_volume >= 0:
        block_0_snipe_pct = block_0_buy_volume / total_buy_volume
    else:
        block_0_snipe_pct = 0.20  # Assumed some sniper risk

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

    # Formulas from specification
    # Normalizing signed buy ratio over a 0.75 target
    signed_buy_ratio_norm = min(1.0, signed_buy_ratio / 0.75)
    
    orderflow_purity_score = (
        0.35 * signed_buy_ratio_norm +
        0.25 * max(0.0, 1.0 - block_0_snipe_pct) +
        0.20 * max(0.0, 1.0 - repeat_buyer_ratio) +
        0.20 * max(0.0, 1.0 - sybil_cluster_ratio)
    )

    return {
        "signed_buy_ratio": round(signed_buy_ratio, 4),
        "block_0_snipe_pct": round(block_0_snipe_pct, 4),
        "repeat_buyer_ratio": round(repeat_buyer_ratio, 4),
        "sybil_cluster_ratio": round(sybil_cluster_ratio, 4),
        "organic_taker_volume_ratio": round(organic_taker_volume_ratio, 4),
        "orderflow_purity_score": round(orderflow_purity_score, 4),
    }

