"""Liquidity Quality Engine ensuring slip metrics aren't ignored resolving trades on fragile LP bases."""

from typing import Dict, Any, Optional
import math
from src.strategy.types import LiquidityQualityEvidence


def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def compute_liquidity_quality_metrics(token_ctx: Dict[str, Any]) -> LiquidityQualityEvidence:
    buy_impact = _to_float(token_ctx.get("jupiter_buy_impact_bps"))
    sell_impact = _to_float(token_ctx.get("jupiter_sell_impact_bps"))
    
    base_share = _to_float(token_ctx.get("base_amm_liquidity_share"))
    dyn_share = _to_float(token_ctx.get("dynamic_liquidity_share"))
    
    # Missing data logic enforcing conservative penalties. 
    # High impact forces scores drastically downward blocking executions outright if thresholds exceed parameters.
    if buy_impact < 0:
        buy_impact = 100.0 # Extremely dangerous assumption if blind
    if sell_impact < 0:
        sell_impact = 150.0 # Exits matter more, severe conservative bounds
    if base_share < 0:
        base_share = 0.10 # Assume mostly dynamic
    if dyn_share < 0:
        dyn_share = 0.90
        
    uses_dyn = bool(token_ctx.get("route_uses_dynamic_liquidity", True))
    
    # Normalizing Impact
    # Buy impact normalizes via 60 multiplier baseline
    buy_norm = max(0.0, 1.0 - (buy_impact / 60.0))
    # Sell impact structurally more important hence larger multiplier limit giving harsh clamps 
    sell_norm = max(0.0, 1.0 - (sell_impact / 80.0))
    
    base_norm = min(1.0, base_share)
    
    # Mathematical bound integration 
    liquidity_score = (
        0.40 * buy_norm +
        0.30 * sell_norm +
        0.20 * max(0.0, 1.0 - dyn_share) +
        0.10 * base_norm
    )
    
    return {
        "jupiter_buy_impact_bps": round(buy_impact, 2),
        "jupiter_sell_impact_bps": round(sell_impact, 2),
        "base_amm_liquidity_share": round(base_share, 4),
        "dynamic_liquidity_share": round(dyn_share, 4),
        "route_uses_dynamic_liquidity": uses_dyn,
        "liquidity_quality_score": round(liquidity_score, 4)
    }

def compute_liquidity_refill_half_life(token_address: str, window_sec: int = 120, fetched_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Computes Liquidity Refill Half-Life.
    Formula: HalfLifeSec = window_sec * ln(0.5) / ln( liquidity_{t+120} / liquidity_{peak} )
    """
    if fetched_data is None:
        fetched_data = {}
        
    liq_peak = fetched_data.get("liquidity_peak")
    liq_rec = fetched_data.get("liquidity_recovered")

    if liq_peak is None or liq_rec is None or liq_peak <= 0:
        return {
            "liquidity_refill_half_life_sec": None,
            "liquidity_refill_score": None,
            "liquidity_refill_provenance": {
                "source": "simulated",
                "window_sec": window_sec,
                "error": "missing_data"
            }
        }
        
    ratio = liq_rec / liq_peak
    
    if ratio >= 1.0:
        # Full recovery or no drop
        half_life = 0.0
    elif ratio <= 0.0:
        # Complete drain
        half_life = float('inf')
    else:
        # Math formula implies returning actual seconds of half-life
        # If it takes `window_sec` to reach `ratio`, half-life is window_sec * ln(0.5) / ln(ratio)
        half_life = window_sec * (math.log(0.5) / math.log(ratio))

    if 30 <= half_life <= 180:
        score = 1.0
    elif 180 < half_life <= 300:
        score = 0.6
    else:
        score = 0.0
        
    return {
        "liquidity_refill_half_life_sec": round(half_life, 2) if half_life != float('inf') else 999999.0,
        "liquidity_refill_score": score,
        "liquidity_refill_provenance": {
            "source": fetched_data.get("source", "dune"),
            "window_sec": window_sec
        }
    }
