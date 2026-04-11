"""Dynamic deterministic exit boundaries evaluating NET PnL natively."""

from typing import Dict, Any, Optional
from src.strategy.friction_model import estimate_total_friction_bps
from src.strategy.types import ExitSnapshot, ExitDecision

def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def evaluate_net_executable_pnl(token_ctx: Dict[str, Any]) -> ExitSnapshot:
    gross_pnl = _to_float(token_ctx.get("gross_mark_to_market_pnl_pct"))
    buy_impact = _to_float(token_ctx.get("jupiter_buy_impact_bps"))
    sell_impact = _to_float(token_ctx.get("jupiter_sell_impact_bps"))
    smart_thresh = _to_float(token_ctx.get("smart_money_bagholder_threshold"), default=-5.0)
    bundle_pres = _to_float(token_ctx.get("bundle_sell_pressure"), default=0.0)
    
    # Missing data bounds defending equity heavily
    if gross_pnl is None:
        gross_pnl = -999.0
    if buy_impact < 0:
        buy_impact = 100.0 # Paper liquidity forces heavy assumed costs
    if sell_impact < 0:
        sell_impact = 150.0 
        
    buy_pct = buy_impact / 100.0
    sell_pct = sell_impact / 100.0
    
    total_fee_bps = estimate_total_friction_bps(token_ctx)
    fee_pct = total_fee_bps / 100.0
    
    # Crucial transition evaluating net execution paths explicitly
    net_pnl = gross_pnl - buy_pct - sell_pct - fee_pct
    
    return {
        "gross_mark_to_market_pnl_pct": round(gross_pnl, 4),
        "net_executable_pnl_pct": round(net_pnl, 4),
        "net_executable_pnl_sol": 0.0, # Computed higher up natively scaling per sizing
        "hard_stop_loss_pct": -18.0, # Baseline fallback
        "smart_money_bagholder_threshold": smart_thresh,
        "bundle_sell_pressure": bundle_pres,
        "total_fee_bps_estimate": total_fee_bps
    }

def process_exit_decision(token_ctx: Dict[str, Any]) -> ExitDecision:
    snap = evaluate_net_executable_pnl(token_ctx)
    regime = str(token_ctx.get("regime_decision", "IGNORE")).upper()
    net_pnl = snap["net_executable_pnl_pct"]
    
    # Fallback to extreme defense if gross was literally unknown
    if snap["gross_mark_to_market_pnl_pct"] == -999.0:
        return {"invalidated": True, "action": "DEFENSIVE_EXIT", "reason": "missing_gross_pnl_metrics"}

    # ==========================
    # 1. Unconditional Hard Stop
    # ==========================
    if net_pnl <= -18.0:
        return {"invalidated": True, "action": "FORCE_EXIT", "reason": "unconditional_hard_stop"}

    # ==========================
    # 2. Regime-Aware Stops
    # ==========================
    if regime == "SCALP" and net_pnl <= -12.0:
        return {"invalidated": True, "action": "HARD_SL", "reason": "scalp_hard_stop"}
    if regime == "TREND" and net_pnl <= -22.0:
        return {"invalidated": True, "action": "HARD_SL", "reason": "trend_hard_stop"}
    if regime == "DIP" and net_pnl <= -28.0:
        return {"invalidated": True, "action": "HARD_SL", "reason": "dip_hard_stop"}

    # ==========================
    # 3. Defensive Exits (Bagholder + Bundles)
    # ==========================
    distribution = _to_float(token_ctx.get("distribution_risk"), default=0.0)
    
    if net_pnl < snap["smart_money_bagholder_threshold"] and distribution > 0.6:
        return {"invalidated": True, "action": "DEFENSIVE_EXIT", "reason": "smart_money_bagholder_distributing"}
        
    if snap["bundle_sell_pressure"] > 0.70:
        return {"invalidated": True, "action": "DEFENSIVE_EXIT", "reason": "high_bundle_pressure_post_entry"}

    return {"invalidated": False, "action": "HOLD", "reason": "active_trade_clean_zones"}

# Retained backwards compatibility logic processing early DIP parameters specifically if executed outside master scope.
def evaluate_dip_invalidation(dip_evidence: Dict[str, Any], current_exhaustion: float, min_price_since_entry: float, entry_price: float) -> Dict[str, Any]:
    if min_price_since_entry < entry_price:
        dip_evidence["dip_invalidated_flag"] = True
        return {"invalidated": True, "reason": "price_dropped_below_local_minimum", "action": "HARD_SL"}
        
    if current_exhaustion >= 0 and current_exhaustion < 0.45:
        dip_evidence["dip_invalidated_flag"] = True
        return {"invalidated": True, "reason": "sell_exhaustion_failed", "action": "FAST_EXIT"}
        
    return {"invalidated": False, "reason": "dip_intact", "action": "HOLD"}
