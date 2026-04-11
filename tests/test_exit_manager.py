"""Unit limits verifying hard stop bounds trigger efficiently based upon dynamic friction networks."""

import pytest
from src.strategy.exit_manager import evaluate_net_executable_pnl, process_exit_decision

def _base_ctx():
    return {
        "gross_mark_to_market_pnl_pct": 5.0, # Up 5% gross on paper
        "jupiter_buy_impact_bps": 200.0, # Buy slippage 2%
        "jupiter_sell_impact_bps": 300.0, # Sell slippage 3%
        "base_fee_bps": 5.0, # 0.05%
        "priority_fee_bps": 15.0, # 0.15%
        "jito_tip_estimate_bps": 80.0, # 0.8%
        "smart_money_bagholder_threshold": -5.0,
        "bundle_sell_pressure": 0.10,
        "regime_decision": "SCALP"
    }

def test_gross_green_but_net_negative_case():
    ctx = _base_ctx()
    # Net PNL math = 5% - 2% (buy) - 3% (sell) - (5+15+80)bps=1% fee = -1.0% Net! 
    # That is mathematically negative despite +5% gross graphics.
    
    snap = evaluate_net_executable_pnl(ctx)
    assert snap["net_executable_pnl_pct"] == -1.0
    
    decision = process_exit_decision(ctx)
    assert decision["invalidated"] is False
    assert decision["action"] == "HOLD"
    # Doesn't hit any hard limits but proves Gross != Net

def test_hard_stop_triggers_correctly_by_regime():
    ctx = _base_ctx()
    ctx.update({"gross_mark_to_market_pnl_pct": -6.0}) 
    
    # Gross -6% - 2% - 3% - 1% = -12% Net
    # SCALP limit is -12%. Thus, this explicitly invalidates.
    ctx["regime_decision"] = "SCALP"
    decision = process_exit_decision(ctx)
    assert decision["action"] == "HARD_SL"
    
    # TREND limit is -22%, it should survive at -12%
    ctx["regime_decision"] = "TREND"
    decision_trend = process_exit_decision(ctx)
    assert decision_trend["action"] == "HOLD"

def test_unconditional_hard_stop_bounds():
    ctx = _base_ctx()
    ctx.update({"gross_mark_to_market_pnl_pct": -15.0})
    # Gross -15 - 6 = -21% Net. Unconditional limit is -18% regardless of TREND/DIP.
    ctx["regime_decision"] = "DIP" 
    decision = process_exit_decision(ctx)
    assert decision["action"] == "FORCE_EXIT"
    assert decision["reason"] == "unconditional_hard_stop"

def test_smart_money_bagholder_forces_defensive_exit():
    ctx = _base_ctx()
    ctx.update({
        "gross_mark_to_market_pnl_pct": -2.0, # Net = -8.0%
        "smart_money_bagholder_threshold": -5.0, # Threshold breached (-8 < -5)
        "distribution_risk": 0.85 # High distributions trigger it
    })
    
    decision = process_exit_decision(ctx)
    assert decision["invalidated"] is True
    assert decision["action"] == "DEFENSIVE_EXIT"
    assert decision["reason"] == "smart_money_bagholder_distributing"

def test_missing_sell_impact_degrades_to_conservative_behavior():
    ctx = _base_ctx()
    # Dropping missing network fees causing default defensive behavior
    ctx.pop("jupiter_sell_impact_bps") 
    ctx.pop("jito_tip_estimate_bps")
    
    snap = evaluate_net_executable_pnl(ctx)
    # 150 bps default sell + 15 bps jito default -> heavier limits applied
    # net = 5.0 - 2.0(buy) - 1.5(sell) - 0.35(fees) = 1.15
    assert snap["net_executable_pnl_pct"] > 0
    assert snap["total_fee_bps_estimate"] > 30.0 # Base defaults aggregated
