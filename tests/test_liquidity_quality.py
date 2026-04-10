"""Tests validating deterministic liquidity execution penalties."""

import pytest
from src.strategy.liquidity_metrics import compute_liquidity_quality_metrics
from src.strategy.execution_gates import evaluate_liquidity_gates

def _base_ctx():
    return {
        "jupiter_buy_impact_bps": 15.0,
        "jupiter_sell_impact_bps": 20.0,
        "base_amm_liquidity_share": 0.85,
        "dynamic_liquidity_share": 0.15,
        "route_uses_dynamic_liquidity": False
    }

def test_good_base_liquidity_gets_high_quality_score():
    ctx = _base_ctx()
    metrics = compute_liquidity_quality_metrics(ctx)
    
    # 0.4(1-0.25) + 0.3(1-0.25) + 0.2(1-0.15) + 0.1(0.85) = 0.3+0.225+0.17+0.085 = 0.78
    assert metrics["liquidity_quality_score"] > 0.75
    
    gates = evaluate_liquidity_gates(metrics)
    assert not gates["hard_blockers"]
    assert not gates["soft_blockers"]
    assert not gates["warnings"]

def test_high_dynamic_share_with_impact_gets_penalty():
    ctx = _base_ctx()
    ctx.update({
        "jupiter_buy_impact_bps": 40.0,
        "jupiter_sell_impact_bps": 50.0,
        "base_amm_liquidity_share": 0.10,
        "dynamic_liquidity_share": 0.90, # Massive dynamic share
        "route_uses_dynamic_liquidity": True
    })
    
    metrics = compute_liquidity_quality_metrics(ctx)
    
    # Score gets hit severely by high dynamic limits and impact ratios
    # BuyNorm = 1 - 40/60 = 0.333
    # SellNorm = 1 - 50/80 = 0.375
    # score ≈ 0.4(0.33) + 0.3(0.375) + 0.2(0.1) + 0.1(0.1) ≈ 0.133 + 0.112 + 0.02 + 0.01 ≈ 0.275
    
    assert metrics["liquidity_quality_score"] < 0.45
    
    gates = evaluate_liquidity_gates(metrics)
    assert "fragile_liquidity" in gates["soft_blockers"]

def test_asymmetric_sell_impact_triggers_warning():
    ctx = _base_ctx()
    ctx.update({
        "jupiter_buy_impact_bps": 10.0, 
        "jupiter_sell_impact_bps": 95.0, # Dangerous sell side
    })
    
    metrics = compute_liquidity_quality_metrics(ctx)
    gates = evaluate_liquidity_gates(metrics)
    
    assert "dangerous_sell_slippage" in gates["warnings"]
    assert gates["passed_hard_gates"] is True # Only a warning explicitly!

def test_excessive_buy_impact_triggers_hard_blocker():
    ctx = _base_ctx()
    ctx.update({
        "jupiter_buy_impact_bps": 55.0, # Violates 45 ceiling
        "jupiter_sell_impact_bps": 55.0
    })
    
    metrics = compute_liquidity_quality_metrics(ctx)
    gates = evaluate_liquidity_gates(metrics)
    
    assert "excessive_buy_impact" in gates["hard_blockers"]
    assert gates["passed_hard_gates"] is False

def test_missing_route_data_degrades_honestly():
    ctx = {}
    metrics = compute_liquidity_quality_metrics(ctx)
    
    # Missing data translates natively to high penalty approximations:
    # buy_impact forced to 100
    # sell_impact forced to 150
    # Score mathematically clamps metrics to zero logic yielding minimum score bounded to fractions.
    
    assert metrics["jupiter_buy_impact_bps"] == 100.0
    assert metrics["jupiter_sell_impact_bps"] == 150.0
    assert metrics["liquidity_quality_score"] < 0.20
    
    gates = evaluate_liquidity_gates(metrics)
    
    # 100 > 45 buy impacts hard block execution outright
    assert "excessive_buy_impact" in gates["hard_blockers"]
    assert "dangerous_sell_slippage" in gates["warnings"]
    assert gates["passed_hard_gates"] is False
