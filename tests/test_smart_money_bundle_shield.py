"""Unit tests for Smart Money Distance and Bundle Shield routines."""

import pytest
from src.strategy.smart_money_metrics import compute_smart_money_distance_metrics
from src.strategy.execution_gates import evaluate_smart_money_gates

def _base_ctx():
    return {
        "current_price": 0.010,
        "smart_cohort_weighted_avg_entry_price": 0.009, # Very close, distance = 11.11%
        "recent_bundle_ratio": 0.15, # 15% bundled
        "bundle_sell_pressure": 0.05 # 5% sell pressure from bundles
    }

def test_early_clean_smart_money_gets_high_score():
    ctx = _base_ctx()
    
    metrics = compute_smart_money_distance_metrics(ctx)
    
    # Distance is 11.11% so <= 32 yields score 1.0!
    assert metrics["distance_from_smart_entry_pct"] < 12.0
    assert metrics["smart_money_distance_score"] == 1.0
    
    # Bundle sum is 0.20, max(0, 1 - 0.20/1.5) = 1 - 0.1333 = 0.8667
    assert metrics["bundle_pressure_score"] > 0.85
    
    # Combined = 0.62(1.0) + 0.38(0.8667)
    assert metrics["smart_money_combined_score"] > 0.90
    
    gates = evaluate_smart_money_gates(metrics)
    assert gates["passed_hard_gates"] is True
    assert not gates["hard_blockers"]
    assert not gates["soft_blockers"]
    assert not gates["warnings"]


def test_late_overextended_chase_gets_strong_penalty():
    ctx = _base_ctx()
    ctx.update({
        "current_price": 0.020,
        "smart_cohort_weighted_avg_entry_price": 0.010 # 100% distance
    })
    
    metrics = compute_smart_money_distance_metrics(ctx)
    
    # Distance > 80% yields score 0.0
    assert metrics["smart_money_distance_score"] == 0.0
    assert metrics["distance_from_smart_entry_pct"] == 100.0
    
    gates = evaluate_smart_money_gates(metrics)
    assert "late_smart_money_chase" in gates["warnings"]


def test_high_bundle_pressure_triggers_shield():
    ctx = _base_ctx()
    ctx.update({
        "recent_bundle_ratio": 0.80,
        "bundle_sell_pressure": 0.70
    })
    
    metrics = compute_smart_money_distance_metrics(ctx)
    # bundle_sum = 1.50
    # bundle_pressure_score = max(0, 1.0 - (1.50/1.50)) = 0.0
    assert metrics["bundle_pressure_score"] == 0.0
    
    gates = evaluate_smart_money_gates(metrics)
    assert gates["passed_hard_gates"] is True # Only soft blockers triggers if distance is safe
    assert "high_bundle_pressure" in gates["soft_blockers"]


def test_missing_smart_cohort_data_degrades_honestly():
    ctx = {
        "current_price": 0.010
        # Missing smart_cohort entry data
    }
    
    metrics = compute_smart_money_distance_metrics(ctx)
    
    # Missing data forces distance to 100% -> score 0.0 -> penalty. Ensures bot doesnt guess.
    assert metrics["distance_from_smart_entry_pct"] == 100.0
    assert metrics["smart_money_distance_score"] == 0.0
    
    # Missing bundle data falls back to cautious defaults (0.50 each)
    # bundle sum = 1.0. bundle_score = 1.0 - (1.0/1.5) = 0.3333
    assert 0.30 < metrics["bundle_pressure_score"] < 0.35
    
    gates = evaluate_smart_money_gates(metrics)
    assert not gates["passed_hard_gates"] # 100 distance + low bundle = Blocked!
    assert "overextended_crowded_entry" in gates["hard_blockers"]
    assert "late_smart_money_chase" in gates["warnings"]

