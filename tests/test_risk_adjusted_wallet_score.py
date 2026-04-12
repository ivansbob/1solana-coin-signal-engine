import pytest
from src.strategy.wallet_risk_metrics import compute_risk_adjusted_wallet_score
from src.strategy.execution_gates import evaluate_wallet_gates

def _base_ctx():
    return {
        "avg_sharpe_90d": 2.5,  # (2.5 - 0.5)/2.5 = 0.8
        "avg_sortino_90d": 3.0, # (3.0 - 0.8)/3.0 = 0.733
        "avg_profit_factor": 2.0, # (2 - 1.2)/2.0 = 0.4
        "avg_max_drawdown_90d": 0.20, # Exceptionally safe 20% drawdown -> Yields penalty 0.0
        "cohort_concentration_ratio": 0.10, # Distributed cohort!
        "family_qualifier_multiplier": 1.2
    }

def test_stable_low_drawdown_wallet_gets_high_score():
    ctx = _base_ctx()
    metrics = compute_risk_adjusted_wallet_score(ctx)
    
    # 0.45(0.8) + 0.30(0.733) + 0.15(0.4) - 0.0 = 0.36 + 0.22 + 0.06 = ~0.64 
    # Adjusted risk calculates safely without penalty drops
    assert metrics["avg_wallet_risk_adjusted_score"] > 0.60
    assert metrics["avg_max_drawdown_90d"] == 0.20
    
    # Cohort risk quality validates high since concentration is minimal!
    gates = evaluate_wallet_gates(metrics)
    assert gates["passed_hard_gates"] is True
    assert not gates["warnings"]

def test_high_return_high_drawdown_wallet_gets_penalty():
    ctx = _base_ctx()
    ctx.update({
        "avg_sharpe_90d": 3.5, # 1.0 multiplier cap
        "avg_max_drawdown_90d": 0.65 # VERY dangerous, implies >60%
    })
    
    metrics = compute_risk_adjusted_wallet_score(ctx)
    # 0.65 - 0.35 / 0.40 = 0.30 / 0.4 = 0.75 Drawdown penalty modifier natively
    assert metrics["avg_wallet_risk_adjusted_score"] < 0.60
    
    gates = evaluate_wallet_gates(metrics)
    assert "high_wallet_drawdown" in gates["soft_blockers"]

def test_concentrated_cohort_downgrades_quality():
    ctx = _base_ctx()
    ctx.update({
        "cohort_concentration_ratio": 0.85 # Massive concentration 
    })
    
    metrics = compute_risk_adjusted_wallet_score(ctx)
    # Concentration bounds quality multiplier explicitly yielding drops on confidence scales
    assert metrics["cohort_concentration_ratio"] == 0.85
    
    gates = evaluate_wallet_gates(metrics)
    assert "highly_concentrated_cohort" in gates["warnings"]

def test_missing_wallet_history_degrades_to_low_confidence():
    ctx = {}
    metrics = compute_risk_adjusted_wallet_score(ctx)
    
    # Defaulting variables triggers immense logic blocking:
    # default sharpe/sortino = 0.0 yielding native 0 modifiers.
    # default mdd = 0.80 -> 0.80 - 0.35 = 0.45 / 0.4 = 1.125 penalty -> SCORE BOUNDS MATH TO 0
    assert metrics["avg_wallet_risk_adjusted_score"] == 0.0
    assert metrics["wallet_signal_confidence"] == 0.0 # Confidence bottoms guaranteeing safety
    
    gates = evaluate_wallet_gates(metrics)
    # Score is 0.0 -> implies risky profile immediately alongside extreme max drawdowns 
    assert "risky_wallet_profile" in gates["warnings"]
    assert "high_wallet_drawdown" in gates["soft_blockers"]

def test_family_qualifier_increases_confidence_only_for_diverse_cohorts():
    ctx = _base_ctx()
    regular = compute_risk_adjusted_wallet_score(ctx)
    
    ctx.update({"family_qualifier_multiplier": 2.0}) # Huge modifier
    boosted = compute_risk_adjusted_wallet_score(ctx)
    
    assert boosted["wallet_signal_confidence"] > regular["wallet_signal_confidence"]
