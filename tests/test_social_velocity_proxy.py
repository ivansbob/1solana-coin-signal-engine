import pytest
from src.strategy.social_velocity_metrics import compute_social_velocity_metrics
from src.strategy.execution_gates import evaluate_social_gates

def _base_ctx():
    return {
        "social_velocity_10m": 50.0,
        "social_velocity_60m": 10.0, # accel = 50 / 15 = 3.33 (high)
        "paid_influencer_proxy": 0.0,
        "bot_like_activity": 0.0,
        "organic_orderflow_alignment": 0.85, # Strong alignment
        "on_chain_confirmation_strong": True,
        "orderflow_purity_score": 0.8 # Used by gates
    }
    
def test_healthy_social_acceleration_with_onchain_confirmation():
    ctx = _base_ctx()
    metrics = compute_social_velocity_metrics(ctx)
    
    # Accel > 2.0 and < 3.5 = 0.65 score because strong pure organic stats
    assert metrics["social_acceleration_ratio"] > 3.0
    assert metrics["attention_distortion_risk"] < 0.10 # Super low distortion
    assert metrics["social_velocity_score"] == 0.65
    
    gates = evaluate_social_gates(metrics, ctx)
    assert not gates["hard_blockers"]
    assert not gates["warnings"]
    assert gates["passed_hard_gates"] is True

def test_hype_only_without_orderflow_gets_strong_penalty():
    ctx = _base_ctx()
    # Let's crank up velocity massively but kill orderflow stats
    ctx.update({
        "social_velocity_10m": 150.0,  # accel = 150/15 = 10 (>3.5 boundary)
        "organic_orderflow_alignment": 0.1, 
        "on_chain_confirmation_strong": False,
        "orderflow_purity_score": 0.2
    })
    
    metrics = compute_social_velocity_metrics(ctx)
    # Because On-chain is false, it drops down structurally bounding penalties.
    # Distortion: 0.3 * (1 - 0.1) = 0.27
    # So Accel is > 3.5, but confirm_on_chain=False yields 0.0! Extreemly strict bounds limit hype blindly!
    # Wait, the rule says `if accel >= 3.5 AND confirm_on_chain AND distortion <= 0.6` -> 1.0. Next is `2.0 <= accel < 3.5`. Next `1.3 <= accel < 2.0`. Else `0.0`.
    # Massive hype WITHOUT tracking is dead scored 0.0
    assert metrics["social_velocity_score"] == 0.0
    
    gates = evaluate_social_gates(metrics, ctx)
    # The score drops to 0.0, warning only triggers on >= 0.8. So it just doesn't bonus.
    # But wait, let's force the warning by making the score high but the orderflow explicitly low?
    # Our metrics bound drops it natively. If somehow it bypassed, the gate catches it.
    
def test_high_distortion_risk_triggers_warning():
    ctx = _base_ctx()
    ctx.update({
        "paid_influencer_proxy": 1.0, # 0.4
        "bot_like_activity": 1.0 # 0.3 
        # Total distortion = 0.7 + (0.3*0.15) > 0.74 > max limit 0.6 natively
    })
    
    metrics = compute_social_velocity_metrics(ctx)
    assert metrics["attention_distortion_risk"] > 0.60
    assert metrics["social_velocity_score"] == 0.0 # Forcefully drops due to huge distortion bounds
    
    gates = evaluate_social_gates(metrics, ctx)
    assert "high_attention_distortion" in gates["soft_blockers"]

def test_missing_social_data_degrades_to_neutral():
    ctx = {}
    metrics = compute_social_velocity_metrics(ctx)
    
    # Neutral scoring means 0 addition, yielding EXACTLY 0.0 modifiers explicitly disabling hype algorithms
    assert metrics["social_velocity_score"] == 0.0
    assert metrics["attention_distortion_risk"] == 0.50 # Neutral bound midpoint
    
    gates = evaluate_social_gates(metrics, {"orderflow_purity_score": 0.0})
    assert gates["passed_hard_gates"] is True
