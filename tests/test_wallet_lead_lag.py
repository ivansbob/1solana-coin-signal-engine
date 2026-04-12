"""Unit tests for Wallet Lead-Lag metrics."""

import pytest
from src.strategy.wallet_lead_lag_metrics import compute_wallet_lead_lag_metrics
from src.strategy.execution_gates import evaluate_wallet_lead_lag_gates

def _base_ctx():
    return {
        "wallet_lead_lag_sec": 30.0,
        "multi_timeframe_confirmation": {"1m": True, "5m": True, "15m": True}
    }

def test_strong_lead_lag_high_score():
    ctx = _base_ctx()
    ctx["wallet_lead_lag_sec"] = 22.0  # Within 8-45 range

    metrics = compute_wallet_lead_lag_metrics(ctx)

    assert metrics["lead_lag_score"] == 1.0
    assert metrics["multi_timeframe_confirmation_score"] == 1.0
    assert "lag=22.0s, tf_confirm=3/3" in metrics["lead_lag_provenance"]

    gates = evaluate_wallet_lead_lag_gates(metrics)
    assert gates["passed_hard_gates"] is True
    assert not gates["soft_blockers"]

def test_moderate_lead_lag_medium_score():
    ctx = _base_ctx()
    ctx["wallet_lead_lag_sec"] = 68.0  # 45-90 range

    metrics = compute_wallet_lead_lag_metrics(ctx)

    assert metrics["lead_lag_score"] == 0.65
    assert metrics["wallet_lead_lag_sec"] == 68.0

def test_sybil_like_instant_buys_zero_score():
    ctx = _base_ctx()
    ctx["wallet_lead_lag_sec"] = 2.0  # Too fast, <8

    metrics = compute_wallet_lead_lag_metrics(ctx)

    assert metrics["lead_lag_score"] == 0.0

def test_weak_lead_lag_low_score():
    ctx = _base_ctx()
    ctx["wallet_lead_lag_sec"] = 140.0  # 90-180 range

    metrics = compute_wallet_lead_lag_metrics(ctx)

    assert metrics["lead_lag_score"] == 0.3

def test_missing_lead_lag_data_defaults():
    ctx = {}  # No data

    metrics = compute_wallet_lead_lag_metrics(ctx)

    assert metrics["lead_lag_score"] == 0.0
    assert metrics["multi_timeframe_confirmation_score"] == 0.0

def test_partial_multi_tf_confirmation():
    ctx = _base_ctx()
    ctx["wallet_lead_lag_sec"] = 25.0
    ctx["multi_timeframe_confirmation"] = {"1m": True, "5m": True, "15m": False}  # 2/3

    metrics = compute_wallet_lead_lag_metrics(ctx)

    assert metrics["lead_lag_score"] == 1.0
    assert metrics["multi_timeframe_confirmation_score"] == 0.6

def test_weak_lead_lag_triggers_soft_blocker():
    ctx = _base_ctx()
    ctx["wallet_lead_lag_sec"] = 5.0  # Too fast

    metrics = compute_wallet_lead_lag_metrics(ctx)
    gates = evaluate_wallet_lead_lag_gates(metrics)

    assert "weak_lead_lag" in gates["soft_blockers"]