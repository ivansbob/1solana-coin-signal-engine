"""Unit tests for Orderflow and Sybil Shield validation."""

import pytest
from src.strategy.orderflow_sybil_metrics import compute_orderflow_purity_metrics
from src.strategy.execution_gates import evaluate_orderflow_gates

def _base_ctx():
    return {
        "signed_buy_volume": 8500.0,
        "total_buy_volume": 10000.0,
        "block_0_buy_volume": 500.0,
        "repeat_buyer_count": 2,
        "unique_buyers_1m": 100,
        "wallets_in_largest_cluster": 5,
        "organic_taker_volume": 8000.0,
        "total_volume": 15000.0
    }

def test_clean_organic_flow_gets_high_purity_score():
    ctx = _base_ctx()
    metrics = compute_orderflow_purity_metrics(ctx)
    
    # 85% signed buy ratio -> min(1.0, 0.85/0.75) = 1.0 (0.35 points)
    # block 0 is 5% -> 0.25 * 0.95 = 0.2375
    # repeat buyer is 2% -> 0.20 * 0.98 = 0.196
    # sybil cluster is 5% -> 0.20 * 0.95 = 0.19
    # Total roughly 0.9735
    
    assert metrics["orderflow_purity_score"] > 0.90
    assert metrics["signed_buy_ratio"] == 0.85
    assert metrics["block_0_snipe_pct"] == 0.05
    
    gates = evaluate_orderflow_gates(metrics)
    assert gates["passed_hard_gates"] is True
    assert len(gates["hard_blockers"]) == 0
    assert len(gates["soft_blockers"]) == 0


def test_high_block0_snipe_triggers_hard_blocker():
    ctx = _base_ctx()
    ctx.update({
        "block_0_buy_volume": 4500.0  # 45% of total buys
    })
    
    metrics = compute_orderflow_purity_metrics(ctx)
    assert metrics["block_0_snipe_pct"] == 0.45
    
    gates = evaluate_orderflow_gates(metrics)
    assert gates["passed_hard_gates"] is False
    assert "excessive_block0_sniping" in gates["hard_blockers"]


def test_sybil_cluster_penalty_applies_correctly():
    ctx = _base_ctx()
    ctx.update({
        "wallets_in_largest_cluster": 55, # 55 out of 100
    })
    
    metrics = compute_orderflow_purity_metrics(ctx)
    assert metrics["sybil_cluster_ratio"] == 0.55
    
    # Drops the cluster component to ~0.09
    
    gates = evaluate_orderflow_gates(metrics)
    assert "high_sybil_cluster" in gates["soft_blockers"]


def test_repeat_buyer_heavy_flow_gets_penalty():
    ctx = _base_ctx()
    ctx.update({
        "repeat_buyer_count": 80, # 80 per 100 unique = wash trading heavy
    })
    
    metrics = compute_orderflow_purity_metrics(ctx)
    assert metrics["repeat_buyer_ratio"] == 0.80
    assert metrics["orderflow_purity_score"] < 0.90 # Expected to be lower due to the wash trading impact


def test_missing_orderflow_data_degrades_honestly():
    ctx = {
        # Nothing defined
    }
    
    metrics = compute_orderflow_purity_metrics(ctx)
    
    # Honest downgrades applies penalty caps
    # Extrapolates variables up to missing caps preventing total failure but significantly stunting score
    
    assert metrics["signed_buy_ratio"] == 0.30
    assert metrics["block_0_snipe_pct"] == 0.20
    assert metrics["repeat_buyer_ratio"] == 0.20
    assert metrics["sybil_cluster_ratio"] == 0.25
    assert metrics["organic_taker_volume_ratio"] == 0.30
    
    # 0.35 * (0.30/0.75=0.40) -> 0.14
    # 0.25 * 0.80 -> 0.20
    # 0.20 * 0.80 -> 0.16
    # 0.20 * 0.75 -> 0.15
    # Total roughly ~0.65 -> Passes hard gates but very weak.
    
    assert 0.60 <= metrics["orderflow_purity_score"] <= 0.70
    gates = evaluate_orderflow_gates(metrics)
    assert gates["passed_hard_gates"] is True
