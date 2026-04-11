"""Unit tests for the Regime Router Strategy."""

import pytest
from types import SimpleNamespace

from src.strategy.regime_classifier import decide_regime


@pytest.fixture
def mock_settings():
    return SimpleNamespace(
        ENTRY_SELECTOR_FAILCLOSED=False,
        RUG_DEV_SELL_PRESSURE_HARD=0.25,
        ENTRY_SCALP_SCORE_MIN=80.0,
        ENTRY_SCALP_MAX_AGE_SEC=480,
        ENTRY_RUG_MAX_SCALP=0.55,
        ENTRY_BUY_PRESSURE_MIN_SCALP=0.10,
        ENTRY_FIRST30S_BUY_RATIO_MIN=0.50,
        ENTRY_BUNDLE_CLUSTER_MIN=0.30,
        ENTRY_SCALP_MIN_X_SCORE=50.0,

        ENTRY_TREND_SCORE_MIN=85.0,
        ENTRY_RUG_MAX_TREND=0.55,
        ENTRY_HOLDER_GROWTH_MIN_TREND=50,
        ENTRY_SMART_WALLET_HITS_MIN_TREND=3,
        ENTRY_BUY_PRESSURE_MIN_TREND=0.15,
        ENTRY_TREND_MIN_X_SCORE=65.0,
        
        ENTRY_TREND_MULTI_CLUSTER_MIN=3,
        ENTRY_TREND_CLUSTER_CONCENTRATION_MAX=0.55,
        ENTRY_TREND_DEV_SELL_MAX=0.02,
        
        ENTRY_SCALP_BUNDLE_COUNT_MIN=2,
        
        ENTRY_REGIME_CONFIDENCE_FLOOR_TREND=0.55,
        ENTRY_REGIME_CONFIDENCE_FLOOR_SCALP=0.40,
        LINKAGE_HIGH_RISK_THRESHOLD=0.70,
    )

def _base_ctx():
    return {
        "token_address": "MockAddress",
        "regime_candidate": "ENTRY_CANDIDATE",
        "final_score": 90.0,
        "rug_score": 0.0,
        "rug_verdict": "PASS",
        "mint_revoked": True,
        "dev_sell_pressure_5m": 0.0,
    }


def test_strong_trend_bundle_lp_burn(mock_settings):
    ctx = _base_ctx()
    ctx.update({
        "holder_growth_5m": 120,
        "smart_wallet_hits": 5,
        "buy_pressure": 0.35,
        "x_validation_score": 75.0,
        "x_validation_delta": 10.0,
        "lp_burn_confirmed": True,
        "num_unique_clusters_first_60s": 4,
        "cluster_concentration_ratio": 0.30,
        "bundle_composition_dominant": "buy-only",
        "bundle_timing_from_liquidity_add_min": 1.5,
        "creator_in_cluster_flag": False,
        "bundle_failure_retry_pattern": 1,
        "bundle_success_rate": 0.8,
        "x_status": "ok"
    })
    
    decision = decide_regime(ctx, mock_settings)
    assert decision["regime"] == "TREND"
    assert decision["expected_hold_class"] in {"medium", "long"}
    assert "trend_lp_burn_confirmed" in decision["reason_flags"]


def test_strong_scalp_early_high_velocity(mock_settings):
    ctx = _base_ctx()
    ctx.update({
        "age_sec": 30,
        "volume_velocity": 5.5,
        "first30s_buy_ratio": 0.85,
        "bundle_count_first_60s": 3,
        "bundle_timing_from_liquidity_add_min": 0.1,
        "bundle_wallet_clustering_score": 0.80,
        "cluster_concentration_ratio": 0.60,
        "buy_pressure": 0.25,
        "bundle_cluster_score": 0.40,
        "x_validation_score": 55.0,
        "x_status": "ok",
        # Force fail TREND checks, e.g. lack of holder_growth
        "holder_growth_5m": 0, 
    })
    
    decision = decide_regime(ctx, mock_settings)
    assert decision["regime"] == "SCALP"
    assert decision["expected_hold_class"] == "short"


def test_dirty_orderflow_or_hook(mock_settings):
    ctx = _base_ctx()
    ctx.update({
        # Trend eligible base
        "holder_growth_5m": 120,
        "smart_wallet_hits": 5,
        "buy_pressure": 0.35,
        "x_validation_score": 75.0,
        "x_validation_delta": 10.0,
        "lp_burn_confirmed": True,
        
        # Bad order flow stats that creates blockers
        "num_unique_clusters_first_60s": 1,        # Fails multi cluster CONFIRMATION
        "cluster_concentration_ratio": 0.85,       # Fails max concentration
        "bundle_composition_dominant": "sell-only",# SELL_ONLY!
        "bundle_timing_from_liquidity_add_min": 0.05, # Fails accumulation too instant
        "creator_in_cluster_flag": True,           # Hard blocker!
        "bundle_failure_retry_pattern": 5,         # Retry max exceeded
        "bundle_success_rate": 0.1,                # Rate weak
        "x_status": "ok",

        # It fails SCALP due to low early buy ratio / low velocity
        "volume_velocity": 0.5,
        "first30s_buy_ratio": 0.10,
    })

    decision = decide_regime(ctx, mock_settings)
    assert decision["regime"] == "IGNORE"
    # we expect these to hit the blocker lists for both.
    assert len(decision["blockers"]) > 0


def test_missing_critical_context(mock_settings):
    ctx = {
        # missing token_address, missing scores context etc
    }
    
    # Should trigger the missing context catch and fallback to IGNORE.
    decision = decide_regime(ctx, mock_settings)
    assert decision["regime"] == "IGNORE"
    assert decision["ignore_evaluation"]["ignore"] is True

