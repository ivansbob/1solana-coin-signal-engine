"""Unit tests for DIP regime and explicit exit behaviors."""

import pytest
from types import SimpleNamespace
from src.strategy.regime_classifier import decide_regime
from src.strategy.exit_manager import evaluate_dip_invalidation

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
        
        ENTRY_REGIME_CONFIDENCE_FLOOR_TREND=0.55,
        ENTRY_REGIME_CONFIDENCE_FLOOR_SCALP=0.40,
        ENTRY_REGIME_CONFIDENCE_FLOOR_DIP=0.50,
        LINKAGE_HIGH_RISK_THRESHOLD=0.70,
    )

def _base_ctx():
    return {
        "token_address": "DipAddress1",
        "regime_candidate": "ENTRY_CANDIDATE",
        "final_score": 88.0,
        "rug_score": 0.0,
        "rug_verdict": "PASS",
        "mint_revoked": True,
        "dev_sell_pressure_5m": 0.0,
        "cluster_distribution_risk": 0.10,
    }


def test_strong_dip_recovery_with_support_reclaim(mock_settings):
    ctx = _base_ctx()
    ctx.update({
        "drawdown_from_local_high_pct": 0.35, # 35% drop
        "rebound_strength_pct": 0.12, # 12% bounce
        "sell_exhaustion_score": 0.85,
        "support_reclaim_flag": True,
        
        # Ensure TREND/SCALP are ineligible to prevent overlaps
        "volume_velocity": 0,
        "buy_pressure": 0
    })
    
    decision = decide_regime(ctx, mock_settings)
    assert decision["regime"] == "DIP"
    assert "dip_support_reclaimed" in decision["reason_flags"]


def test_knife_catch_rejection_on_no_rebound(mock_settings):
    ctx = _base_ctx()
    ctx.update({
        "drawdown_from_local_high_pct": 0.40, # Massive 40% drop
        "rebound_strength_pct": 0.02, # Tiny 2% bounce (continuation of dropping)
        "sell_exhaustion_score": 0.30, # Sellers still strong
        "support_reclaim_flag": False,
        "volume_velocity": 0,
        "buy_pressure": 0
    })
    
    decision = decide_regime(ctx, mock_settings)
    assert decision["regime"] == "IGNORE"
    # Should catch blockers protecting the user
    assert "rebound_insufficient" in decision["blockers"] or "support_not_reclaimed" in decision["blockers"]


def test_dip_invalidation_on_new_low():
    position_ctx = {
        "dip_evidence": {"confidence": 0.8},
        "entry_price": 100.0,
        "local_minimum_at_entry": 90.0,
        "minutes_held": 10
    }
    
    current_ctx = {
        "current_price": 85.0 # Fell below the 90.0 support low!
    }
    
    exit_decision = evaluate_dip_invalidation(position_ctx, current_ctx)
    assert exit_decision["invalidated"] is True
    assert exit_decision["action"] == "HARD_SL"


def test_dip_exit_on_sell_exhaustion_failure():
    position_ctx = {
        "dip_evidence": {"confidence": 0.8},
        "entry_price": 100.0,
        "local_minimum_at_entry": 90.0,
        "minutes_held": 10
    }
    
    current_ctx = {
        "current_price": 102.0, # Holding flat
        "sell_exhaustion_score": 0.25 # Exhaustion failed, sellers resumed
    }
    
    exit_decision = evaluate_dip_invalidation(position_ctx, current_ctx)
    assert exit_decision["invalidated"] is True
    assert exit_decision["action"] == "FAST_EXIT"


def test_missing_rebound_data_leads_to_ignore(mock_settings):
    ctx = _base_ctx()
    ctx.update({
        "drawdown_from_local_high_pct": 0.30,
        # Missing rebound, exhaustion, reclaim
        "volume_velocity": 0,
        "buy_pressure": 0
    })
    
    decision = decide_regime(ctx, mock_settings)
    # The absence of critical data forces conservative defaults, blocking the entry.
    assert decision["regime"] == "IGNORE"
    assert "rebound_data_missing" in decision["blockers"]
