import pytest
from src.strategy.scoring_vX import compute_final_score

def test_carry_score_warning_and_synergy():
    """Test evaluating carry score warning limits and synergy bonus directly."""
    # 1. Test Low Carry Warning (< 0.35)
    candidate_low = {
        "token_address": "CARRY123",
        "dex_screener_score": 5.0,
        "carry_total_score": 0.2, 
        "defi_health": {
            "defi_health_score": 0.5,
            "is_microcap_meme": False,
            "defi_coverage_status": "full"
        }
    }
    
    result_low = compute_final_score(candidate_low, settings={})
    
    assert "low_carry_potential" in result_low.get("carry_warnings", []), "Missing low carry potential warning"
    
    # 2. Test Synergy Bonus (+0.12 if Carry >= 0.75 and DeFiHealth >= 0.7)
    candidate_synergy = {
        "token_address": "CARRY456",
        "dex_screener_score": 5.0,
        "carry_total_score": 0.8,
        "defi_health": {
            "defi_health_score": 0.8,
            "is_microcap_meme": False,
            "defi_coverage_status": "full"
        }
    }
    
    result_synergy = compute_final_score(candidate_synergy, settings={})
    assert "low_carry_potential" not in result_synergy.get("carry_warnings", [])
    
    # Base = 5.0 + 0.15*0.8(DeFi) + 0.09*0.8(Carry) + 0.12(Synergy) = 5.312
    # Minus 20 if entry block? We mock without checking net exec so friction is default -15 meaning -20 score modifier.
    # Actually evaluate_net_executable_pnl probably returns something like 0 without mocks or uses mock?
    # Let's compare difference between synergy and near-synergy to avoid test brittleness around friction
    
    candidate_near = {
        "token_address": "CARRY789",
        "dex_screener_score": 5.0,
        "carry_total_score": 0.7, # misses 0.75 by 0.05
        "defi_health": {
            "defi_health_score": 0.8,
            "is_microcap_meme": False,
            "defi_coverage_status": "full"
        }
    }
    
    result_near = compute_final_score(candidate_near, settings={})
    
    score_syn = result_synergy.get("final_score_with_orderflow", 0.0)
    score_near = result_near.get("final_score_with_orderflow", 0.0)
    
    # Difference should be synergy bonus (0.12) + difference in carry base addition (0.09 * (0.8 - 0.7)) = 0.129
    diff = score_syn - score_near
    assert abs(diff - 0.129) < 0.001, f"Expected difference ~0.129, got {diff}"

def test_carry_score_none_graceful_handling():
    """Test carry score missing entirely doesn't crash."""
    candidate_none = {
        "token_address": "CARRY999",
        "dex_screener_score": 5.0
    }
    result = compute_final_score(candidate_none, settings={})
    assert "low_carry_potential" not in result.get("carry_warnings", [])
    assert result.get("final_score_with_orderflow") is not None

def test_lead_lag_score_and_synergy():
    """Test evaluating lead lag score limits and synergy bonus directly."""
    # 1. Test Weak Lead Lag Warning (lead_lag_score < 0.4)
    candidate_weak = {
        "token_address": "LEAD123",
        "dex_screener_score": 5.0,
        "wallet_lead_lag_sec": 140.0, 
        "multi_timeframe_confirmation": {"1m": False, "5m": False, "15m": False}
    }
    
    result_weak = compute_final_score(candidate_weak, settings={})
    assert "weak_lead_lag" in result_weak.get("wallet_lead_lag_soft_blockers", []), "Missing weak lead lag soft blocker"
    
    # 2. Test Synergy Bonus (+0.12 if LeadLag >= 0.8 and SmartMoney >= 0.7)
    candidate_synergy = {
        "token_address": "LEAD456",
        "dex_screener_score": 5.0,
        "wallet_lead_lag_sec": 22.0, 
        "multi_timeframe_confirmation": {"1m": True, "5m": True, "15m": True},
        "current_price": 100.0,
        "smart_cohort_weighted_avg_entry_price": 100.0
    }
    
    result_synergy = compute_final_score(candidate_synergy, settings={})
    assert "weak_lead_lag" not in result_synergy.get("wallet_lead_lag_soft_blockers", [])
    
    candidate_near = {
        "token_address": "LEAD789",
        "dex_screener_score": 5.0,
        "wallet_lead_lag_sec": 68.0, 
        "multi_timeframe_confirmation": {"1m": True, "5m": True, "15m": True},
        "current_price": 100.0,
        "smart_cohort_weighted_avg_entry_price": 100.0
    }
    
    result_near = compute_final_score(candidate_near, settings={})
    
    score_syn = result_synergy.get("final_score_with_orderflow", 0.0)
    score_near = result_near.get("final_score_with_orderflow", 0.0)
    
    # Difference should be synergy bonus (0.12) + diff in LeadLag (0.11 * (1.0 - 0.65)) = 0.1585
    diff = score_syn - score_near
    assert abs(diff - 0.1585) < 0.001, f"Expected difference ~0.1585, got {diff}"
