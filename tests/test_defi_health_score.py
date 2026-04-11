import pytest
from src.strategy.defi_health_metrics import DefiAnalyzer

def test_healthy_defi_protocol_gets_high_health_score():
    health = DefiAnalyzer.calculate_defi_health(
        liquidity=5_000_000,
        name="Jito",
        tvl_growth_7d=0.30,
        fees_30d_annualized=150_000_000,
        tvl=300_000_000,
        utilization_rate=0.85,
        smart_money_netflow_score=0.9
    )
    assert not health["is_microcap_meme"]
    assert health["defi_health_score"] > 0.8
    assert health["rotation_context_state"] == "defi_rotation"

def test_weak_revenue_and_tvl_gets_low_score():
    health = DefiAnalyzer.calculate_defi_health(
        liquidity=2_000_000,
        name="DeadProtocol",
        tvl_growth_7d=-0.10,
        fees_30d_annualized=0,
        tvl=50_000,
        utilization_rate=0.10,
        smart_money_netflow_score=0.1
    )
    assert not health["is_microcap_meme"]
    assert health["defi_health_score"] < 0.35 # Hits weak properties limit
    assert health["rotation_context_state"] == "neutral"

def test_microcap_ignores_defi_health_score():
    health = DefiAnalyzer.calculate_defi_health(
        liquidity=50_000, # Extremely tiny implicitly marking meme boundaries functionally
        name="cat_dog_moon_inu",
        tvl_growth_7d=0.0,
        fees_30d_annualized=0.0,
        tvl=0.0,
        utilization_rate=0.0,
        smart_money_netflow_score=0.0
    )
    assert health["is_microcap_meme"] is True
    assert health["defi_health_score"] == 0.0 # Forcefully defaults zeros
    assert health["rotation_context_state"] == "meme_dominant"

def test_missing_defi_data_degrades_gracefully():
    health = DefiAnalyzer.calculate_defi_health(
        liquidity=6_000_000,
        name="Jupiter",
        tvl_growth_7d=0.0,
        fees_30d_annualized=1000,
        tvl=0.0, # Division over zero handling implicitly protecting
        utilization_rate=0.0,
        smart_money_netflow_score=0.5
    )
    assert health["revenue_yield_proxy"] == 0.0
    assert health["defi_coverage_status"] == "partial"
