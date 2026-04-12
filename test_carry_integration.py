#!/usr/bin/env python3

"""Quick test to verify carry score integration with scoring_vX"""

from src.strategy.defi_carry_metrics import CarryAnalyzer

def test_carry_score_integration():
    """Test that carry score computes correctly and can be integrated"""
    
    # Test high carry scenario
    result = CarryAnalyzer.compute_points_restaking_carry(
        token_address="test_token",
        points_accrued_7d=280.0,
        points_accrued_30d=100.0,
        blended_apy=0.20,
        token_inflation_rate=0.05
    )
    
    print(f"High carry test:")
    print(f"  Points carry score: {result['points_carry_score']}")
    print(f"  Restaking yield proxy: {result['restaking_yield_proxy']}")
    print(f"  Carry total score: {result['carry_total_score']}")
    print(f"  Expected: 1.0, Got: {result['carry_total_score']}")
    
    assert abs(result['carry_total_score'] - 1.0) < 0.001, f"Expected ~1.0, got {result['carry_total_score']}"
    
    # Test medium carry scenario
    result2 = CarryAnalyzer.compute_points_restaking_carry(
        token_address="test_token2",
        points_accrued_7d=70.0,
        points_accrued_30d=50.0,
        blended_apy=0.12,
        token_inflation_rate=0.03
    )
    
    print(f"\nMedium carry test:")
    print(f"  Points carry score: {result2['points_carry_score']}")
    print(f"  Restaking yield proxy: {result2['restaking_yield_proxy']}")
    print(f"  Carry total score: {result2['carry_total_score']}")
    print(f"  Expected: ~0.59, Got: {result2['carry_total_score']}")
    
    assert abs(result2['carry_total_score'] - 0.59) < 0.01, f"Expected ~0.59, got {result2['carry_total_score']}"
    
    # Test that the fields exist for CandidateSnapshot
    candidate = {
        "token_address": "test",
        "symbol": "TEST",
        "points_carry_score": result['points_carry_score'],
        "restaking_yield_proxy": result['restaking_yield_proxy'],
        "carry_total_score": result['carry_total_score'],
        "carry_provenance": result['carry_provenance']
    }
    
    print(f"\nCandidate snapshot fields present:")
    print(f"  points_carry_score: {candidate['points_carry_score']}")
    print(f"  restaking_yield_proxy: {candidate['restaking_yield_proxy']}")
    print(f"  carry_total_score: {candidate['carry_total_score']}")
    print(f"  carry_provenance keys: {list(candidate['carry_provenance'].keys())}")
    
    print("\n✅ All integration tests passed!")
    return True

if __name__ == "__main__":
    test_carry_score_integration()