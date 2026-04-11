import pytest
from src.strategy.defi_carry_metrics import CarryAnalyzer

def test_high_points_and_yield():
    """Test case: high points velocity (2.8) + high yield (14%) -> expected score 1.0"""
    result = CarryAnalyzer.compute_points_restaking_carry(
        token_address="TEST",
        points_accrued_7d=28000,
        points_accrued_30d=10000,  # Velocity = 28000/10001 = 2.8
        blended_apy=0.18,          # 18% APY
        token_inflation_rate=0.02  # 2% inflation
    )
    
    # PointsVelocity = 2.8 -> PointsVelocityNorm = min(1.0, 2.8/2.5) = 1.0
    # RestakingYieldProxy = 0.18 * (1-0.02) = 0.1764 -> 17.64%
    # RestakingYieldNorm = min(1.0, max(0, (17.64-4)/12)) = min(1.0, 13.64/12) = 1.0
    # CarryTotalScore = 0.55*1.0 + 0.45*1.0 = 1.0
    
    assert result["points_carry_score"] == 1.0
    assert result["restaking_yield_proxy"] == 0.1764
    assert result["carry_total_score"] == 1.0
    assert result["carry_provenance"]["points_accrued_7d"] == 28000
    assert result["carry_provenance"]["points_accrued_30d"] == 10000

def test_medium_points_and_yield():
    """Test case: medium points velocity (1.4) + medium yield (7%) -> expected score ~0.65"""
    result = CarryAnalyzer.compute_points_restaking_carry(
        token_address="TEST",
        points_accrued_7d=14000,
        points_accrued_30d=10000,  # Velocity = 14000/10001 = 1.4
        blended_apy=0.11,          # 11% APY
        token_inflation_rate=0.02  # 2% inflation
    )
    
    # PointsVelocity = 1.4 -> PointsVelocityNorm = min(1.0, 1.4/2.5) = 0.56
    # RestakingYieldProxy = 0.11 * (1-0.02) = 0.1078 -> 10.78%
    # RestakingYieldNorm = min(1.0, max(0, (10.78-4)/12)) = 6.78/12 = 0.565
    # CarryTotalScore = 0.55*0.56 + 0.45*0.565 = 0.308 + 0.25425 = 0.56225
    
    assert abs(result["points_carry_score"] - 0.56) < 0.001
    assert abs(result["carry_total_score"] - 0.56225) < 0.001

def test_low_points_and_yield():
    """Test case: low points velocity (0.6) + low yield (3%) -> expected score ~0.2"""
    result = CarryAnalyzer.compute_points_restaking_carry(
        token_address="TEST",
        points_accrued_7d=6000,
        points_accrued_30d=10000,   # Velocity = 6000/10001 = 0.6
        blended_apy=0.05,           # 5% APY
        token_inflation_rate=0.02   # 2% inflation
    )
    
    # PointsVelocity = 0.6 -> PointsVelocityNorm = min(1.0, 0.6/2.5) = 0.24
    # RestakingYieldProxy = 0.05 * (1-0.02) = 0.049 -> 4.9%
    # RestakingYieldNorm = min(1.0, max(0, (4.9-4)/12)) = 0.9/12 = 0.075
    # CarryTotalScore = 0.55*0.24 + 0.45*0.075 = 0.132 + 0.03375 = 0.16575
    
    assert abs(result["points_carry_score"] - 0.24) < 0.001
    assert abs(result["carry_total_score"] - 0.16575) < 0.001

def test_points_only():
    """Test case: only points data provided, yield data missing"""
    result = CarryAnalyzer.compute_points_restaking_carry(
        token_address="TEST",
        points_accrued_7d=5000,
        points_accrued_30d=10000
        # blended_apy and token_inflation_rate are None
    )
    
    # PointsVelocity = 5000/10001 = 0.5 -> PointsVelocityNorm = 0.5/2.5 = 0.2
    # RestakingYieldProxy = None -> RestakingYieldNorm = 0
    # CarryTotalScore = 0.55*0.2 + 0.45*0 = 0.11
    
    assert abs(result["points_carry_score"] - 0.2) < 0.001
    assert result["restaking_yield_proxy"] is None
    assert abs(result["carry_total_score"] - 0.11) < 0.001

def test_missing_data():
    """Test case: no data provided"""
    result = CarryAnalyzer.compute_points_restaking_carry(
        token_address="TEST"
        # All parameters are None
    )
    
    # All values should be default/zero
    assert result["points_carry_score"] == 0.0
    assert result["restaking_yield_proxy"] is None
    assert result["carry_total_score"] == 0.0
    assert "error" not in result["carry_provenance"]  # No error, just missing data