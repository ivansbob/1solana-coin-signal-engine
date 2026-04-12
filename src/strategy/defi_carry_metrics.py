"""
Analyzes Points / Restaking Carry Score metrics generating strictly formatted carry evidence structs safely parsing limits inherently.
"""

from typing import Dict, Any, Optional


class CarryAnalyzer:
    @staticmethod
    def compute_points_restaking_carry(
        token_address: str,
        points_accrued_7d: Optional[float] = None,
        points_accrued_30d: Optional[float] = None,
        blended_apy: Optional[float] = None,
        token_inflation_rate: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Computes Points / Restaking Carry Score for a token.
        
        Returns dict with:
        - points_velocity: velocity of points accrual
        - restaking_yield_proxy: adjusted yield proxy
        - carry_total_score: combined carry score (0..1)
        - carry_provenance: dict with source data and calculations
        """
        
        try:
            # Handle missing data gracefully
            if points_accrued_7d is None or points_accrued_30d is None:
                points_velocity = None
            else:
                # PointsVelocity = points_accrued_7d / (points_accrued_30d + 1)
                points_velocity = points_accrued_7d / (points_accrued_30d + 1)
            
            if blended_apy is None or token_inflation_rate is None:
                restaking_yield_proxy = None
            else:
                # RestakingYieldProxy = blended_apy * (1 - token_inflation_rate)
                restaking_yield_proxy = blended_apy * (1 - token_inflation_rate)
            
            # Normalize components
            points_velocity_norm = 0.0
            if points_velocity is not None:
                # min(1.0, points_velocity / 2.5)
                points_velocity_norm = min(1.0, points_velocity / 2.5)
            
            restaking_yield_norm = 0.0
            if restaking_yield_proxy is not None:
                # Convert to percentage for normalization (yield proxy is in decimal form)
                restaking_yield_proxy_pct = restaking_yield_proxy * 100.0
                # min(1.0, max(0, (restaking_yield_proxy_pct - 4) / 12))
                restaking_yield_norm = min(1.0, max(0.0, (restaking_yield_proxy_pct - 4.0) / 12.0))
            
            # CarryTotalScore = 0.55 * PointsVelocityNorm + 0.45 * RestakingYieldNorm
            carry_total_score = 0.55 * points_velocity_norm + 0.45 * restaking_yield_norm
            
            # Ensure bounds
            carry_total_score = min(1.0, max(0.0, carry_total_score))
            
            provenance = {
                "points_accrued_7d": points_accrued_7d,
                "points_accrued_30d": points_accrued_30d,
                "blended_apy": blended_apy,
                "token_inflation_rate": token_inflation_rate,
                "points_velocity_raw": points_velocity,
                "restaking_yield_proxy_raw": restaking_yield_proxy,
                "points_velocity_norm": points_velocity_norm,
                "restaking_yield_norm": restaking_yield_norm,
                "calculation_timestamp": None,  # Would be set by caller
                "data_source": "dune_defillama"
            }
            
            return {
                "points_carry_score": points_velocity_norm,
                "restaking_yield_proxy": restaking_yield_proxy,
                "carry_total_score": carry_total_score,
                "carry_provenance": provenance
            }
            
        except Exception as e:
            # Full failure degraded mode isolating metrics completely
            return {
                "points_carry_score": 0.0,
                "restaking_yield_proxy": None,
                "carry_total_score": 0.0,
                "carry_provenance": {
                    "error": str(e),
                    "data_source": "failed"
                }
            }