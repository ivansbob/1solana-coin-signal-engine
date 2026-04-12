"""
Analyzes Protocol Health metrics generating strictly formatted DefiHealthEvidence structs safely parsing limits inherently.
"""

from src.strategy.types import DefiHealthEvidence

class DefiAnalyzer:
    @staticmethod
    def is_microcap_meme(liquidity: float, name: str) -> bool:
        """Determines if object inherently acts as completely void of protocol layers mapping heuristics safely."""
        name_l = name.lower()
        if liquidity < 500_000 and any(kw in name_l for kw in ["dog", "cat", "pepe", "wif", "sol", "moon", "inu", "shit"]):
            return True
        if liquidity < 100_000:
            return True # Extremely small microcap regardless of symbol
        return False
        
    @staticmethod
    def calculate_defi_health(
        liquidity: float,
        name: str,
        tvl_growth_7d: float, 
        fees_30d_annualized: float, 
        tvl: float,
        utilization_rate: float,
        smart_money_netflow_score: float
    ) -> DefiHealthEvidence:
        
        # 1. Block micro-caps preventing penalty distortions natively
        microcap = DefiAnalyzer.is_microcap_meme(liquidity, name)
        if microcap:
            return {
                "defi_health_score": 0.0,
                "tvl_trend_proxy": 0.0,
                "revenue_yield_proxy": 0.0,
                "utilization_norm": 0.0,
                "rotation_context_state": "meme_dominant",
                "defi_coverage_status": "missing",
                "is_microcap_meme": True
            }
            
        # 2. Extract mathematical dependencies tracking proxies gracefully.
        # Fallback implicitly handling division over zeroes mapping assumptions cleanly.
        try:
            tvl_trend_norm = min(1.0, max(0.0, tvl_growth_7d / 0.25))
            
            revenue_yield_proxy = 0.0
            if tvl > 0:
                revenue_yield_proxy = min(1.0, max(0.0, (fees_30d_annualized / tvl) / 0.20))
                
            utilization_norm = min(1.0, max(0.0, (utilization_rate - 0.50) / 0.35))
            smart_money_norm = min(1.0, max(0.0, smart_money_netflow_score))
            
            health = (
                0.35 * tvl_trend_norm +
                0.30 * revenue_yield_proxy +
                0.20 * utilization_norm +
                0.15 * smart_money_norm
            )
            
            ctx = "defi_rotation" if health > 0.55 else "neutral"
            status = "full" if tvl > 0 and utilization_rate > 0 else "partial"
            
            return {
                "defi_health_score": health,
                "tvl_trend_proxy": tvl_trend_norm,
                "revenue_yield_proxy": revenue_yield_proxy,
                "utilization_norm": utilization_norm,
                "rotation_context_state": ctx,
                "defi_coverage_status": status,
                "is_microcap_meme": False
            }
            
        except Exception:
            # Full failure degraded mode isolating metrics completely
            return {
                "defi_health_score": 0.0,
                "tvl_trend_proxy": 0.0,
                "revenue_yield_proxy": 0.0,
                "utilization_norm": 0.0,
                "rotation_context_state": "neutral",
                "defi_coverage_status": "missing",
                "is_microcap_meme": False
            }
