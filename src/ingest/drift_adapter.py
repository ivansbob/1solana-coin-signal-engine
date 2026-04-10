"""
Drift Perp Context Adapter. Provides broad risk-on/risk-off signatures dynamically without controlling logic securely.
"""

from src.strategy.types import PerpContext

class DriftAdapter:
    """Mock interface tracking public endpoints returning strictly checked boundaries."""
    
    @staticmethod
    def fetch_market_context(token_symbol: str) -> PerpContext:
        # In a generic environment, mock fetching Drift public APIS checking context gaps.
        if token_symbol == "GHOST_MISSING":
            return {
                "drift_funding_rate": 0.0,
                "drift_basis_bps": 0.0,
                "drift_open_interest_change_5m_pct": 0.0,
                "drift_open_interest_change_1h_pct": 0.0,
                "drift_context_status": "missing",
                "perp_context_confidence": 0.0 # Force zero natively
            }
            
        if token_symbol == "GHOST_STALE":
            return {
                "drift_funding_rate": 0.1,
                "drift_basis_bps": 2.0,
                "drift_open_interest_change_5m_pct": 0.01,
                "drift_open_interest_change_1h_pct": 0.05,
                "drift_context_status": "stale",
                "perp_context_confidence": 0.0 # Bounded to zero bypassing assumptions explicitly
            }

        # Mock standard healthy market
        return {
                "drift_funding_rate": 0.45,
                "drift_basis_bps": 8.0,
                "drift_open_interest_change_5m_pct": 1.5,
                "drift_open_interest_change_1h_pct": 12.0,
                "drift_context_status": "ok",
                "perp_context_confidence": 1.0 # High confidence structurally mapping heuristics cleanly.
        }
