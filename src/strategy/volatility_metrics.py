"""Volatility compression metrics for breakout detection."""

from typing import Dict, Any, Optional

def compute_vol_compression_breakout(token_address: str, short_window: int = 5, long_window: int = 60, fetched_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Computes volatility compression metrics using local formulas:
    - Ratio = ATR_5m / (ATR_60m + 0.0001)
    - Normalizes ratio to score (0..1)
    - Checks for >8% breakout inside the fast window resolving compression.
    """
    if fetched_data is None:
        fetched_data = {}
        
    atr_5m = fetched_data.get("atr_5m")
    atr_60m = fetched_data.get("atr_60m")
    price_change_15m = fetched_data.get("price_change_15m_pct", 0.0)
    
    if atr_5m is None or atr_60m is None:
        return {
            "vol_compression_ratio": None,
            "vol_compression_score": None,
            "breakout_confirmed": False,
            "provenance": {
                "source": "simulated",
                "short_window": short_window,
                "long_window": long_window,
                "error": "missing_data"
            }
        }
        
    ratio = atr_5m / (atr_60m + 0.0001)
    
    if ratio <= 0.55:
        score = 1.0
    elif 0.55 < ratio <= 0.75:
        score = 0.65
    elif 0.75 < ratio <= 0.95:
        score = 0.3
    else:
        score = 0.0
        
    breakout = bool(price_change_15m > 8.0)
    
    return {
        "vol_compression_ratio": round(ratio, 4),
        "vol_compression_score": score,
        "breakout_confirmed": breakout,
        "provenance": {
            "source": fetched_data.get("source", "dune"),
            "short_window": short_window,
            "long_window": long_window
        }
    }
