"""Computes Wallet Lead-Lag metrics for detecting temporal lead in smart wallet purchases."""

from typing import Dict, Any, Optional

def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def compute_wallet_lead_lag_metrics(token_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Computes wallet lead-lag metrics based on temporal differences between leader and follower wallets.

    Leaders: wallets with historical win-rate >= 65%
    LeadLagSec: average time difference between leader buys and follower buys
    """
    # Assume these fields are populated from Dune query or cache
    lead_lag_sec = _to_float(token_ctx.get("wallet_lead_lag_sec"), default=0.0)
    multi_tf_confirmation = token_ctx.get("multi_timeframe_confirmation", {})  # dict with '1m', '5m', '15m' bools

    # LeadLagScore based on lag
    if 8 <= lead_lag_sec <= 45:
        lead_lag_score = 1.0
    elif 45 < lead_lag_sec <= 90:
        lead_lag_score = 0.65
    elif 90 < lead_lag_sec <= 180:
        lead_lag_score = 0.3
    else:
        lead_lag_score = 0.0  # Too fast (sybil) or too slow

    # Multi-Timeframe Confirmation Score
    tf_confirmations = [
        multi_tf_confirmation.get('1m', False),
        multi_tf_confirmation.get('5m', False),
        multi_tf_confirmation.get('15m', False)
    ]
    confirmed_count = sum(tf_confirmations)

    if confirmed_count == 3:
        multi_tf_score = 1.0
    elif confirmed_count == 2:
        multi_tf_score = 0.6
    elif confirmed_count >= 1:
        multi_tf_score = 0.2
    else:
        multi_tf_score = 0.0

    # Provenance
    provenance = f"lag={lead_lag_sec:.1f}s, tf_confirm={confirmed_count}/3"

    return {
        "wallet_lead_lag_sec": round(lead_lag_sec, 2),
        "lead_lag_score": round(lead_lag_score, 4),
        "multi_timeframe_confirmation_score": round(multi_tf_score, 4),
        "lead_lag_provenance": provenance
    }