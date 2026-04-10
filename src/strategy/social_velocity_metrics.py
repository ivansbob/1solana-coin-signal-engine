"""Computes proxy dynamics identifying hype-fueled distortions versus clean community growth."""

from typing import Dict, Any
from src.strategy.types import SocialVelocityEvidence

def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def compute_social_velocity_metrics(token_ctx: Dict[str, Any]) -> SocialVelocityEvidence:
    vel_10m = _to_float(token_ctx.get("social_velocity_10m"))
    vel_60m = _to_float(token_ctx.get("social_velocity_60m"))
    
    paid_influencer = _to_float(token_ctx.get("paid_influencer_proxy"), default=0.0)
    bot_activity = _to_float(token_ctx.get("bot_like_activity"), default=0.0)
    organic_orderflow = _to_float(token_ctx.get("organic_orderflow_alignment"), default=1.0)

    # Missing Data Logic: Fallback to highly neutral positions scoring strictly 0 impacts overall
    if vel_10m < 0 or vel_60m < 0:
        vel_10m = 0.0
        vel_60m = 0.0
        accel_ratio = 1.0 # Force neutral path giving 0 scoring
        distortion_risk = 0.5 # Neutral penalty
    else:
        # Acceleration bounded by +5 base denominator guarding division by zero
        accel_ratio = vel_10m / (vel_60m + 5.0)
        distortion_risk = (0.4 * paid_influencer) + (0.3 * bot_activity) + (0.3 * (1.0 - organic_orderflow))
        
    distortion_risk = min(1.0, max(0.0, distortion_risk))
    
    # 3. Validation Bounds
    confirm_on_chain = bool(token_ctx.get("on_chain_confirmation_strong", organic_orderflow > 0.6))
    
    if accel_ratio >= 3.5 and confirm_on_chain and distortion_risk <= 0.6:
        score = 1.0
    elif 2.0 <= accel_ratio < 3.5 and distortion_risk <= 0.6:
        score = 0.65
    elif 1.3 <= accel_ratio < 2.0 and distortion_risk <= 0.6:
        score = 0.25
    else:
        # Heavily penalizes drops where bots or influencers warp momentum beyond safety bounds
        score = 0.0
        
    return {
        "social_velocity_10m": round(vel_10m, 2),
        "social_velocity_60m": round(vel_60m, 2),
        "social_acceleration_ratio": round(accel_ratio, 4),
        "attention_distortion_risk": round(distortion_risk, 4),
        "social_velocity_score": round(score, 4)
    }

