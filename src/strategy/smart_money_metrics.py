"""Calculates distances from original deep smart money accumulation to prevent chasing exhaustions."""

from typing import Dict, Any
from src.strategy.types import SmartMoneyEvidence

def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
            
        return float(value)
    except (TypeError, ValueError):
        return default

def compute_smart_money_distance_metrics(token_ctx: Dict[str, Any]) -> SmartMoneyEvidence:
    current_price = _to_float(token_ctx.get("current_price"))
    smart_cohort_entry = _to_float(token_ctx.get("smart_cohort_weighted_avg_entry_price"))
    
    # 1. Distance Calculation (missing-data honest fallback)
    if smart_cohort_entry > 0 and current_price > 0:
        distance = ((current_price - smart_cohort_entry) / smart_cohort_entry) * 100.0
    else:
        # Deep penalty: Being blind to Smart Money implies we should assume we are dangerously late
        distance = 100.0 
        
    # 2. Smart Money Score Bounds
    if distance <= 32:
        dist_score = 1.0
    elif distance <= 55:
        dist_score = 0.70
    elif distance <= 80:
        dist_score = 0.35
    else:
        dist_score = 0.0
        
    # 3. Bundle Shield Evaluation
    recent_bundle_ratio = _to_float(token_ctx.get("recent_bundle_ratio"), default=0.50)
    bundle_sell_pressure = _to_float(token_ctx.get("bundle_sell_pressure"), default=0.50)
    
    # Protect against negatives via default assumptions if broken payloads occur
    if recent_bundle_ratio < 0:
        recent_bundle_ratio = 0.50
    if bundle_sell_pressure < 0:
        bundle_sell_pressure = 0.50
        
    bundle_sum = recent_bundle_ratio + bundle_sell_pressure
    bundle_pressure_score = max(0.0, 1.0 - min(1.0, bundle_sum / 1.5))
    
    # 4. Aggregation
    combined_score = (0.62 * dist_score) + (0.38 * bundle_pressure_score)
    
    return {
        "distance_from_smart_entry_pct": round(distance, 4),
        "smart_money_distance_score": round(dist_score, 4),
        "bundle_pressure_score": round(bundle_pressure_score, 4),
        "smart_money_combined_score": round(combined_score, 4),
    }

