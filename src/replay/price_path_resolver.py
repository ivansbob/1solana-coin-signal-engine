"""Resolves the raw price fetches into mathematically bounded Status properties."""

import time
from typing import Dict, Any, Optional
from src.strategy.types import PricePathEvidence

def resolve_price_path(price_data: Optional[Dict[str, Any]], target_timestamp_sec: int, source_name: str) -> PricePathEvidence:
    """
    Decodes the strict semantic state of the fetched price path.
    """
    if price_data is None:
        return {
            "price_path_status": "missing",
            "price_path_source": source_name,
            "price_path_confidence": 0.0,
            "gap_size_sec": -1,
            "backfill_applied": False,
            "price_path_diagnostic": f"{source_name}_returned_no_data"
        }
        
    actual_time = price_data.get("timestamp", target_timestamp_sec)
    gap_size = abs(target_timestamp_sec - actual_time)
    
    # Missing Date Semantics Strictly Enforced:
    if gap_size == 0:
        status = "full"
        confidence = 1.0
    elif gap_size < 300: # 5 minutes is partial
        status = "partial"
        confidence = 0.7
    else:
        status = "stale"
        confidence = 0.0
        
    backfill = price_data.get("is_backfill", False)
    if backfill:
        confidence *= 0.5 # Downgrade confidence logically
        
    return {
        "price_path_status": status,
        "price_path_source": source_name,
        "price_path_confidence": confidence,
        "gap_size_sec": gap_size,
        "backfill_applied": backfill,
        "price_path_diagnostic": f"resolved_{status}_from_{source_name}"
    }
