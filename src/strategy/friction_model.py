"""Friction models enforcing conservative execution constraints measuring network base fees dynamically."""

from typing import Dict, Any

def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def estimate_total_friction_bps(token_ctx: Dict[str, Any]) -> float:
    """Calculates all associated pipeline operational fees natively as basis points."""
    base_fee = _to_float(token_ctx.get("base_fee_bps"))
    priority_fee = _to_float(token_ctx.get("priority_fee_bps"))
    jito_tip = _to_float(token_ctx.get("jito_tip_estimate_bps"))
    
    # Missing data logic - missing network variables MUST be evaluated defensively
    if base_fee < 0:
        base_fee = 5.0 # Basic Solana operational bounds
    if priority_fee < 0:
        priority_fee = 10.0 # Standard network saturation
    if jito_tip < 0:
        jito_tip = 15.0 # Typical Jito tip for fast execution security
        
    return base_fee + priority_fee + jito_tip

