"""Friction models enforcing conservative execution constraints measuring network base fees dynamically."""

from typing import Dict, Any

def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def estimate_total_friction_bps(token_ctx: Dict[str, Any], trade_size_usd: float = 100.0) -> float:
    """Calculates operational fees AND dynamic AMM price impact."""
    base_fee = _to_float(token_ctx.get("base_fee_bps"), 5.0)
    priority_fee = _to_float(token_ctx.get("priority_fee_bps"), 10.0)
    jito_tip = _to_float(token_ctx.get("jito_tip_estimate_bps"), 15.0)
    
    # --- НОВАЯ ЛОГИКА AMM IMPACT (Price Impact) ---
    liquidity_usd = _to_float(token_ctx.get("liquidity_usd", 0.0))
    amm_impact_bps = 0.0
    
    if liquidity_usd > 0:
        # Аппроксимация сдвига цены для Constant Product AMM (x*y=k)
        # Удваиваем ликвидность в делителе, так как USD ликвидность разделена на два токена
        impact_pct = trade_size_usd / (liquidity_usd / 2)
        amm_impact_bps = impact_pct * 10000  # конвертация в bps
        
    # Каппируем скольжение на уровне 25% (2500 bps), чтобы избежать математических аномалий
    amm_impact_bps = min(amm_impact_bps, 2500.0)
        
    return base_fee + priority_fee + jito_tip + amm_impact_bps

