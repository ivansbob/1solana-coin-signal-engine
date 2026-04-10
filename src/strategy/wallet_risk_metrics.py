"""Computes robust historical performance limits ensuring wallet clusters function responsibly natively."""

from typing import Dict, Any
from src.strategy.types import WalletCohortEvidence

def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def compute_risk_adjusted_wallet_score(token_ctx: Dict[str, Any]) -> WalletCohortEvidence:
    sharpe = _to_float(token_ctx.get("avg_sharpe_90d"))
    sortino = _to_float(token_ctx.get("avg_sortino_90d"))
    pf = _to_float(token_ctx.get("avg_profit_factor"))
    mdd = _to_float(token_ctx.get("avg_max_drawdown_90d"))
    
    # Missing wallet data logic heavily degrading values guaranteeing algorithms never trust blanks
    if sharpe < -50.0 or sortino < -50.0 or pf < -50.0 or mdd < -50.0:
        # A missing data drop
        sharpe = 0.0
        sortino = 0.0
        pf = 1.0 # 1.0 breaks even
        mdd = 0.80 # 80% default drawdown forces absolute horror on scoring drops natively
    else:
        if sharpe < 0: sharpe = 0.0
        if sortino < 0: sortino = 0.0
        if pf < 0: pf = 1.0
        if mdd < 0: mdd = 0.80
        
    sharpe_norm = min(1.0, max(0.0, (sharpe - 0.5) / 2.5))
    sortino_norm = min(1.0, max(0.0, (sortino - 0.8) / 3.0))
    pf_norm = min(1.0, max(0.0, (pf - 1.2) / 2.0))
    mdd_penalty = max(0.0, (mdd - 0.35) / 0.40)
    
    # 0 max floors to prevent negative leakage 
    risk_adjusted = max(0.0, (
        (0.45 * sharpe_norm) +
        (0.30 * sortino_norm) +
        (0.15 * pf_norm) -
        (0.25 * mdd_penalty)
    ))
    
    concentration = _to_float(token_ctx.get("cohort_concentration_ratio"), default=0.8)
    if concentration < 0:
        concentration = 0.8
        
    quality = (0.60 * risk_adjusted) + (0.40 * max(0.0, 1.0 - concentration))
    
    family_mult = _to_float(token_ctx.get("family_qualifier_multiplier"), default=1.0)
    if family_mult < 0:
        family_mult = 1.0
        
    confidence = risk_adjusted * quality * family_mult
    
    return {
        "avg_sharpe_90d": round(sharpe, 4),
        "avg_sortino_90d": round(sortino, 4),
        "avg_profit_factor": round(pf, 4),
        "avg_max_drawdown_90d": round(mdd, 4),
        "avg_wallet_risk_adjusted_score": round(risk_adjusted, 4),
        "cohort_concentration_ratio": round(concentration, 4),
        "family_qualifier_multiplier": round(family_mult, 4),
        "wallet_signal_confidence": round(confidence, 4)
    }

