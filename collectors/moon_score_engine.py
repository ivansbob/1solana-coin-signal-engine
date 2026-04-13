from typing import Dict, Any, List
from datetime import datetime, timezone

from config.settings import Settings


def calculate_moon_score(token: Dict[str, Any]) -> Dict[str, Any]:
    """
    Продвинутый Heuristic Moon Score (0-100)
    Основан на реальных предикторах успеха токенов 2025-2026:
    - Pump.fun graduation dynamics
    - Liquidity + Buyer velocity
    - GitHub traction (из PR-3)
    - Risk & distribution quality
    - Continuation signals
    """
    score = 0.0
    reasons: List[str] = []
    risk_flags: List[str] = []

    # ====================== 1. PUMP.FUN GRADUATION (самый сильный сигнал) ======================
    if token.get("source") == "pump_fun":
        curve = float(token.get("curve_progress_pct", 0))
        if curve >= 95:
            score += 42
            reasons.append("Raydium Graduation Imminent (95%+)")
        elif curve >= 85:
            score += 28
            reasons.append("Strong Bonding Curve Progress")
        elif curve >= 70:
            score += 12
            reasons.append("Approaching Graduation")

        buyer_vel = float(token.get("buyer_velocity", 0))
        if buyer_vel > 35:
            score += 22
            reasons.append("Extreme Buyer Velocity")
        elif buyer_vel > 18:
            score += 14
            reasons.append("Strong Buyer Momentum")

    # ====================== 2. LIQUIDITY & ON-CHAIN VELOCITY ======================
    liq_vel_1h = float(token.get("liquidity_velocity_1h", 0))
    if liq_vel_1h > 120:
        score += 18
        reasons.append("Explosive Liquidity Inflow")
    elif liq_vel_1h > 60:
        score += 11
        reasons.append("Strong Liquidity Buildup")

    unique_buyers = int(token.get("unique_buyers_1h", 0))
    if unique_buyers > 80:
        score += 16
        reasons.append("High Unique Buyer Participation")
    elif unique_buyers > 35:
        score += 9
        reasons.append("Decent Buyer Diversity")

    # ====================== 3. GITHUB VELOCITY CROSS-SIGNAL (PR-3) ======================
    github_vel = float(token.get("combined_velocity_score", 0))
    if github_vel > 25:
        score += 19
        reasons.append("High GitHub Velocity Match")
    elif github_vel > 12:
        score += 10
        reasons.append("Moderate GitHub Traction")

    agentic_potential = float(token.get("agentic_potential", 0))
    if agentic_potential > 45:
        score += 14
        reasons.append("Strong Agentic / AI Potential")

    # ====================== 4. RISK & DISTRIBUTION FILTERS ======================
    dev_sell = float(token.get("dev_sell_pressure_5m", 0))
    if dev_sell > 0.12:
        score -= 25
        risk_flags.append("High Dev Sell Pressure")
    elif dev_sell < 0.03:
        score += 8
        reasons.append("Clean Dev Wallet")

    holder_conc = float(token.get("holder_concentration", 0.65))
    if holder_conc > 0.78:
        score -= 18
        risk_flags.append("High Holder Concentration")
    elif holder_conc < 0.42:
        score += 12
        reasons.append("Healthy Holder Distribution")

    # ====================== 5. FINAL ADJUSTMENTS ======================
    # Age bonus для очень молодых токенов с сильными сигналами
    age_hours = float(token.get("age_hours", 24))
    if age_hours < 6 and score > 50:
        score += 10
        reasons.append("Very Early Stage + Strong Signals")

    # Evidence quality penalty
    evidence_coverage = float(token.get("evidence_coverage_ratio", 0.7))
    if evidence_coverage < 0.5:
        score *= 0.75
        risk_flags.append("Low Evidence Coverage")

    final_score = round(max(0, min(100, score)), 1)

    token.update({
        "moon_score": final_score,
        "moon_reasons": reasons,
        "moon_risk_flags": risk_flags,
        "moon_confidence": "high" if final_score >= 75 else ("medium" if final_score >= 45 else "low"),
        "scored_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "moon_provenance": "heuristic_moon_score_engine_2026"
    })

    return token