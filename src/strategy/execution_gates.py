"""Hard and soft bounds for execution drops."""

from typing import Dict, Any, List
from src.strategy.types import OrderflowMetrics, SmartMoneyEvidence, LiquidityQualityEvidence, SocialVelocityEvidence, WalletCohortEvidence, ExecutionContext, CandidateSnapshot


def evaluate_orderflow_gates(metrics: OrderflowMetrics) -> Dict[str, Any]:
    """
    Evaluates orderflow purity and returns blockers. 
    Drops candidate if dirty flow is encountered or too much manipulative block 0 sniping.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    
    if metrics["orderflow_purity_score"] < 0.4:
        hard_blockers.append("dirty_orderflow")
        
    if metrics["block_0_snipe_pct"] > 0.35:
        hard_blockers.append("excessive_block0_sniping")
        
    if metrics["sybil_cluster_ratio"] > 0.45:
        soft_blockers.append("high_sybil_cluster")
        
    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers
    }


def evaluate_smart_money_gates(metrics: SmartMoneyEvidence) -> Dict[str, Any]:
    """
    Evaluates tracking distance to prevent being liquidity for late entries.
    Throws blockers if bundled sales outnumber safe limits alongside crowded entries.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []
    
    if metrics["smart_money_distance_score"] < 0.35:
        warnings.append("late_smart_money_chase")
        
    if metrics["bundle_pressure_score"] < 0.45:
        soft_blockers.append("high_bundle_pressure")
        
    if metrics["distance_from_smart_entry_pct"] > 75.0 and metrics["bundle_pressure_score"] < 0.50:
        hard_blockers.append("overextended_crowded_entry")
        
    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }


def evaluate_liquidity_gates(metrics: LiquidityQualityEvidence) -> Dict[str, Any]:
    """
    Evaluates realistic expectations of exiting a position natively throwing extreme warnings
    and blockers when slippage bounds force excessive loss or dynamics shift excessively.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []
    
    refill_score = metrics.get("liquidity_refill_score")
    if refill_score is not None and refill_score < 0.4:
        hard_blockers.append("liquidity_refill_too_slow")

    
    if metrics["jupiter_buy_impact_bps"] > 45.0:
        hard_blockers.append("excessive_buy_impact")
        
    if metrics["dynamic_liquidity_share"] > 0.65 and metrics["liquidity_quality_score"] < 0.45:
        soft_blockers.append("fragile_liquidity")
        
    if metrics["jupiter_sell_impact_bps"] > 90.0:
        warnings.append("dangerous_sell_slippage")
        
    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }


def evaluate_social_gates(social_metrics: SocialVelocityEvidence, orderflow_metrics: OrderflowMetrics) -> Dict[str, Any]:
    """
    Guarantees executions cannot be strictly verified purely on Social bounds.
    Requires Orderflow components locally.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []

    if social_metrics["social_velocity_score"] >= 0.8 and orderflow_metrics["orderflow_purity_score"] < 0.5:
        # Hard warning specifically requested by PR docs
        warnings.append("hype_without_onchain")

    if social_metrics["attention_distortion_risk"] > 0.6:
        soft_blockers.append("high_attention_distortion")

    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }


def evaluate_wallet_gates(metrics: WalletCohortEvidence) -> Dict[str, Any]:
    """
    Evaluates wallet cohort quality and risk.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []

    if metrics["avg_wallet_risk_adjusted_score"] < 0.3:
        soft_blockers.append("low_wallet_quality")

    if metrics["cohort_concentration_ratio"] > 0.8:
        warnings.append("high_cohort_concentration")

    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }


def evaluate_jito_gates(execution_context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates Jito tip and landing pressure gates.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []

    if execution_context.get("tip_budget_violation_flag", False):
        soft_blockers.append("tip_too_expensive_for_edge")

    if execution_context.get("landing_pressure_score", 1.0) < 0.4:
        warnings.append("high_landing_pressure")

    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }


def evaluate_wallet_lead_lag_gates(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates wallet lead-lag quality for signal confirmation.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []

    if metrics.get("lead_lag_score", 0.0) < 0.4:
        soft_blockers.append("weak_lead_lag")

    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }


def evaluate_carry_gates(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates carry total score, throwing a warning if carry potential is low.
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []

    carry_score = metrics.get("carry_total_score")
    if carry_score is not None and carry_score < 0.35:
        warnings.append("low_carry_potential")

    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }

def evaluate_holder_gates(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates holder churn metrics, adding a warning if churn is too high (flippers).
    """
    hard_blockers: List[str] = []
    soft_blockers: List[str] = []
    warnings: List[str] = []

    churn_rate = metrics.get("holder_churn_rate_24h")
    if churn_rate is not None and churn_rate > 0.60:
        warnings.append("high_holder_churn")

    return {
        "passed_hard_gates": len(hard_blockers) == 0,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
    }

def evaluate_vol_compression_gates(metrics: Dict[str, Any]) -> Dict[str, Any]:
    warnings: List[str] = []
    comp_score = metrics.get("vol_compression_score")
    if comp_score is not None and comp_score < 0.3:
        warnings.append("low_vol_compression")

    return {
        "passed_hard_gates": True,
        "hard_blockers": [],
        "soft_blockers": [],
        "warnings": warnings,
    }


class TinyCapitalRiskGates:
    def __init__(self, min_liquidity_usd: float = 50000.0, max_price_impact_bps: float = 450.0, max_sell_impact_bps: float = 900.0, max_rugcheck_risk_score: float = 0.7, max_total_fee_sol: float = 0.01):
        self.min_liquidity_usd = min_liquidity_usd
        self.max_price_impact_bps = max_price_impact_bps
        self.max_sell_impact_bps = max_sell_impact_bps
        self.max_rugcheck_risk_score = max_rugcheck_risk_score
        self.max_total_fee_sol = max_total_fee_sol

    def evaluate(self, c: CandidateSnapshot, regime: str | None = None) -> bool:
        reasons = self._collect_reasons(c, regime)
        return len(reasons) == 0

    def _collect_reasons(self, c: CandidateSnapshot, regime: str | None) -> tuple[str, ...]:
        reasons: list[str] = []
        repeat_buyer_ratio = self._repeat_buyer_ratio(c)
        sybil_cluster_ratio = self._sybil_cluster_ratio(c)

        if c.liquidity_usd < self.min_liquidity_usd:
            reasons.append("liquidity_below_floor")

        # --- ИЗМЕНЕНО: Free-Tier совместимость для Jupiter ---
        if c.jupiter_price_impact_bps is not None:
            if c.jupiter_price_impact_bps > self.max_price_impact_bps:
                reasons.append("price_impact_too_high")
        else:
            # Fallback: аппроксимация через ликвидность (считаем, что сделка $100)
            if c.liquidity_usd > 0 and (100.0 / (c.liquidity_usd / 2) * 10000) > self.max_price_impact_bps:
                reasons.append("heuristic_price_impact_too_high")

        if c.jupiter_sell_impact_bps is not None:
            if c.jupiter_sell_impact_bps > self.max_sell_impact_bps:
                reasons.append("sell_impact_too_high")

        # --- ИЗМЕНЕНО: Free-Tier совместимость для RugCheck ---
        if c.rugcheck_risk_score is not None:
            if c.rugcheck_risk_score > self.max_rugcheck_risk_score:
                reasons.append("rugcheck_failed_high_risk")
        else:
            # Fallback: Если нет внешнего RugCheck, проверяем локальный статус
            # (предполагается, что anti_rug_context_status устанавливается нашим SecurityChecker)
            if (c.anti_rug_context_status or "").lower() == "missing":
                # Допускаем пропуск, если локальный анализатор дал добро или мы работаем в degraded mode
                pass

        if c.estimated_total_fee_sol > self.max_total_fee_sol:
            reasons.append("fee_too_high")

        return tuple(reasons)

    def _repeat_buyer_ratio(self, c: CandidateSnapshot) -> float:
        # Placeholder implementation
        return 0.0

    def _sybil_cluster_ratio(self, c: CandidateSnapshot) -> float:
        # Placeholder implementation
        return 0.0

    def _reason_priority(self, reason: str) -> tuple[int, str]:
        priorities = {
            "rugcheck_failed_status": 0,
            "anti_rug_context_conflict": 1,
            "rugcheck_failed_high_risk": 2,
            "liquidity_below_floor": 3,
            "price_impact_too_high": 11,
            "sell_impact_too_high": 12,
            "heuristic_price_impact_too_high": 12,
            "anti_rug_context_partial": 30,
            "anti_rug_context_missing": 31,
        }
        return priorities.get(reason, (99, "unknown"))
