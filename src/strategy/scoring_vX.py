"""Orchestrator to hook regime router and metrics before final score."""

from typing import Mapping, Dict, Any, Optional
from config.settings import load_settings
from src.strategy.regime_classifier import decide_regime
from src.strategy.orderflow_purity_metrics import compute_orderflow_purity_metrics
from src.strategy.orderflow_metrics import compute_cumulative_delta_divergence
from src.strategy.execution_gates import (
    evaluate_orderflow_gates,
    evaluate_smart_money_gates,
    evaluate_liquidity_gates,
    evaluate_social_gates,
    evaluate_wallet_gates,
    evaluate_jito_gates,
    evaluate_wallet_lead_lag_gates,
    evaluate_carry_gates,
)
from src.strategy.smart_money_metrics import compute_smart_money_distance_metrics
from src.strategy.liquidity_metrics import compute_liquidity_quality_metrics, compute_liquidity_refill_half_life
from src.strategy.exit_manager import evaluate_net_executable_pnl

from src.strategy.social_velocity_metrics import compute_social_velocity_metrics
from src.strategy.narrative_metrics import compute_narrative_velocity
from src.strategy.wallet_risk_metrics import compute_risk_adjusted_wallet_score
from src.strategy.wallet_lead_lag_metrics import compute_wallet_lead_lag_metrics
from src.strategy.volatility_metrics import compute_vol_compression_breakout
from src.strategy.holder_metrics import compute_holder_churn_metrics


from src.ingest.jito_priority_context import JitoPriorityContextAdapter
from src.paper.landing_pressure_sim import LandingPressureSimulator


def augment_token_with_regime(
    token_ctx: Mapping[str, Any], settings: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Applies the declarative regime rules directly to the given token context.
    Returns a new dictionary containing the original context updated with the RegimeDecision outputs.
    """
    token_dict = dict(token_ctx)
    if settings is None:
        settings = load_settings()

    decision = decide_regime(token_dict, settings)

    token_dict["regime_decision"] = decision["regime"]
    token_dict["regime_confidence"] = decision["confidence"]
    token_dict["expected_hold_class"] = decision["expected_hold_class"]
    token_dict["regime_reason"] = decision["reason"]
    token_dict["regime_reason_flags"] = decision["reason_flags"]
    token_dict["regime_warnings"] = decision["warnings"]
    token_dict["regime_blockers"] = decision["blockers"]

    return token_dict


def compute_final_score(
    token_ctx: Mapping[str, Any], settings: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Computes final TotalScore appending orderflow purity weighting.
    """
    token_dict = dict(token_ctx)
    if settings is None:
        settings = load_settings()

    orderflow_metrics = compute_orderflow_purity_metrics(token_dict)
    cum_delta_metrics = compute_cumulative_delta_divergence(
        token_dict.get("token_address", "")
    )
    order_gates = evaluate_orderflow_gates(orderflow_metrics)

    smart_metrics = compute_smart_money_distance_metrics(token_dict)
    smart_gates = evaluate_smart_money_gates(smart_metrics)

    liq_metrics = compute_liquidity_quality_metrics(token_dict)
    
    refill_metrics = compute_liquidity_refill_half_life(
        token_dict.get("token_address", ""),
        fetched_data=token_dict.get("raw_liquidity_refill_data")
    )
    # Combine so evaluate_liquidity_gates can see it
    liq_metrics.update(refill_metrics)
    
    liq_gates = evaluate_liquidity_gates(liq_metrics)


    soc_metrics = compute_social_velocity_metrics(token_dict)
    soc_gates = evaluate_social_gates(soc_metrics, orderflow_metrics)

    narrative_metrics = compute_narrative_velocity(token_dict.get("token_address", ""))

    wallet_metrics = compute_risk_adjusted_wallet_score(token_dict)
    wallet_gates = evaluate_wallet_gates(wallet_metrics)

    wallet_lead_lag_metrics = compute_wallet_lead_lag_metrics(token_dict)
    wallet_lead_lag_gates = evaluate_wallet_lead_lag_gates(wallet_lead_lag_metrics)

    carry_gates = evaluate_carry_gates(token_dict)

    vol_compression_metrics = compute_vol_compression_breakout(
        token_dict.get("token_address", ""),
        fetched_data=token_dict.get("raw_vol_compression_data")
    )
    from src.strategy.execution_gates import evaluate_vol_compression_gates, evaluate_holder_gates
    vol_comp_gates = evaluate_vol_compression_gates(vol_compression_metrics)
    token_dict.update(vol_compression_metrics)
    
    holder_metrics = compute_holder_churn_metrics(
        token_dict.get("token_address", ""),
        fetched_data=token_dict.get("raw_holder_churn_data")
    )
    holder_gates = evaluate_holder_gates(holder_metrics)
    token_dict.update(holder_metrics)




    # Compute Jito Priority Context
    jito_adapter = JitoPriorityContextAdapter(settings.get("jito_priority", {}))
    jito_context = jito_adapter.build_jito_context(token_dict)
    jito_context_dict = {
        "priority_lane": jito_context.priority_lane,
        "congestion_level": jito_context.congestion_level,
        "recent_failed_tx_rate": jito_context.recent_failed_tx_rate,
        "base_tip_lamports": jito_context.base_tip_lamports,
        "dynamic_tip_target_lamports": jito_context.dynamic_tip_target_lamports,
        "tip_efficiency_score": jito_context.tip_efficiency_score,
        "landing_pressure_score": jito_context.landing_pressure_score,
        "tip_budget_violation_flag": jito_context.tip_budget_violation_flag,
        "estimated_landing_improvement_pct": jito_context.estimated_landing_improvement_pct,
        "timestamp_sec": jito_context.timestamp_sec,
    }

    # Compute Jito Gates
    jito_gates = evaluate_jito_gates(jito_context_dict)

    # Compute Landing Pressure Simulation
    landing_sim = LandingPressureSimulator(settings.get("landing_simulation", {}))
    landing_result = landing_sim.simulate_landing_pressure(jito_context)
    landing_evidence = {
        "success_rate": landing_result.success_rate,
        "average_landing_time_ms": landing_result.average_landing_time_ms,
        "failure_reasons": landing_result.failure_reasons,
        "simulated_tx_count": landing_result.simulated_tx_count,
        "estimated_landing_improvement_pct": landing_sim.estimate_landing_improvement(
            jito_context
        ),
    }

    def calculate_total_score(candidate: Any) -> float:
        base_score = min(10.0, max(0.0, candidate.get("dex_screener_score", 0.0)))

        # Optional Context Modifiers
        ctx_mod = 0.0
        perp = candidate.get("perp_context")
        if perp and perp.get("drift_context_status") == "ok":
            ctx_mod += 0.05 * perp.get("perp_context_confidence", 0.0)

        defi = candidate.get("defi_health", {})
        if defi and not defi.get("is_microcap_meme", True):
            if defi.get("defi_coverage_status") in ["full", "partial"]:
                # +0.15 max DeFi scaling inherently boosting logical confidence explicitly avoiding inflation directly.
                ctx_mod += 0.15 * defi.get("defi_health_score", 0.0)

        # Points / Restaking Carry Score
        carry_score = candidate.get("carry_total_score")
        if carry_score is not None:
            # Recommended weight 0.09, can be increased to 0.14 for pure DeFi tokens
            ctx_mod += 0.09 * carry_score

        # Carry / DeFi Health synergy bonus (within calculate_total_score)
        defi_health_score = candidate.get("defi_health", {}).get(
            "defi_health_score", 0.0
        )
        if carry_score is not None and carry_score >= 0.75 and defi_health_score >= 0.7:
            ctx_mod += 0.12
            
        # Vol Compression Breakout
        vol_score = candidate.get("vol_compression_score")
        if vol_score is not None:
            ctx_mod += 0.11 * vol_score
            if candidate.get("breakout_confirmed"):
                ctx_mod += 0.06
            if candidate.get("breakout_confirmed") and vol_score >= 0.65:
                ctx_mod += 0.15

        # Liquidity Refill 
        refill_score = candidate.get("liquidity_refill_score")
        if refill_score is not None:
            ctx_mod += 0.09 * refill_score
            
        # Holder Churn
        holder_churn_score = candidate.get("holder_churn_score")
        if holder_churn_score is not None:
            ctx_mod += 0.11 * holder_churn_score




        # Jito Execution Modifiers
        execution_ctx_mod = 0.0
        jito_ctx = candidate.get("jito_priority_context")
        if jito_ctx:
            tip_eff = jito_ctx.get("tip_efficiency_score", 0.0)
            landing_press = jito_ctx.get("landing_pressure_score", 0.0)
            execution_ctx_mod += 0.04 * tip_eff * landing_press

        return base_score + ctx_mod + execution_ctx_mod

    base_score = calculate_total_score(token_dict)

    # Combined additions
    total_score = base_score + (
        13.0 * orderflow_metrics.get("orderflow_purity_score", 0.0)
    )
    total_score += 13.0 * smart_metrics.get("smart_money_combined_score", 0.0)
    total_score += 11.0 * liq_metrics.get("liquidity_quality_score", 0.0)
    total_score += (
        8.0
        * soc_metrics.get("social_velocity_score", 0.0)
        * (1.0 - soc_metrics.get("attention_distortion_risk", 0.0))
    )
    total_score += 0.10 * narrative_metrics.get("narrative_velocity_score", 0.0)
    total_score += 14.0 * wallet_metrics.get("wallet_signal_confidence", 0.0)
    total_score += 0.11 * wallet_lead_lag_metrics.get("lead_lag_score", 0.0)
    total_score += 0.08 * wallet_lead_lag_metrics.get(
        "multi_timeframe_confirmation_score", 0.0
    )
    if cum_delta_metrics.get("cum_delta_score") is not None:
        total_score += 0.12 * cum_delta_metrics.get("cum_delta_score", 0.0)

    # Synergy bonus
    if (
        wallet_lead_lag_metrics.get("lead_lag_score", 0.0) >= 0.8
        and smart_metrics.get("smart_money_combined_score", 0.0) >= 0.7
    ):
        total_score += 0.12

    # Simulated Friction Pre-check
    sim_ctx = dict(token_dict)
    sim_ctx["gross_mark_to_market_pnl_pct"] = 0.0  # Pretend we just entered
    exit_snap = evaluate_net_executable_pnl(sim_ctx)
    entry_friction = exit_snap["net_executable_pnl_pct"]

    entry_blockers = []
    if entry_friction < -15.0:
        entry_blockers.append("excessive_entry_friction_net_loss")
        total_score -= 20.0  # Harsh deduction if executing burns 15% guaranteed.

    # Push into context
    token_dict.update(orderflow_metrics)
    token_dict.update(cum_delta_metrics)
    token_dict.update(smart_metrics)
    token_dict.update(liq_metrics)
    token_dict.update(soc_metrics)
    token_dict.update(narrative_metrics)
    token_dict.update(wallet_metrics)
    token_dict.update(wallet_lead_lag_metrics)



    token_dict["orderflow_hard_blockers"] = order_gates["hard_blockers"]
    token_dict["orderflow_soft_blockers"] = order_gates["soft_blockers"]
    token_dict["orderflow_passed_gates"] = order_gates["passed_hard_gates"]

    token_dict["smart_money_hard_blockers"] = smart_gates["hard_blockers"]
    token_dict["smart_money_soft_blockers"] = smart_gates["soft_blockers"]
    token_dict["smart_money_warnings"] = smart_gates["warnings"]
    token_dict["smart_money_passed_gates"] = smart_gates["passed_hard_gates"]

    token_dict["liquidity_hard_blockers"] = liq_gates["hard_blockers"]
    token_dict["liquidity_soft_blockers"] = liq_gates["soft_blockers"]
    token_dict["liquidity_warnings"] = liq_gates["warnings"]
    token_dict["liquidity_ passed_gates"] = liq_gates["passed_hard_gates"]

    token_dict["social_hard_blockers"] = soc_gates["hard_blockers"]
    token_dict["social_soft_blockers"] = soc_gates["soft_blockers"]
    token_dict["social_warnings"] = soc_gates["warnings"]
    token_dict["social_passed_gates"] = soc_gates["passed_hard_gates"]

    token_dict["wallet_hard_blockers"] = wallet_gates["hard_blockers"]
    token_dict["wallet_soft_blockers"] = wallet_gates["soft_blockers"]
    token_dict["wallet_warnings"] = wallet_gates["warnings"]
    token_dict["wallet_passed_gates"] = wallet_gates["passed_hard_gates"]

    token_dict["wallet_lead_lag_hard_blockers"] = wallet_lead_lag_gates["hard_blockers"]
    token_dict["wallet_lead_lag_soft_blockers"] = wallet_lead_lag_gates["soft_blockers"]
    token_dict["wallet_lead_lag_warnings"] = wallet_lead_lag_gates["warnings"]
    token_dict["wallet_lead_lag_passed_gates"] = wallet_lead_lag_gates[
        "passed_hard_gates"
    ]

    token_dict["carry_hard_blockers"] = carry_gates["hard_blockers"]
    token_dict["carry_soft_blockers"] = carry_gates["soft_blockers"]
    token_dict["carry_warnings"] = carry_gates["warnings"]
    token_dict["carry_passed_gates"] = carry_gates["passed_hard_gates"]

    token_dict["vol_compression_warnings"] = vol_comp_gates["warnings"]

    token_dict["holder_hard_blockers"] = holder_gates["hard_blockers"]
    token_dict["holder_soft_blockers"] = holder_gates["soft_blockers"]
    token_dict["holder_warnings"] = holder_gates["warnings"]
    token_dict["holder_passed_gates"] = holder_gates["passed_hard_gates"]



    token_dict["jito_hard_blockers"] = jito_gates["hard_blockers"]
    token_dict["jito_soft_blockers"] = jito_gates["soft_blockers"]
    token_dict["jito_warnings"] = jito_gates["warnings"]
    token_dict["jito_passed_gates"] = jito_gates["passed_hard_gates"]

    token_dict["jito_priority_context"] = jito_context_dict
    token_dict["landing_evidence"] = landing_evidence

    token_dict["simulated_entry_friction_pct"] = entry_friction
    token_dict["entry_friction_hard_blockers"] = entry_blockers

    token_dict["final_score_with_orderflow"] = round(total_score, 4)

    return token_dict
