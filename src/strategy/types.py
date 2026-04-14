"""Strict type definitions for Regime routing."""

from typing import TypedDict, Literal, List, Optional, Any, Dict

RegimeType = Literal["SCALP", "TREND", "DIP", "IGNORE", "UNKNOWN"]
HoldClassType = Literal["short", "medium", "long", "none"]

class VolCompressionEvidence(TypedDict):
    vol_compression_ratio: float
    vol_compression_score: float
    breakout_confirmed: bool
    provenance: Dict[str, Any]


class OrderflowMetrics(TypedDict, total=False):
    signed_buy_ratio: float
    block_0_snipe_pct: float
    repeat_buyer_ratio: float
    sybil_cluster_ratio: float
    organic_taker_volume_ratio: float
    orderflow_purity_score: float
    ghost_bid_ratio: float
    wash_trade_proxy: float
    organic_buy_ratio: float
    cum_delta_divergence: Optional[float]
    cum_delta_score: Optional[float]
    cum_delta_provenance: Dict[str, Any]

class SmartMoneyEvidence(TypedDict):
    distance_from_smart_entry_pct: float
    smart_money_distance_score: float
    bundle_pressure_score: float
    smart_money_combined_score: float

class LiquidityQualityEvidence(TypedDict):
    jupiter_buy_impact_bps: float
    jupiter_sell_impact_bps: float
    base_amm_liquidity_share: float
    dynamic_liquidity_share: float
    route_uses_dynamic_liquidity: bool
    liquidity_quality_score: float

class SocialVelocityEvidence(TypedDict):
    social_velocity_10m: float
    social_velocity_60m: float
    social_acceleration_ratio: float
    attention_distortion_risk: float
    social_velocity_score: float

class NarrativeVelocityEvidence(TypedDict):
    narrative_velocity_5m: float
    narrative_velocity_60m: float
    narrative_acceleration_ratio: float
    narrative_velocity_score: float

class WalletCohortEvidence(TypedDict):
    avg_sharpe_90d: float
    avg_sortino_90d: float
    avg_profit_factor: float
    avg_max_drawdown_90d: float
    avg_wallet_risk_adjusted_score: float
    cohort_concentration_ratio: float
    family_qualifier_multiplier: float
    wallet_signal_confidence: float

class PricePathEvidence(TypedDict):
    price_path_status: str # 'full', 'partial', 'missing', 'stale', 'rate_limited'
    price_path_source: str # 'jupiter', 'pyth', 'geckoterminal', 'dexscreener', 'backfill'
    price_path_confidence: float # 0.0 to 1.0
    gap_size_sec: int
    backfill_applied: bool
    price_path_diagnostic: str

class ReplayRow(TypedDict):
    token_address: str
    timestamp_sec: int
    price_path: PricePathEvidence
    candidate_snapshot_score: float # Mapped total score limits
    
class AblationResult(TypedDict):
    experiment_id: str
    component_mask: dict[str, bool]
    threshold_overrides: dict[str, float]
    regime_name: str
    baseline_metric_value: float
    ablated_metric_value: float
    delta_metric_value: float
    improvement_significant: bool

class ExperimentManifest(TypedDict):
    experiment_id: str
    timestamp: str
    baseline_metrics: dict[str, float]
    ablated_metrics: dict[str, float]
    
class MetricDefinition(TypedDict):
    name: str # e.g. "liquidity_quality_score"
    display_name: str # "Liquidity Quality Score"
    unit: str # "0..1", "bps", "Sol", "count"
    directionality: str # "higher_is_better", "lower_is_better", "context_only"
    trust_level: str # "execution_grade", "research_grade", "heuristic", "context_only"
    description: str
    source: str # Jupiter, Dune, Pyth
    interpretation: str
    
class TrendEvidence(TypedDict):
    reason_flags: List[str]
    warnings: List[str]
    confidence_score: float # 0.0 to 1.0

class PerpContext(TypedDict):
    drift_funding_rate: float
    drift_basis_bps: float
    drift_open_interest_change_5m_pct: float
    drift_open_interest_change_1h_pct: float
    drift_context_status: str # "ok" | "partial" | "missing" | "stale"
    perp_context_confidence: float # 0.0 to 1.0
    
class DefiHealthEvidence(TypedDict):
    defi_health_score: float
    tvl_trend_proxy: float
    revenue_yield_proxy: float
    utilization_norm: float
    rotation_context_state: str # "meme_dominant" | "neutral" | "defi_rotation"
    defi_coverage_status: str # "full" | "partial" | "missing"
    is_microcap_meme: bool

class CandidateSnapshot(TypedDict):
     token_address: str
     symbol: str
     dex_screener_score: float
     jupiter_liquidity_usd: float
     smart_money_inflows_1h_usd: float
     sybil_ratio: float
     base_price_sol: float
     regime: str
     trend_evidence: TrendEvidence
     perp_context: PerpContext
     defi_health: DefiHealthEvidence
     wallet_confidence: float
     bundle_sell_pressure: float
     total_fee_bps_estimate: float
     wallet_lead_lag_sec: float
     lead_lag_score: float
     multi_timeframe_confirmation_score: float
     lead_lag_provenance: str
     points_carry_score: float
     restaking_yield_proxy: Optional[float]
     carry_total_score: float
     carry_provenance: Dict[str, Any]
     vol_compression_ratio: Optional[float]
     vol_compression_score: Optional[float]
     breakout_confirmed: Optional[bool]
     vol_compression_provenance: Optional[Dict[str, Any]]
     liquidity_refill_half_life_sec: Optional[float]
     liquidity_refill_score: Optional[float]
     liquidity_refill_provenance: Optional[Dict[str, Any]]
     holder_churn_rate_24h: Optional[float]
     new_buyers_ratio_24h: Optional[float]
     returning_buyers_ratio_24h: Optional[float]
     holder_churn_score: Optional[float]
     liquidity_usd: float  # Alias for jupiter_liquidity_usd for gates
     jupiter_price_impact_bps: Optional[float]
     jupiter_sell_impact_bps: Optional[float]
     rugcheck_risk_score: Optional[float]
     estimated_total_fee_sol: float
     anti_rug_context_status: Optional[str]

class ExitSnapshot(TypedDict):
    gross_mark_to_market_pnl_pct: float
    net_executable_pnl_pct: float
    net_executable_pnl_sol: float
    hard_stop_loss_pct: float
    smart_money_bagholder_threshold: float
    bundle_sell_pressure: float
    total_fee_bps_estimate: float

class ExitDecision(TypedDict):
    invalidated: bool
    action: str
    reason: str

class ScalpEvidence(TypedDict):
    reason_flags: List[str]
    warnings: List[str]
    blockers: List[str]
    confidence: float
    support_points: float
    missing_optional: int

class DipEvidence(TypedDict):
    reason_flags: List[str]
    warnings: List[str]
    blockers: List[str]
    confidence: float
    support_points: float
    missing_optional: int
    drawdown_from_local_high_pct: float
    rebound_strength_pct: float
    sell_exhaustion_score: float
    support_reclaim_flag: bool
    dip_invalidated_flag: bool

class CandidateEvaluation(TypedDict):
    eligible: bool
    reason: str
    flags: List[str]
    warnings: List[str]
    failures: List[str]

class IgnoreEvaluation(TypedDict):
    ignore: bool
    reason: str
    flags: List[str]
    warnings: List[str]
    missing_fields: List[str]

class ExecutionContext(TypedDict):
    jito_priority_context: Optional[Any]  # JitoPriorityContext
    landing_pressure_score: float
    tip_efficiency_score: float
    tip_budget_violation_flag: bool
    dynamic_tip_target_lamports: int
    priority_lane: str  # "baseline" | "elevated" | "congested"

class LandingEvidence(TypedDict):
    success_rate: float
    average_landing_time_ms: float
    failure_reasons: Dict[str, int]
    simulated_tx_count: int
    estimated_landing_improvement_pct: float

class RegimeDecision(TypedDict):
    regime: RegimeType
    confidence: float
    expected_hold_class: HoldClassType
    reason: str
    reason_flags: List[str]
    warnings: List[str]
    blockers: List[str]
    trend_evidence: Optional[TrendEvidence]
    scalp_evidence: Optional[ScalpEvidence]
    dip_evidence: Optional[DipEvidence]
    smart_money_evidence: Optional[SmartMoneyEvidence]
    liquidity_quality_evidence: Optional[LiquidityQualityEvidence]
    social_velocity_evidence: Optional[SocialVelocityEvidence]
    wallet_cohort_evidence: Optional[WalletCohortEvidence]
    ignore_evaluation: Optional[IgnoreEvaluation]
