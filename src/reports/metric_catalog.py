"""
Centralized registry for scoring components ensuring Live Traders intuitively understand boundaries.
"""

from typing import Dict, List
from src.strategy.types import MetricDefinition

def build_metric_catalog() -> Dict[str, MetricDefinition]:
    return {
        "liquidity_quality_score": {
            "name": "liquidity_quality_score",
            "display_name": "Liquidity Quality Score",
            "unit": "0..1",
            "directionality": "higher_is_better",
            "trust_level": "execution_grade",
            "description": "True slippage penalty measured via micro-capital tests tracking buy/sell impact.",
            "source": "Jupiter Quote Simulation",
            "interpretation": "1.0 = Highly Liquid, 0.0 = Extremely Rugged. Do not trade < 0.6."
        },
        "orderflow_purity_score": {
            "name": "orderflow_purity_score",
            "display_name": "Orderflow Purity",
            "unit": "0..1",
            "directionality": "higher_is_better",
            "trust_level": "execution_grade",
            "description": "Ratio of valid human buyers tracking Block-0 sybil traces and MEV clustering.",
            "source": "Helius Transaction Lake",
            "interpretation": "Scores > 0.8 guarantee relatively safe orderflow. Plummets aggressively when clustered sybils hit."
        },
        "social_velocity_score": {
            "name": "social_velocity_score",
            "display_name": "Social Velocity",
            "unit": "0..10.0 (additive)",
            "directionality": "higher_is_better",
            "trust_level": "heuristic",
            "description": "Calculates Twitter/Telegram mention accelerates (10m vs 60m). Heavily distorted by bots.",
            "source": "Social Search APIs",
            "interpretation": "Additive bonus only. NEVER rely exclusively on high values without on-chain liquidity."
        },
        "wallet_signal_confidence": {
            "name": "wallet_signal_confidence",
            "display_name": "Wallet Signal Confidence",
            "unit": "0..1",
            "directionality": "higher_is_better",
            "trust_level": "research_grade",
            "description": "Mathematically bounded index checking Sharpe and Sortino while bounding 90d drawdowns.",
            "source": "Dune Analytics SQL Backfill",
            "interpretation": "Drawdowns > 59% aggressively crater scores to 0.0 protecting copy-strategies."
        },
        "lead_lag_score": {
            "name": "lead_lag_score",
            "display_name": "Lead-Lag Score",
            "unit": "0..1",
            "directionality": "higher_is_better",
            "trust_level": "research_grade",
            "description": "Temporal lead of smart wallets (win-rate >=65%) over followers, detecting true alpha chains.",
            "source": "Dune Analytics SQL",
            "interpretation": "1.0 = Strong lead (8-45s), 0.0 = Too fast/slow. Confirms TREND/SCALP signals."
        },
        "multi_timeframe_confirmation_score": {
            "name": "multi_timeframe_confirmation_score",
            "display_name": "Multi-Timeframe Confirmation Score",
            "unit": "0..1",
            "directionality": "higher_is_better",
            "trust_level": "research_grade",
            "description": "Confirmation of wallet activity across 1m, 5m, 15m timeframes.",
            "source": "Dune Analytics SQL",
            "interpretation": "1.0 = All 3 timeframes, 0.6 = 2/3, 0.2 = 1/3. Reduces false positives."
        }
    }

def get_metric(name: str) -> MetricDefinition:
    catalog = build_metric_catalog()
    return catalog.get(name, {
        "name": name,
        "display_name": name.replace("_", " ").title(),
        "unit": "unknown",
        "directionality": "context_only",
        "trust_level": "unknown",
        "description": "Missing definition.",
        "source": "unknown",
        "interpretation": "Missing explicitly registered logic - treat cautiously."
    })
