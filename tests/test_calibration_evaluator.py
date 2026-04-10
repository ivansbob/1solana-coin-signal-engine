from src.calibration.evaluator import compute_metrics, detect_regime_collapse
from src.calibration.leaderboard import compare_to_baseline


def test_compute_metrics_and_regime_collapse_and_baseline_comparison():
    trades = [
        {"pnl_pct": 10.0, "effective_position_pct": 0.10},
        {"pnl_pct": -5.0, "effective_position_pct": 0.50},
        {"pnl_pct": 5.0, "recommended_position_pct": 0.40},
    ]
    metrics = compute_metrics(trades)

    assert round(metrics["expectancy"], 2) == 3.33
    assert round(metrics["size_weighted_expectancy"], 2) == 0.5
    assert metrics["winrate"] == 2 / 3

    collapsed = detect_regime_collapse({"regimes": {"scalp_trades": 3, "trend_trades": 0}})
    assert collapsed is True

    baseline = {"validation": {"expectancy": 0.01}}
    candidate = {"validation": {"expectancy": 0.02}}
    assert compare_to_baseline(candidate, baseline, "validation_expectancy") is True
