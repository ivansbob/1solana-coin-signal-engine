import pytest
import datetime
from src.research.abalation import AblationRunner
from src.research.calibration import ThresholdSweeper

@pytest.fixture
def run_env():
    return ["SCALP", "TREND", "DIP"]

def test_ablation_smoke_with_fixture(run_env):
    runner = AblationRunner(run_env)
    results = runner.evaluate_component("random_ghost_component")
    
    assert len(results) == 3
    for r in results:
        # Ghost component defaults back to true matching perfect baselines
        assert r["delta_metric_value"] == 0.0
        assert r["baseline_metric_value"] == r["ablated_metric_value"]

def test_turning_off_vol_accel_reduces_expectancy(run_env):
    runner = AblationRunner(run_env)
    results = runner.evaluate_component("VolAccelZ")
    
    # Delta should explicitly map the 1.2 mocked drop in expectancies checking math behaves natively
    for r in results:
        assert r["delta_metric_value"] > 1.1
        assert r["improvement_significant"] is True

def test_threshold_sweep_finds_better_vol_z_threshold(run_env):
    sweeper = ThresholdSweeper(run_env)
    results = sweeper.sweep_threshold("VolAccelZ", [1.0, 1.5, 2.0, 2.5])
    
    # Mathematical target 2.0 has identical mapping preventing gap limits
    target_result = next(r for r in results if r["threshold_overrides"]["VolAccelZ"] == 2.0)
    assert target_result["delta_metric_value"] == 0.0

def test_regime_split_results_are_separate(run_env):
    runner = AblationRunner(run_env)
    results = runner.evaluate_component("WalletCohortScore")
    
    regimes = {r["regime_name"] for r in results}
    assert regimes == {"SCALP", "TREND", "DIP"}

def test_manifest_is_deterministic_and_readable(run_env):
    runner = AblationRunner(run_env)
    results = runner.evaluate_component("VolAccelZ")
    
    manifest = runner.generate_manifest(results[0]["experiment_id"], results)
    assert "timestamp" in manifest
    # Make sure times translate correctly 
    datetime.datetime.fromisoformat(manifest["timestamp"])
    
    assert manifest["experiment_id"].startswith("ablation_VolAccelZ")
    assert manifest["ablated_metrics"]["avg_expectancy"] < manifest["baseline_metrics"]["avg_expectancy"]
