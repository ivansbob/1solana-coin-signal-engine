#!/usr/bin/env python3
"""
CLI Execution wrapper running abalation parameters isolated completely.
"""
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.research.abalation import AblationRunner
from src.research.calibration import ThresholdSweeper

def main():
    regimes = ["SCALP", "TREND", "DIP"]
    runner = AblationRunner(regimes)
    
    components_to_test = ["VolAccelZ", "WalletCohortScore", "SocialVelocityScore"]
    
    print("--- RUNNING OFFLINE ABLATION ---")
    for component in components_to_test:
        results = runner.evaluate_component(component)
        for r in results:
            sig = "VERIFIED" if r["improvement_significant"] else "REJECTED"
            print(f"[{r['regime_name']}] Ablating {component}: Baseline={r['baseline_metric_value']} vs Ablated={r['ablated_metric_value']} -> Delta: {r['delta_metric_value']:.2f} ({sig})")

    print("\n--- RUNNING THRESHOLD SWEEPS ---")
    sweeper = ThresholdSweeper(regimes)
    sweep_results = sweeper.sweep_threshold("VolAccelZ_threshold", [1.0, 1.5, 2.0, 2.5])
    for r in sweep_results:
        sig = "IMPROVEMENT" if r["improvement_significant"] else "DEGRADED"
        val = r["threshold_overrides"]["VolAccelZ_threshold"]
        print(f"[{r['regime_name']}] Parameter VolAccelZ_threshold={val} -> Delta: {r['delta_metric_value']:.2f} ({sig})")

if __name__ == "__main__":
    main()
