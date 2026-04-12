"""
Main file for ablation. Note the typo 'abalation.py' is kept intentionally for historical project continuity.
"""

from typing import Dict, Any, List
import uuid
import datetime
from src.strategy.types import AblationResult, ExperimentManifest

class OfflineReplayMocker:
    """Mocks execution over static data ensuring we run identical comparisons offline."""
    @staticmethod
    def run_simulation(component_mask: Dict[str, bool], metrics_base: float) -> float:
        # Mock calculation: Start with metrics base and subtract value if disabled.
        # This completely isolates tracking drops intuitively.
        score = metrics_base
        if not component_mask.get("VolAccelZ", True): score -= 1.2
        if not component_mask.get("WalletCohortScore", True): score -= 0.8
        if not component_mask.get("SmartMoneyCombinedScore", True): score -= 1.5
        if not component_mask.get("LiquidityQualityScore", True): score -= 0.5
        if not component_mask.get("SocialVelocityScore", True): score -= 0.3
        return max(0.0, score)


class AblationRunner:
    """Manages explicit execution loops comparing identical configurations."""
    
    def __init__(self, regimes: List[str]):
        self.regimes = regimes
        
    def evaluate_component(self, component_name: str) -> List[AblationResult]:
        experiment_id = f"ablation_{component_name}_{uuid.uuid4().hex[:6]}"
        results = []
        
        for regime in self.regimes:
            # Baseline is everything True
            baseline_mask = {
                "VolAccelZ": True,
                "WalletCohortScore": True,
                "SmartMoneyCombinedScore": True,
                "LiquidityQualityScore": True,
                "SocialVelocityScore": True
            }
            
            ablated_mask = dict(baseline_mask)
            if component_name in ablated_mask:
                ablated_mask[component_name] = False
                
            baseline_val = OfflineReplayMocker.run_simulation(baseline_mask, metrics_base=5.0)
            ablated_val = OfflineReplayMocker.run_simulation(ablated_mask, metrics_base=5.0)
            
            delta = baseline_val - ablated_val
            
            result: AblationResult = {
                "experiment_id": experiment_id,
                "component_mask": ablated_mask,
                "threshold_overrides": {},
                "regime_name": regime,
                "baseline_metric_value": baseline_val,
                "ablated_metric_value": ablated_val,
                "delta_metric_value": delta,
                "improvement_significant": delta >= 0.4 # Significance threshold
            }
            results.append(result)
            
        return results
        
    def generate_manifest(self, experiment_id: str, results: List[AblationResult]) -> ExperimentManifest:
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return {
            "experiment_id": experiment_id,
            "timestamp": ts,
            "baseline_metrics": {"avg_expectancy": 5.0},
            "ablated_metrics": {"avg_expectancy": sum(r["ablated_metric_value"] for r in results)/len(results)}
        }
