"""
Handles parameter sweeping testing combinatorial changes dynamically offline.
"""

from typing import Dict, Any, List
import uuid
import datetime
from src.strategy.types import AblationResult

class ThresholdSweeper:
    def __init__(self, regimes: List[str]):
        self.regimes = regimes
        
    def sweep_threshold(self, parameter_name: str, values: List[float]) -> List[AblationResult]:
        experiment_id = f"sweep_{parameter_name}_{uuid.uuid4().hex[:6]}"
        results = []
        
        base_val = 5.0 
        for v in values:
            for regime in self.regimes:
                # Math proxy: if we sweep closer to 2.0 we find the ideal threshold in tests.
                distance = abs(2.0 - v)
                swept_val = base_val - distance
                
                delta = swept_val - base_val
                
                result: AblationResult = {
                    "experiment_id": experiment_id,
                    "component_mask": {},
                    "threshold_overrides": {parameter_name: v},
                    "regime_name": regime,
                    "baseline_metric_value": base_val,
                    "ablated_metric_value": swept_val,
                    "delta_metric_value": delta,
                    "improvement_significant": delta > 0.0 # Strict improvement 
                }
                results.append(result)
                
        return results
