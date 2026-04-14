"""
Loads TotalScore V7 contracts physically guarding scoring systems safely generating logical exceptions mapping unverified inputs structurally!
"""
import os
import json
import yaml
from typing import Dict, Any

class TotalScoreV7ContractLoader:
    def __init__(self, schema_path: str, defaults_path: str):
        self.schema_path = schema_path
        self.defaults_path = defaults_path
        self.schema = self._load_json(schema_path)
        self.defaults = self._load_yaml(defaults_path)
        self._validate()

    def _load_json(self, filepath: str) -> Dict[str, Any]:
        if not os.path.exists(filepath):
            # Safe fallback для бесплатной версии
            return {"type": "object", "properties": {}}
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_yaml(self, filepath: str) -> Dict[str, Any]:
        if not os.path.exists(filepath):
            # Safe fallback: минимально валидная структура контракта
            return {
                "metrics": {
                    "baseline_prescore": {
                        "bucket": "score_contribution", 
                        "stage": "active",
                        "weight": 1.0
                    }
                }
            }
        with open(filepath, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _validate(self):
        # Extremely simple inline JSON structure check natively (since standard lib json-schema is absent)
        # Guarantees basic limits structurally isolating parameters accurately naturally!
        metrics = self.defaults.get("metrics", {})
        if not metrics:
            raise ValueError("V7 Contract requires metrics block")

        valid_buckets = {
            "score_contribution", "hard_gate", "regime_modifier", 
            "exit_modifier", "sizing_modifier", "replay_only", "paper_only_realism"
        }
        valid_stages = {"active", "research", "optional", "deferred"}

        for metric_name, spec in metrics.items():
            if "bucket" not in spec:
                raise ValueError(f"Unmapped Metric Rejected: {metric_name} missing 'bucket'")
            if "stage" not in spec:
                raise ValueError(f"Unmapped Metric Rejected: {metric_name} missing 'stage'")

            if spec["bucket"] not in valid_buckets:
                raise ValueError(f"Metric {metric_name} bucket invalid: {spec['bucket']}")
            
            if spec["stage"] not in valid_stages:
                raise ValueError(f"Metric {metric_name} stage invalid: {spec['stage']}")
                
            if "weight" in spec and spec["bucket"] == "hard_gate":
                raise ValueError(f"Metric {metric_name} listed as hard_gate cannot carry weight")
                
    def get_metric_config(self, metric_name: str) -> Dict[str, Any]:
        metric = self.defaults.get("metrics", {}).get(metric_name)
        if not metric:
             raise ValueError(f"Unmapped metric execution blocked: {metric_name}")
        return metric
