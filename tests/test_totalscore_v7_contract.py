import pytest
import os
import tempfile
import yaml
import json
from src.strategy.totalscore_v7 import TotalScoreV7ContractLoader

def test_deterministic_weight_table_loading():
    schema_path = "./data_contracts/candidate_snapshot_v7.schema.json"
    defaults_path = "./configs/totalscore_v7.defaults.yaml"
    loader = TotalScoreV7ContractLoader(schema_path, defaults_path)
    
    vol = loader.get_metric_config("VolAccelZ")
    assert vol["weight"] == 0.38
    assert vol["bucket"] == "score_contribution"

def test_unmapped_metric_is_rejected():
    schema_path = "./data_contracts/candidate_snapshot_v7.schema.json"
    defaults_path = "./configs/totalscore_v7.defaults.yaml"
    loader = TotalScoreV7ContractLoader(schema_path, defaults_path)
    
    with pytest.raises(ValueError) as exc:
        loader.get_metric_config("UnregisteredGhostProxyMetric")
        
    assert "Unmapped metric execution blocked" in str(exc.value)

def test_totalscore_v7_contract_parity_with_schema():
    schema_path = "./data_contracts/candidate_snapshot_v7.schema.json"
    defaults_path = "./configs/totalscore_v7.defaults.yaml"
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    with open(defaults_path, 'r') as f:
        defaults = yaml.safe_load(f)

    valid_buckets = set(schema["properties"]["metrics"]["patternProperties"]["^[A-Za-z0-9_]+$"]["properties"]["bucket"]["enum"])
    valid_stages = set(schema["properties"]["metrics"]["patternProperties"]["^[A-Za-z0-9_]+$"]["properties"]["stage"]["enum"])

    for metric_name, spec in defaults["metrics"].items():
        assert spec["bucket"] in valid_buckets, f"Metric {metric_name} bucket not in schema"
        assert spec["stage"] in valid_stages, f"Metric {metric_name} stage not in schema"

def test_contract_validates_invalid_buckets_correctly():
    # Setup dummy temp parameters testing schemas safely!
    with tempfile.TemporaryDirectory() as td:
        schema_path = os.path.join(td, "schema.json")
        defaults_path = os.path.join(td, "defaults.yaml")

        with open(schema_path, "w") as f:
            json.dump({}, f) # Stub

        with open(defaults_path, "w") as f:
            yaml.dump({"metrics": {"BadMetric": {"bucket": "random_fake_bucket", "stage": "active"}}}, f)

        with pytest.raises(ValueError) as exc:
            TotalScoreV7ContractLoader(schema_path, defaults_path)
        assert "bucket invalid" in str(exc.value)
