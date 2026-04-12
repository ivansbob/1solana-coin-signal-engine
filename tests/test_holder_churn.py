import json
import pytest
import os
from src.strategy.holder_metrics import compute_holder_churn_metrics

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def load_fixture(name: str):
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)

def test_holder_churn_sticky():
    data = load_fixture("holder_churn_sticky.json")
    result = compute_holder_churn_metrics("tokenX", fetched_data=data)
    
    # 44 / 200 = 0.22 churn => returning = 0.78
    # score logic: >= 0.65 -> 1.0
    assert result["holder_churn_rate_24h"] == 0.22
    assert result["returning_buyers_ratio_24h"] == 0.78
    assert result["holder_churn_score"] == 1.0

def test_holder_churn_flipper():
    data = load_fixture("holder_churn_flipper.json")
    result = compute_holder_churn_metrics("tokenX", fetched_data=data)
    
    # 156 / 200 = 0.78 churn => returning = 0.22
    # score logic: < 0.40 -> 0.0
    assert result["holder_churn_rate_24h"] == 0.78
    assert result["returning_buyers_ratio_24h"] == 0.22
    assert result["holder_churn_score"] == 0.0

def test_holder_churn_mixed():
    data = load_fixture("holder_churn_mixed.json")
    result = compute_holder_churn_metrics("tokenX", fetched_data=data)
    
    # 96 / 200 = 0.48 churn => returning = 0.52
    # score logic: 0.40 <= returning < 0.65 -> 0.5
    assert result["holder_churn_rate_24h"] == 0.48
    assert result["returning_buyers_ratio_24h"] == 0.52
    assert result["holder_churn_score"] == 0.5

def test_holder_churn_missing():
    result = compute_holder_churn_metrics("tokenX", fetched_data=None)
    assert result["holder_churn_rate_24h"] is None
    assert result["holder_churn_score"] is None
    assert result["holder_churn_provenance"]["error"] == "missing_data"
