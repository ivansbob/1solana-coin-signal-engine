import pytest
from src.reports.metric_catalog import get_metric

def test_metric_catalog_is_consistent_with_scoring_components():
    # Verify core known metrics structurally pass correctly yielding explicit fields
    metric = get_metric("liquidity_quality_score")
    assert metric["trust_level"] == "execution_grade"
    assert metric["directionality"] == "higher_is_better"
    
def test_missing_metrics_do_not_throw_errors():
    # If a metric is totally unregistered, we immediately mark it as unknown avoiding crashes while signaling structurally.
    missing = get_metric("random_ghost_heuristic")
    assert missing["trust_level"] == "unknown"
    assert "Missing" in missing["description"]
