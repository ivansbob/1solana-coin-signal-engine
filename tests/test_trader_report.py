import pytest
from src.reports.trader_report import TraderReportGenerator

def test_trader_report_smoke_with_fixture():
    metrics = {
        "liquidity_quality_score": 0.85,
        "social_velocity_score": 7.0
    }
    
    report = TraderReportGenerator.generate_candidate_report(metrics, "TREND")
    assert "Candidate Evaluation: Regime `TREND`" in report
    assert "Liquidity Quality Score" in report
    
def test_report_shows_trust_levels_and_provenance():
    metrics = {
        "liquidity_quality_score": 0.90
    }
    
    report = TraderReportGenerator.generate_candidate_report(metrics, "SCALP")
    # Priority rendering displays execution grades inherently securely
    assert "**EXECUTION_GRADE**" in report
    assert "Jupiter Quote" in report

def test_high_heuristic_metrics_are_marked_as_such():
    metrics = {
        "social_velocity_score": 9.9
    }
    report = TraderReportGenerator.generate_candidate_report(metrics, "SCALP")
    assert "*HEURISTIC*" in report

def test_missing_data_is_not_hidden_in_report():
    metrics = {
        "liquidity_quality_score": None # Data mapping explicitly failed
    }
    report = TraderReportGenerator.generate_candidate_report(metrics, "DIP")
    assert "~~DEGRADED~~" in report
    assert "MISSING" in report
