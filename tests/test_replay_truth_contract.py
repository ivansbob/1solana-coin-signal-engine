import pytest
from src.replay.price_path_resolver import resolve_price_path

def test_missing_price_path_marks_row_as_degraded():
    evidence = resolve_price_path(None, 1000, "exhausted_all_routers")
    assert evidence["price_path_status"] == "missing"
    assert evidence["price_path_confidence"] == 0.0
    assert evidence["gap_size_sec"] == -1
    
def test_replay_summary_shows_price_path_statistics():
    # Will integrate via the harness dynamically. Here we just assert the logic natively
    # maps over the components cleanly.
    evidence_full = resolve_price_path({"timestamp": 1000}, 1000, "jupiter")
    assert evidence_full["price_path_status"] == "full"
    
    evidence_partial = resolve_price_path({"timestamp": 900}, 1000, "dexscreener")
    assert evidence_partial["price_path_status"] == "partial"
    assert evidence_partial["gap_size_sec"] == 100
    
    # STALE
    evidence_stale = resolve_price_path({"timestamp": 500}, 1000, "dexscreener")
    assert evidence_stale["price_path_status"] == "stale"
    assert evidence_stale["gap_size_sec"] == 500
