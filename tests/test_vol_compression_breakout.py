import json
import pytest
import os
from src.strategy.volatility_metrics import compute_vol_compression_breakout

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def load_fixture(name: str):
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)

def test_vol_compression_strong_breakout():
    data = load_fixture("vol_compression_strong_breakout.json")
    result = compute_vol_compression_breakout("tokenX", fetched_data=data)
    
    # atr_5m: 0.005, atr_60m: 0.0104 -> ratio = 0.005 / (0.0104 + 0.0001) = 0.005/0.0105 = 0.476
    # score -> 1.0
    # breakout_confirmed -> True (12.5 > 8)
    
    assert round(result["vol_compression_ratio"], 2) == 0.48
    assert result["vol_compression_score"] == 1.0
    assert result["breakout_confirmed"] is True
    assert result["provenance"]["source"] == "dune"

def test_vol_compression_steady():
    data = load_fixture("vol_compression_steady.json")
    result = compute_vol_compression_breakout("tokenX", fetched_data=data)
    
    # atr_5m: 0.0083, atr_60m: 0.0100 -> ratio = 0.0083 / 0.0101 = 0.82
    # score -> 0.3
    # breakout_confirmed -> False (2.1 < 8)
    
    assert round(result["vol_compression_ratio"], 2) == 0.82
    assert result["vol_compression_score"] == 0.3
    assert result["breakout_confirmed"] is False

def test_vol_compression_expansion():
    data = load_fixture("vol_compression_expansion.json")
    result = compute_vol_compression_breakout("tokenX", fetched_data=data)
    
    # atr_5m: 0.015, atr_60m: 0.010 -> ratio = 0.015 / 0.0101 = 1.48
    # score -> 0.0
    
    assert round(result["vol_compression_ratio"], 2) == 1.49
    assert result["vol_compression_score"] == 0.0
    assert result["breakout_confirmed"] is False

def test_vol_compression_missing():
    data = load_fixture("vol_compression_missing.json")
    result = compute_vol_compression_breakout("tokenX", fetched_data=data)
    
    assert result["vol_compression_ratio"] is None
    assert result["vol_compression_score"] is None
    assert result["breakout_confirmed"] is False
    assert result["provenance"]["error"] == "missing_data"

def test_vol_compression_dead_pool():
    # If a pool is dead, atr_5m might be 0, atr_60m 0 => ratio = 0 / 0.0001 = 0.0 => score 1.0?
    # Wait, the prompt says "метрик отсекает 'шумовые' спайки... dead pool?". 
    # If atr_60m is very small too: 
    data = {"atr_5m": 0.000001, "atr_60m": 0.000001, "price_change_15m_pct": 0.0}
    result = compute_vol_compression_breakout("tokenX", fetched_data=data)
    
    # 0.000001 / 0.000101 = 0.009 -> score = 1.0 (Compression) 
    # But no breakout, so it gets lower score total. Works as expected natively.
    assert result["vol_compression_score"] == 1.0
    assert result["breakout_confirmed"] is False
