import json
import pytest
import os
import math
from src.strategy.liquidity_metrics import compute_liquidity_refill_half_life

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def load_fixture(name: str):
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)

def test_liquidity_refill_fast():
    data = load_fixture("liquidity_refill_fast.json")
    result = compute_liquidity_refill_half_life("tokenX", window_sec=120, fetched_data=data)
    
    # ratio = 376500 / 1000000 = 0.3765
    # half_life = 120 * ln(0.5) / ln(0.3765) = 120 * -0.693147 / -0.97682 = 85.15
    # score -> 1.0 (between 30 and 180)
    
    assert round(result["liquidity_refill_half_life_sec"]) == 85
    assert result["liquidity_refill_score"] == 1.0

def test_liquidity_refill_slow():
    data = load_fixture("liquidity_refill_slow.json")
    result = compute_liquidity_refill_half_life("tokenX", window_sec=120, fetched_data=data)
    
    # ratio = 820000 / 1000000 = 0.82
    # half_life = 120 * ln(0.5) / ln(0.82) = 120 * -0.693147 / -0.19845 = 419.1
    # score -> 0.0
    
    assert round(result["liquidity_refill_half_life_sec"]) == 419
    assert result["liquidity_refill_score"] == 0.0

def test_liquidity_refill_dead():
    data = load_fixture("liquidity_refill_dead.json")
    result = compute_liquidity_refill_half_life("tokenX", window_sec=120, fetched_data=data)
    
    # ratio = 50000 / 1000000 = 0.05
    # half_life = 120 * ln(0.5) / ln(0.05) = 120 * -0.6931 / -2.9957 = 27.7
    # Wait, the prompt says "liquidity_refill_dead.json (liquidity ne vozvratitca -> blocker)".
    # If ratio is very low, half life is short because it dropped INSTANTLY and stayed? 
    # Actually, the half-life of liquidity draining is fast (27s) but we want REFILLED logic.
    # Ah! The formula ln(0.5) / ln(...) means: if it drops to 50% in X, how fast is the drop.
    # If it recovers to 80%, the "half life" measure in this prompt calculates how fast it drains?! 
    # "за которое ликвидность возвращается к 80%..."
    # The math they gave `ln(0.5)/ln(liq/peak)` actually calculates the "half life of the decay", not refill.
    # But I must follow their exact formula.
    # For dead pool, it drained to 0.05. half_life is 27. 27 is < 30.
    # Since it's < 30, the score is 0.0! (because 1.0 is 30 <= x <= 180).
    # So it naturally gets score 0.0 and becomes a blocker! Perfect.
    
    assert round(result["liquidity_refill_half_life_sec"]) == 28
    assert result["liquidity_refill_score"] == 0.0

def test_liquidity_refill_missing_data():
    result = compute_liquidity_refill_half_life("tokenX", window_sec=120, fetched_data=None)
    assert result["liquidity_refill_half_life_sec"] is None
    assert result["liquidity_refill_score"] is None
