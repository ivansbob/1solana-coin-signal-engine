import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trading.fill_model import simulate_entry_fill, simulate_exit_fill


class S:
    PAPER_DEFAULT_SLIPPAGE_BPS = 150
    PAPER_MAX_SLIPPAGE_BPS = 1200
    PAPER_SLIPPAGE_LIQUIDITY_SENSITIVITY = 1.0
    PAPER_PRIORITY_FEE_BASE_SOL = 0.00002
    PAPER_PRIORITY_FEE_SPIKE_MULTIPLIER = 1.75
    PAPER_FAILED_TX_BASE_PROB = 0.0
    PAPER_FAILED_TX_LOW_LIQUIDITY_ADDON = 0.0
    PAPER_FAILED_TX_HIGH_VOLATILITY_ADDON = 0.0
    PAPER_PARTIAL_FILL_ALLOWED = True
    PAPER_PARTIAL_FILL_MIN_RATIO = 0.5
    PAPER_SOL_USD_FALLBACK = 100.0
    PAPER_MAX_POOL_PARTICIPATION_PCT = 0.03
    EXIT_SCALP_STOP_LOSS_PCT = -10
    EXIT_TREND_HARD_STOP_PCT = -18


def test_simulate_entry_fill_success():
    fill = simulate_entry_fill(
        {"token_address": "So1", "recommended_position_pct": 0.5, "entry_confidence": 0.8},
        {"price_usd": 1.0, "liquidity_usd": 1_000_000, "volatility": 0.1},
        {"free_capital_sol": 0.1},
        S(),
    )
    assert fill["tx_failed"] is False
    assert 0 < fill["fill_ratio"] <= 1


def test_simulate_entry_fill_prefers_effective_position_pct():
    fill = simulate_entry_fill(
        {"token_address": "So1", "recommended_position_pct": 0.5, "effective_position_pct": 0.2, "entry_confidence": 0.8},
        {"price_usd": 1.0, "liquidity_usd": 1_000_000, "volatility": 0.1},
        {"free_capital_sol": 0.1},
        S(),
    )
    assert fill["tx_failed"] is False
    assert fill["requested_notional_sol"] <= 0.02 + 1e-9


def test_simulate_exit_fill_success():
    fill = simulate_exit_fill(
        {"position_id": "pos_0001", "remaining_size_sol": 0.05, "entry_price_usd": 1.0},
        {"exit_decision": "FULL_EXIT", "exit_fraction": 1.0, "signal_quality": 1.0},
        {"price_usd": 1.2, "liquidity_usd": 1_000_000, "volatility": 0.1},
        S(),
    )
    assert fill["tx_failed"] is False
    assert fill["filled_notional_sol"] > 0


def test_simulate_exit_fill_reprices_proceeds_when_price_moves_up():
    fill = simulate_exit_fill(
        {"position_id": "pos_0001", "remaining_size_sol": 0.05, "entry_price_usd": 100.0},
        {"exit_decision": "FULL_EXIT", "exit_fraction": 1.0, "signal_quality": 1.0},
        {"price_usd": 150.0, "liquidity_usd": 1_000_000, "volatility": 0.1},
        S(),
    )
    assert fill["tx_failed"] is False
    assert 0 < fill["filled_cost_basis_sol"] <= fill["requested_notional_sol"]
    assert fill["filled_notional_sol"] > fill["filled_cost_basis_sol"]


def test_simulate_exit_fill_failclosed_uses_pessimistic_execution_not_fake_breakeven():
    fill = simulate_exit_fill(
        {"position_id": "pos_0002", "remaining_size_sol": 0.05, "entry_price_usd": 100.0, "entry_decision": "SCALP"},
        {"exit_decision": "FULL_EXIT", "exit_fraction": 1.0, "exit_reason": "missing_current_state_failclosed", "exit_flags": ["failclosed_missing_fields"], "exit_warnings": ["missing_critical_price_usd_now"]},
        {"liquidity_usd": 1_000_000, "volatility": 0.1},
        S(),
    )
    assert fill["tx_failed"] is False
    assert fill["execution_assumption"] == "failclosed_pessimistic_price"
    assert fill["degraded_execution_path"] is True
    assert fill["executed_price_usd"] < 100.0

def test_simulate_entry_fill_caps_requested_size_by_pool_liquidity_participation():
    fill = simulate_entry_fill(
        {"token_address": "SoCap", "recommended_position_pct": 1.0, "entry_confidence": 1.0},
        {"price_usd": 1.0, "liquidity_usd": 1_000.0, "volatility": 0.1, "sol_usd": 100.0},
        {"free_capital_sol": 10.0},
        S(),
    )
    assert fill["requested_notional_sol"] <= 0.3 + 1e-9
    assert fill["requested_notional_sol"] < 10.0


def test_simulate_exit_fill_applies_sol_usd_correction_to_proceeds():
    stronger_sol = simulate_exit_fill(
        {
            "position_id": "pos_0003",
            "remaining_size_sol": 1.0,
            "entry_price_usd": 100.0,
            "entry_snapshot": {"sol_usd": 100.0},
        },
        {"exit_decision": "FULL_EXIT", "exit_fraction": 1.0, "signal_quality": 1.0},
        {"price_usd": 150.0, "liquidity_usd": 1_000_000, "volatility": 0.1, "sol_usd": 200.0},
        S(),
    )
    weaker_sol = simulate_exit_fill(
        {
            "position_id": "pos_0004",
            "remaining_size_sol": 1.0,
            "entry_price_usd": 100.0,
            "entry_snapshot": {"sol_usd": 100.0},
        },
        {"exit_decision": "FULL_EXIT", "exit_fraction": 1.0, "signal_quality": 1.0},
        {"price_usd": 150.0, "liquidity_usd": 1_000_000, "volatility": 0.1, "sol_usd": 100.0},
        S(),
    )
    assert stronger_sol["tx_failed"] is False
    assert weaker_sol["tx_failed"] is False
    assert stronger_sol["filled_notional_sol"] < weaker_sol["filled_notional_sol"]
