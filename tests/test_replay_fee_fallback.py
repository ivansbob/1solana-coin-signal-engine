from __future__ import annotations

from src.replay.historical_replay_harness import _build_settings, _estimate_replay_exit_pnl


def test_missing_priority_fee_uses_honest_zero_observation_and_keeps_winner_plausible():
    settings = _build_settings({}, wallet_weighting="off")
    current = {
        "gross_pnl_pct": 12.0,
        "liquidity_usd": 32000.0,
        "volume_velocity": 3.2,
        "sell_pressure": 0.18,
        "congestion_multiplier": 1.0,
        "sol_usd": 100.0,
        "exit_decision": "FULL_EXIT",
        "exit_reason": "scalp_momentum_decay_after_recheck",
        "exit_flags": [],
    }
    position_ctx = {
        "effective_position_pct": 0.25,
        "entry_confidence": 0.84,
        "liquidity_usd": 32000.0,
        "volume_velocity": 3.2,
        "sol_usd": 100.0,
    }

    pnl = _estimate_replay_exit_pnl(current, position_ctx, settings)

    assert pnl["gross_pnl_pct"] == 12.0
    assert pnl["net_pnl_pct"] is not None
    assert pnl["net_pnl_pct"] > 0
    assert pnl["net_pnl_pct"] < pnl["gross_pnl_pct"]
    assert (pnl["gross_pnl_pct"] - pnl["net_pnl_pct"]) < 5.0
