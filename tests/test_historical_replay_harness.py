from __future__ import annotations

import json
from pathlib import Path

from src.replay import historical_replay_harness as replay_harness
from src.replay.historical_replay_harness import _build_settings, run_historical_replay

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "historical_replay"


def test_historical_replay_reconstructs_resolved_winning_trade(tmp_path):
    result = run_historical_replay(
        artifact_dir=FIXTURES / "full_win",
        run_id="unit_full_win",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    summary = result["summary"]
    trade = result["artifacts"].trades[0]
    row = result["artifacts"].trade_feature_matrix[0]

    assert summary["historical_rows_used"] == 1
    assert summary["partial_rows"] == 0
    assert summary["wallet_weighting_requested_mode"] == "off"
    assert summary["replay_score_source"] == "generic_scored_artifact_rescored"
    assert summary["wallet_mode_parity_status"] == "comparable"
    assert summary["historical_input_hash"]
    assert trade["replay_resolution_status"] == "resolved"
    assert trade["gross_pnl_pct"] is not None
    assert trade["net_pnl_pct"] < trade["gross_pnl_pct"]
    assert trade["replay_score_source"] == "generic_scored_artifact_rescored"
    assert trade["wallet_mode_parity_status"] == "comparable"
    assert trade["net_pnl_pct"] > 0
    assert row["replay_input_origin"] == "historical"
    assert row["replay_data_status"] == "historical"
    assert row["replay_score_source"] == "generic_scored_artifact_rescored"
    assert row["wallet_mode_parity_status"] == "comparable"
    assert row["historical_input_hash"] == summary["historical_input_hash"]
    assert row["synthetic_assist_flag"] is False


def test_historical_replay_reconstructs_resolved_losing_trade(tmp_path):
    result = run_historical_replay(
        artifact_dir=FIXTURES / "full_loss",
        run_id="unit_full_loss",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    trade = result["artifacts"].trades[0]
    assert trade["replay_resolution_status"] == "resolved"
    assert trade["gross_pnl_pct"] is not None
    assert trade["net_pnl_pct"] < trade["gross_pnl_pct"]
    assert trade["net_pnl_pct"] < 0
    assert trade["exit_reason_final"] in {"trend_hard_stop", "scalp_stop_loss"}


def test_build_settings_applies_candidate_overrides_for_replay_runs():
    settings = _build_settings(
        {
            "EXIT_SCALP_STOP_LOSS_PCT": -10,
            "baseline": {"EXIT_TREND_HARD_STOP_PCT": -18},
            "candidate": {"EXIT_SCALP_STOP_LOSS_PCT": -7, "EXIT_TREND_HARD_STOP_PCT": -14},
        },
        wallet_weighting="shadow",
    )

    assert settings.EXIT_SCALP_STOP_LOSS_PCT == -7
    assert settings.EXIT_TREND_HARD_STOP_PCT == -14
    assert settings.WALLET_WEIGHTING_MODE == "shadow"


def test_historical_replay_matrix_includes_discovery_lag_and_tx_lake_provenance(tmp_path):
    artifact_dir = tmp_path / "fixture"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "entry_candidates.json").write_text(json.dumps([{
        "token_address": "tok_prov",
        "pair_address": "pair_prov",
        "entry_decision": "ENTER",
    }]), encoding="utf-8")
    (artifact_dir / "scored_tokens.jsonl").write_text(json.dumps({
        "token_address": "tok_prov",
        "pair_address": "pair_prov",
        "symbol": "PROV",
        "final_score": 89.0,
        "final_score_pre_wallet": 84.0,
        "regime_candidate": "ENTRY_CANDIDATE",
        "regime_decision": "SCALP",
        "entry_confidence": 0.81,
        "recommended_position_pct": 0.22,
        "effective_position_pct": 0.18,
        "base_position_pct": 0.25,
        "liquidity_usd": 31000.0,
        "buy_pressure": 0.82,
        "first30s_buy_ratio": 0.79,
        "volume_velocity": 4.1,
        "bundle_count_first_60s": 2,
        "bundle_success_rate": 0.71,
        "discovery_lag_penalty_applied": True,
        "discovery_lag_blocked_trend": False,
        "discovery_lag_size_multiplier": 0.6,
        "discovery_lag_score_penalty": 5.0,
        "tx_batch_status": "usable",
        "tx_batch_freshness": "stale_cache_allowed",
        "tx_fetch_mode": "upstream_failed_use_stale",
        "tx_batch_warning": "upstream_failed_use_stale",
        "entry_price": 1.0,
        "entry_time": "2026-03-10T12:00:00Z",
        "features": {
            "age_sec": 75,
            "age_minutes": 1.25,
            "liquidity_usd": 31000.0,
            "buy_pressure": 0.82,
            "volume_velocity": 4.1,
            "smart_wallet_hits": 1,
        },
    }) + "\n", encoding="utf-8")
    (artifact_dir / "price_paths.json").write_text(json.dumps([{
        "token_address": "tok_prov",
        "pair_address": "pair_prov",
        "price_path": [
            {"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"},
            {"offset_sec": 35, "price": 1.08, "timestamp": "2026-03-10T12:00:35Z"},
        ],
    }]), encoding="utf-8")

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_provenance_passthrough",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    row = result["artifacts"].trade_feature_matrix[0]
    assert row["discovery_lag_penalty_applied"] is True
    assert row["discovery_lag_blocked_trend"] is False
    assert row["discovery_lag_size_multiplier"] == 0.6
    assert row["discovery_lag_score_penalty"] == 5.0
    assert row["tx_batch_status"] == "usable"
    assert row["tx_batch_freshness"] == "stale_cache_allowed"
    assert row["tx_fetch_mode"] == "upstream_failed_use_stale"
    assert row["tx_batch_warning"] == "upstream_failed_use_stale"


def test_historical_replay_treats_explicit_scalp_signal_as_enter(tmp_path):
    artifact_dir = tmp_path / "fixture_scalp_signal"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "scored_tokens.jsonl").write_text(json.dumps({
        "token_address": "tok_scalp",
        "pair_address": "pair_scalp",
        "symbol": "SCLP",
        "final_score": 91.0,
        "final_score_pre_wallet": 88.0,
        "entry_confidence": 0.82,
        "recommended_position_pct": 0.25,
        "effective_position_pct": 0.25,
        "base_position_pct": 0.25,
        "liquidity_usd": 40000.0,
        "buy_pressure": 0.84,
        "volume_velocity": 4.8,
        "x_validation_score": 72.0,
        "entry_price": 1.0,
        "entry_time": "2026-03-10T12:00:00Z",
        "features": {
            "age_sec": 90,
            "liquidity_usd": 40000.0,
            "buy_pressure": 0.84,
            "volume_velocity": 4.8,
        },
    }) + "\n", encoding="utf-8")
    (artifact_dir / "signals.jsonl").write_text(json.dumps({
        "token_address": "tok_scalp",
        "pair_address": "pair_scalp",
        "decision": "SCALP",
        "entry_decision": "SCALP",
        "regime_decision": "SCALP",
        "ts": "2026-03-10T12:00:00Z",
    }) + "\n", encoding="utf-8")
    (artifact_dir / "price_paths.json").write_text(json.dumps([{
        "token_address": "tok_scalp",
        "pair_address": "pair_scalp",
        "price_path": [
            {"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"},
            {"offset_sec": 45, "price": 1.09, "timestamp": "2026-03-10T12:00:45Z"},
            {"offset_sec": 90, "price": 0.97, "timestamp": "2026-03-10T12:01:30Z"},
        ],
    }]), encoding="utf-8")

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_scalp_signal_enters",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    assert result["artifacts"].signals[0]["entry_decision"] == "SCALP"
    assert len(result["artifacts"].trades) == 1
    assert len(result["artifacts"].trade_feature_matrix) == 1
    assert result["artifacts"].positions[0]["status"] != "ignored"
    assert result["artifacts"].trades[0]["entry_decision"] == "SCALP"
    assert result["artifacts"].trades[0]["decision"] == "SCALP"


def test_historical_replay_preserves_explicit_ignore_signal_as_ignore(tmp_path):
    artifact_dir = tmp_path / "fixture_ignore_signal"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "scored_tokens.jsonl").write_text(json.dumps({
        "token_address": "tok_ignore",
        "pair_address": "pair_ignore",
        "symbol": "IGN",
        "final_score": 89.0,
        "entry_price": 1.0,
        "entry_time": "2026-03-10T12:00:00Z",
    }) + "\n", encoding="utf-8")
    (artifact_dir / "signals.jsonl").write_text(json.dumps({
        "token_address": "tok_ignore",
        "pair_address": "pair_ignore",
        "decision": "IGNORE",
        "entry_decision": "IGNORE",
        "regime_decision": "SCALP",
        "ts": "2026-03-10T12:00:00Z",
    }) + "\n", encoding="utf-8")
    (artifact_dir / "price_paths.json").write_text(json.dumps([{
        "token_address": "tok_ignore",
        "pair_address": "pair_ignore",
        "price_path": [
            {"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"},
            {"offset_sec": 45, "price": 1.09, "timestamp": "2026-03-10T12:00:45Z"},
        ],
    }]), encoding="utf-8")

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_ignore_signal_ignored",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    assert result["artifacts"].signals[0]["entry_decision"] == "IGNORE"
    assert result["artifacts"].trades == []
    assert result["artifacts"].trade_feature_matrix == []
    assert result["artifacts"].positions[0]["status"] == "ignored"


def test_historical_replay_uses_historical_regime_for_exit_resolution(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "fixture_historical_regime_exit"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "scored_tokens.jsonl").write_text(json.dumps({
        "token_address": "tok_trend",
        "pair_address": "pair_trend",
        "symbol": "TRND",
        "final_score": 93.0,
        "entry_price": 1.0,
        "entry_time": "2026-03-10T12:00:00Z",
        "recommended_position_pct": 0.2,
    }) + "\n", encoding="utf-8")
    (artifact_dir / "signals.jsonl").write_text(json.dumps({
        "token_address": "tok_trend",
        "pair_address": "pair_trend",
        "decision": "ENTER",
        "entry_decision": "ENTER",
        "regime_decision": "TREND",
        "ts": "2026-03-10T12:00:00Z",
    }) + "\n", encoding="utf-8")
    (artifact_dir / "price_paths.json").write_text(json.dumps([{
        "token_address": "tok_trend",
        "pair_address": "pair_trend",
        "price_path": [
            {"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"},
            {"offset_sec": 60, "price": 1.01, "timestamp": "2026-03-10T12:01:00Z"},
        ],
    }]), encoding="utf-8")

    calls = {"trend": 0, "scalp": 0}

    def fake_trend_exit(position_ctx, current, settings):
        calls["trend"] += 1
        return {
            "exit_decision": "FULL_EXIT",
            "exit_reason": "trend_test_exit",
            "exit_flags": ["trend_test_exit"],
            "exit_warnings": [],
        }

    def fake_scalp_exit(position_ctx, current, settings):
        calls["scalp"] += 1
        return {
            "exit_decision": None,
            "exit_reason": None,
            "exit_flags": [],
            "exit_warnings": [],
        }

    monkeypatch.setattr(replay_harness, "evaluate_hard_exit", lambda position_ctx, current, settings: {
        "exit_decision": None,
        "exit_reason": None,
        "exit_flags": [],
        "exit_warnings": [],
    })
    monkeypatch.setattr(replay_harness, "evaluate_trend_exit", fake_trend_exit)
    monkeypatch.setattr(replay_harness, "evaluate_scalp_exit", fake_scalp_exit)

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_historical_regime_exit",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    assert calls["trend"] >= 1
    assert calls["scalp"] == 0
    assert result["artifacts"].trades[0]["exit_reason_final"] == "trend_test_exit"
