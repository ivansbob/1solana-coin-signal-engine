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


def _write_unresolved_fixture(
    artifact_dir: Path,
    *,
    entry_decision: str = "ENTER",
    regime_decision: str = "SCALP",
    entry_time: str | int = "2026-03-10T12:00:00Z",
    entry_price: float | None = 1.0,
    price_path: list[dict] | None = None,
    truncated: bool = False,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "entry_candidates.json").write_text(json.dumps([{
        "token_address": "tok_unresolved",
        "pair_address": "pair_unresolved",
        "entry_decision": entry_decision,
        "regime_decision": regime_decision,
        "entry_time": entry_time,
        "entry_price": entry_price,
    }]), encoding="utf-8")
    (artifact_dir / "scored_tokens.jsonl").write_text(json.dumps({
        "token_address": "tok_unresolved",
        "pair_address": "pair_unresolved",
        "symbol": "UNR",
        "entry_decision": entry_decision,
        "regime_decision": regime_decision,
        "entry_time": entry_time,
        "entry_price": entry_price,
        "price_usd": 1.0,
        "final_score": 40.0,
        "final_score_pre_wallet": 40.0,
        "entry_confidence": 0.8,
        "recommended_position_pct": 0.25,
        "effective_position_pct": 0.25,
        "base_position_pct": 0.25,
        "liquidity_usd": 30000.0,
        "buy_pressure": 0.8,
        "volume_velocity": 3.0,
    }) + chr(10), encoding="utf-8")
    row = {
        "token_address": "tok_unresolved",
        "pair_address": "pair_unresolved",
        "price_path": price_path or [],
    }
    if truncated:
        row["truncated"] = True
    (artifact_dir / "price_paths.json").write_text(json.dumps([row]), encoding="utf-8")


def test_historical_replay_emits_trade_and_matrix_for_missing_price_path(tmp_path):
    artifact_dir = tmp_path / "fixture_missing_price_path"
    _write_unresolved_fixture(artifact_dir, price_path=[])

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_missing_price_path",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    assert len(result["artifacts"].trades) == 1
    assert len(result["artifacts"].trade_feature_matrix) == 1
    trade = result["artifacts"].trades[0]
    position = result["artifacts"].positions[0]
    assert trade["replay_resolution_status"] == "unresolved"
    assert trade["replay_data_status"] == "historical_partial"
    assert "missing_price_path" in (trade.get("exit_warnings") or [])
    assert position["status"] == "open"
    assert position["resolution_status"] == "unresolved"


def test_historical_replay_emits_trade_and_matrix_for_truncated_price_path(tmp_path):
    artifact_dir = tmp_path / "fixture_truncated_price_path"
    _write_unresolved_fixture(
        artifact_dir,
        price_path=[
            {"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"},
            {"offset_sec": 15, "price": 1.01, "timestamp": "2026-03-10T12:00:15Z"},
        ],
        truncated=True,
    )

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_truncated_price_path",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    trade = result["artifacts"].trades[0]
    assert len(result["artifacts"].trade_feature_matrix) == 1
    assert trade["replay_resolution_status"] == "partial"
    assert "truncated_price_path" in (trade.get("exit_warnings") or [])


def test_historical_replay_emits_partial_trade_when_only_partial_exit_is_seen(tmp_path, monkeypatch):
    import src.replay.historical_replay_harness as replay_harness

    artifact_dir = tmp_path / "fixture_partial_exit_only"
    _write_unresolved_fixture(
        artifact_dir,
        regime_decision="TREND",
        price_path=[
            {"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"},
            {"offset_sec": 30, "price": 1.16, "timestamp": "2026-03-10T12:00:30Z"},
        ],
    )

    original_resolve_exit = replay_harness._resolve_exit

    def fake_resolve_exit(base_context, entry, token_payload, regime_decision, state, settings):
        payload = original_resolve_exit(base_context, entry, token_payload, regime_decision, state, settings)
        payload.update({
            "resolution_status": "partial",
            "replay_data_status": "historical_partial",
            "warning": "partial_exit_without_full_exit",
            "exit_decision": "PARTIAL_EXIT",
            "exit_reason_final": "trend_partial_take_profit_1",
            "exit_flags": ["partial_take_profit_1"],
            "exit_warnings": ["partial_exit_without_full_exit"],
        })
        return payload

    monkeypatch.setattr(replay_harness, "_resolve_exit", fake_resolve_exit)

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_partial_exit_only",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    trade = result["artifacts"].trades[0]
    assert trade["replay_resolution_status"] == "partial"
    assert trade["exit_decision"] == "PARTIAL_EXIT"
    assert trade["exit_reason_final"]
    assert len(result["artifacts"].trade_feature_matrix) == 1


def test_replay_uses_partial_historical_row_when_post_entry_points_exist(tmp_path):
    artifact_dir = tmp_path / "fixture_partial_usable"
    _write_unresolved_fixture(
        artifact_dir,
        price_path=[
            {"timestamp": 100, "offset_sec": 0, "price": 1.0},
            {"timestamp": 160, "offset_sec": 60, "price": 1.1},
            {"timestamp": 220, "offset_sec": 120, "price": 1.2},
        ],
    )
    rows = json.loads((artifact_dir / "price_paths.json").read_text(encoding="utf-8"))
    rows[0]["price_path_status"] = "partial"
    (artifact_dir / "price_paths.json").write_text(json.dumps(rows), encoding="utf-8")
    entries = json.loads((artifact_dir / "entry_candidates.json").read_text(encoding="utf-8"))
    entries[0]["entry_time"] = 100
    (artifact_dir / "entry_candidates.json").write_text(json.dumps(entries), encoding="utf-8")

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_partial_usable", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)

    assert result["summary"]["historical_rows_used"] == 1
    assert result["summary"]["partial_but_usable_rows"] == 1
    assert result["summary"]["unresolved_rows"] == 0


def test_replay_marks_unresolved_when_no_post_entry_points_exist(tmp_path):
    artifact_dir = tmp_path / "fixture_no_post_entry"
    _write_unresolved_fixture(
        artifact_dir,
        price_path=[
            {"timestamp": 100, "offset_sec": 0, "price": 1.0},
            {"timestamp": 120, "offset_sec": 20, "price": 1.1},
        ],
    )
    rows = json.loads((artifact_dir / "price_paths.json").read_text(encoding="utf-8"))
    rows[0]["price_path_status"] = "partial"
    (artifact_dir / "price_paths.json").write_text(json.dumps(rows), encoding="utf-8")
    entries = json.loads((artifact_dir / "entry_candidates.json").read_text(encoding="utf-8"))
    entries[0]["entry_time"] = 200
    (artifact_dir / "entry_candidates.json").write_text(json.dumps(entries), encoding="utf-8")

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_no_post_entry", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    trade = result["artifacts"].trades[0]
    assert trade["replay_resolution_status"] == "unresolved"
    assert "missing_price_path" in (trade.get("exit_warnings") or [])


def test_replay_summary_counts_partial_but_usable_rows(tmp_path):
    artifact_dir = FIXTURES / "full_win"
    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_summary_partial_usable", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    assert "partial_but_usable_rows" in result["summary"]
    assert "missing_price_path_rows" in result["summary"]


def test_replay_generates_trade_observation_from_gap_filled_partial_path(tmp_path):
    artifact_dir = tmp_path / "fixture_gap_fill_partial"
    _write_unresolved_fixture(
        artifact_dir,
        price_path=[
            {"timestamp": 100, "offset_sec": 0, "price": 1.0},
            {"timestamp": 160, "offset_sec": 60, "price": 1.05, "gap_filled": True},
            {"timestamp": 220, "offset_sec": 120, "price": 1.1, "gap_filled": True},
        ],
    )
    rows = json.loads((artifact_dir / "price_paths.json").read_text(encoding="utf-8"))
    rows[0]["price_path_status"] = "partial"
    rows[0]["gap_fill_applied"] = True
    (artifact_dir / "price_paths.json").write_text(json.dumps(rows), encoding="utf-8")
    entries = json.loads((artifact_dir / "entry_candidates.json").read_text(encoding="utf-8"))
    entries[0]["entry_time"] = 100
    (artifact_dir / "entry_candidates.json").write_text(json.dumps(entries), encoding="utf-8")

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_gap_fill_partial", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    assert len(result["artifacts"].trades) == 1
    assert result["summary"]["gap_filled_rows_used"] >= 1


def test_replay_derives_entry_price_from_historical_observation_when_missing_in_payload(tmp_path):
    artifact_dir = tmp_path / "fixture_entry_bridge_exact_timestamp"
    _write_unresolved_fixture(
        artifact_dir,
        entry_price=None,
        entry_time=1774158360,
        price_path=[
            {"offset_sec": 0, "price": 1.0, "timestamp": 1774158360},
            {"offset_sec": 30, "price": 1.1, "timestamp": 1774158390},
        ],
    )

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_entry_bridge_exact", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    trade = result["artifacts"].trades[0]
    assert trade["entry_price"] == 1.0
    assert trade["entry_price_source"] == "historical_price_path"
    assert trade["entry_price_timestamp"] == 1774158360


def test_replay_uses_first_post_entry_observation_for_entry_price(tmp_path):
    artifact_dir = tmp_path / "fixture_entry_bridge_first_post_entry"
    _write_unresolved_fixture(
        artifact_dir,
        entry_price=None,
        entry_time=1774158340,
        price_path=[
            {"offset_sec": 0, "price": 1.05, "timestamp": 1774158360},
            {"offset_sec": 30, "price": 1.1, "timestamp": 1774158390},
        ],
    )

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_entry_bridge_first_post", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    trade = result["artifacts"].trades[0]
    assert trade["entry_price"] == 1.05
    assert trade["entry_price_source"] == "historical_price_path"
    assert trade["entry_price_timestamp"] == 1774158360


def test_replay_keeps_entry_price_null_when_no_usable_observation_exists(tmp_path):
    artifact_dir = tmp_path / "fixture_entry_bridge_no_usable_observation"
    _write_unresolved_fixture(
        artifact_dir,
        entry_price=None,
        entry_time=1774158500,
        price_path=[
            {"offset_sec": 0, "price": 1.0, "timestamp": 1774158360},
            {"offset_sec": 30, "price": 1.1, "timestamp": 1774158390},
        ],
    )

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_entry_bridge_none", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    trade = result["artifacts"].trades[0]
    assert trade["entry_price"] is None
    assert trade["replay_data_status"] == "historical_partial"


def test_replay_accepts_gecko_timestamp_field_for_entry_price_bridge(tmp_path):
    artifact_dir = tmp_path / "fixture_entry_bridge_gecko_shape"
    _write_unresolved_fixture(
        artifact_dir,
        entry_price=None,
        entry_time=1774158360,
        price_path=[
            {"offset_sec": 0, "price": 2.5e-06, "timestamp": 1774158360, "volume": 1758.95},
            {"offset_sec": 45, "price": 3.0e-06, "timestamp": 1774158405, "volume": 1942.11},
        ],
    )

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_entry_bridge_gecko", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    trade = result["artifacts"].trades[0]
    assert trade["entry_price"] == 2.5e-06
    assert trade["entry_price_source"] == "historical_price_path"
    assert trade["entry_price_timestamp"] == 1774158360


def test_replay_generates_non_null_pnl_when_entry_and_exit_prices_resolve_from_historical_path(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "fixture_entry_bridge_pnl"
    _write_unresolved_fixture(
        artifact_dir,
        entry_price=None,
        entry_time=1774158360,
        price_path=[
            {"offset_sec": 0, "price": 1.0, "timestamp": 1774158360},
            {"offset_sec": 60, "price": 1.2, "timestamp": 1774158420},
        ],
    )

    monkeypatch.setattr(replay_harness, "evaluate_hard_exit", lambda position_ctx, current, settings: {
        "exit_decision": None,
        "exit_reason": None,
        "exit_flags": [],
        "exit_warnings": [],
    })
    monkeypatch.setattr(replay_harness, "evaluate_scalp_exit", lambda position_ctx, current, settings: {
        "exit_decision": "FULL_EXIT" if current.get("hold_sec", 0) >= 60 else None,
        "exit_reason": "test_full_exit" if current.get("hold_sec", 0) >= 60 else None,
        "exit_flags": ["test_full_exit"] if current.get("hold_sec", 0) >= 60 else [],
        "exit_warnings": [],
    })

    result = run_historical_replay(artifact_dir=artifact_dir, run_id="unit_entry_bridge_pnl", config_path=ROOT / "config" / "replay.default.yaml", output_base_dir=tmp_path, dry_run=True)
    trade = result["artifacts"].trades[0]
    assert trade["entry_price"] == 1.0
    assert trade["exit_price"] == 1.2
    assert trade["gross_pnl_pct"] is not None
    assert trade["net_pnl_pct"] is not None


def test_historical_replay_ignored_token_still_does_not_emit_trade(tmp_path):
    artifact_dir = tmp_path / "fixture_ignored_emit_contract"
    _write_unresolved_fixture(artifact_dir, entry_decision="IGNORE")

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_ignored_emit_contract",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    assert result["artifacts"].trades == []
    assert result["artifacts"].trade_feature_matrix == []


def test_historical_replay_summary_counts_opened_unresolved_positions(tmp_path, monkeypatch):
    import src.replay.historical_replay_harness as replay_harness

    base = tmp_path / "fixture_summary_counts"
    artifact_dir = base / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    entries = [
        {"token_address": "tok_resolved", "pair_address": "pair_resolved", "entry_decision": "ENTER", "regime_decision": "SCALP", "entry_time": "2026-03-10T12:00:00Z", "entry_price": 1.0},
        {"token_address": "tok_unresolved", "pair_address": "pair_unresolved", "entry_decision": "ENTER", "regime_decision": "SCALP", "entry_time": "2026-03-10T12:00:00Z", "entry_price": 1.0},
        {"token_address": "tok_ignored", "pair_address": "pair_ignored", "entry_decision": "IGNORE", "regime_decision": "SCALP", "entry_time": "2026-03-10T12:00:00Z", "entry_price": 1.0},
    ]
    (artifact_dir / "entry_candidates.json").write_text(json.dumps(entries), encoding="utf-8")
    scored = [
        {"token_address": "tok_resolved", "pair_address": "pair_resolved", "symbol": "RES", "entry_decision": "ENTER", "regime_decision": "SCALP", "entry_time": "2026-03-10T12:00:00Z", "entry_price": 1.0, "price_usd": 1.0, "final_score": 40.0, "final_score_pre_wallet": 40.0, "entry_confidence": 0.8, "recommended_position_pct": 0.25, "effective_position_pct": 0.25, "base_position_pct": 0.25, "liquidity_usd": 30000.0, "buy_pressure": 0.8, "volume_velocity": 3.0},
        {"token_address": "tok_unresolved", "pair_address": "pair_unresolved", "symbol": "UNR", "entry_decision": "ENTER", "regime_decision": "SCALP", "entry_time": "2026-03-10T12:00:00Z", "entry_price": 1.0, "price_usd": 1.0, "final_score": 40.0, "final_score_pre_wallet": 40.0, "entry_confidence": 0.8, "recommended_position_pct": 0.25, "effective_position_pct": 0.25, "base_position_pct": 0.25, "liquidity_usd": 30000.0, "buy_pressure": 0.8, "volume_velocity": 3.0},
        {"token_address": "tok_ignored", "pair_address": "pair_ignored", "symbol": "IGN", "entry_decision": "IGNORE", "regime_decision": "SCALP", "entry_time": "2026-03-10T12:00:00Z", "entry_price": 1.0, "price_usd": 1.0, "final_score": 40.0, "final_score_pre_wallet": 40.0, "entry_confidence": 0.8, "recommended_position_pct": 0.25, "effective_position_pct": 0.25, "base_position_pct": 0.25, "liquidity_usd": 30000.0, "buy_pressure": 0.8, "volume_velocity": 3.0},
    ]
    (artifact_dir / "scored_tokens.jsonl").write_text("".join(json.dumps(row) + chr(10) for row in scored), encoding="utf-8")
    (artifact_dir / "price_paths.json").write_text(json.dumps([
        {"token_address": "tok_resolved", "pair_address": "pair_resolved", "price_path": [{"offset_sec": 0, "price": 1.0, "timestamp": "2026-03-10T12:00:00Z"}]},
        {"token_address": "tok_unresolved", "pair_address": "pair_unresolved", "price_path": []},
    ]), encoding="utf-8")

    original_resolve_exit = replay_harness._resolve_exit

    def fake_resolve_exit(base_context, entry, token_payload, regime_decision, state, settings):
        token = base_context.get("token_address") or token_payload.get("token_address")
        if token == "tok_resolved":
            return {
                "resolution_status": "resolved",
                "replay_data_status": "historical",
                "exit_decision": "FULL_EXIT",
                "exit_reason_final": "scalp_take_profit",
                "exit_flags": ["take_profit"],
                "exit_warnings": [],
                "exit_price": 1.18,
                "exit_time": "2026-03-10T12:00:35Z",
                "hold_sec": 35,
                "gross_pnl_pct": 18.0,
                "net_pnl_pct": 17.0,
            }
        return original_resolve_exit(base_context, entry, token_payload, regime_decision, state, settings)

    monkeypatch.setattr(replay_harness, "_resolve_exit", fake_resolve_exit)

    result = run_historical_replay(
        artifact_dir=artifact_dir,
        run_id="unit_summary_counts",
        config_path=ROOT / "config" / "replay.default.yaml",
        output_base_dir=tmp_path,
        dry_run=True,
    )

    summary = result["summary"]
    assert summary["opened_positions"] == 2
    assert summary["unresolved_open_positions"] == 1
    assert summary["ignored_rows"] == 1
    assert summary["trades"] == 2
    assert summary["trade_feature_matrix_rows"] == 2
