#!/usr/bin/env python3
"""Smoke test for the canonical historical-replay -> runtime -> paper bridge."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import load_settings
from paper_runner import run_paper_cycle
from src.promotion.runtime_signal_adapter import adapt_runtime_signal_batch
from src.promotion.runtime_signal_loader import load_latest_runtime_signal_batch
from utils.clock import utc_now_iso
from utils.io import ensure_dir, materialize_jsonl, read_json, write_json


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )


def _write_json_payload(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _canonical_replay_row() -> dict:
    signal_ts = utc_now_iso()
    return {
        "schema_version": "trade_feature_matrix.v1",
        "token_address": "So11111111111111111111111111111111111111112",
        "pair_address": "pair_paper_canonical_smoke",
        "symbol": "CANON",
        "signal_ts": signal_ts,
        "decision": "ENTER",
        "regime_decision": "SCALP",
        "entry_decision": "SCALP",
        "recommended_position_pct": 0.75,
        "base_position_pct": 0.5,
        "effective_position_pct": 0.3,
        "sizing_multiplier": 0.6,
        "sizing_origin": "historical_replay_canonical",
        "sizing_reason_codes": [
            "historical_replay_canonical_bridge",
            "preserve_precomputed_effective_position_pct",
        ],
        "sizing_confidence": 0.86,
        "evidence_quality_score": 0.78,
        "evidence_conflict_flag": False,
        "partial_evidence_flag": False,
        "entry_confidence": 0.84,
        "entry_reason": "canonical_replay_bridge_smoke",
        "entry_snapshot": {
            "price_usd": 1.0,
            "liquidity_usd": 1_000_000,
            "x_validation_score": 82,
            "buy_pressure": 0.82,
            "volume_velocity": 4.2,
        },
        "x_status": "healthy",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic paper-trader smoke over the canonical replay bridge.")
    parser.add_argument("--base-dir", default=str(REPO_ROOT / "data/smoke/paper_trader"))
    args = parser.parse_args()

    smoke_dir = Path(args.base_dir).expanduser().resolve()
    if smoke_dir.exists():
        shutil.rmtree(smoke_dir)
    ensure_dir(smoke_dir)
    processed_dir = ensure_dir(smoke_dir / "processed")

    os.environ["SMOKE_DIR"] = str(smoke_dir)
    os.environ["PROCESSED_DATA_DIR"] = str(processed_dir)
    os.environ["PAPER_FAILED_TX_BASE_PROB"] = "0"
    os.environ["PAPER_FAILED_TX_LOW_LIQUIDITY_ADDON"] = "0"
    os.environ["PAPER_FAILED_TX_HIGH_VOLATILITY_ADDON"] = "0"
    os.environ["PAPER_PARTIAL_FILL_ALLOWED"] = "false"

    settings = load_settings()
    replay_row = _canonical_replay_row()

    _write_jsonl(processed_dir / "trade_feature_matrix.jsonl", [replay_row])
    write_json(
        processed_dir / "runtime_signal_pipeline_manifest.json",
        {
            "pipeline_run_id": "paper_trader_smoke",
            "pipeline_status": "ok",
            "pipeline_origin": "historical_replay",
        },
    )
    _write_json_payload(processed_dir / "exit_decisions.json", [])
    _write_json_payload(
        processed_dir / "market_states.json",
        [
            {
                "token_address": replay_row["token_address"],
                "price_usd": 1.0,
                "liquidity_usd": 1_000_000,
                "volatility": 0.12,
                "sol_usd": 100.0,
            }
        ],
    )

    batch = load_latest_runtime_signal_batch(processed_dir, stale_after_sec=None)
    assert batch["selected_origin"] == "historical_replay"
    assert batch["origin_tier"] == "canonical"
    assert str(batch["selected_artifact"]).endswith("trade_feature_matrix.jsonl")

    runtime_signals = adapt_runtime_signal_batch(
        batch["signals"],
        runtime_signal_origin=batch.get("selected_origin") or "unknown",
        source_artifact=batch.get("selected_artifact"),
        runtime_origin_tier=batch.get("origin_tier"),
        runtime_pipeline_origin=batch.get("runtime_pipeline_origin"),
        runtime_pipeline_status=batch.get("runtime_pipeline_status"),
        runtime_pipeline_manifest=batch.get("runtime_pipeline_manifest"),
    )
    assert len(runtime_signals) == 1
    runtime_signal = runtime_signals[0]

    assert runtime_signal["runtime_signal_origin"] == "historical_replay"
    assert runtime_signal["runtime_origin_tier"] == "canonical"
    assert runtime_signal["source_artifact"] and str(runtime_signal["source_artifact"]).endswith("trade_feature_matrix.jsonl")
    assert runtime_signal["base_position_pct"] == replay_row["base_position_pct"]
    assert runtime_signal["effective_position_pct"] == replay_row["effective_position_pct"]
    assert runtime_signal["sizing_multiplier"] == replay_row["sizing_multiplier"]
    assert runtime_signal["sizing_origin"] == replay_row["sizing_origin"]
    assert runtime_signal["sizing_reason_codes"] == replay_row["sizing_reason_codes"]
    assert runtime_signal["sizing_confidence"] == replay_row["sizing_confidence"]
    assert runtime_signal["evidence_quality_score"] == replay_row["evidence_quality_score"]
    assert runtime_signal["evidence_conflict_flag"] is replay_row["evidence_conflict_flag"]
    assert runtime_signal["partial_evidence_flag"] is replay_row["partial_evidence_flag"]

    _write_json_payload(processed_dir / "entry_candidates.json", runtime_signals)

    state = run_paper_cycle(settings)

    materialize_jsonl(processed_dir / "signals.jsonl")
    materialize_jsonl(processed_dir / "trades.jsonl")
    positions_payload = {
        "positions": state.get("positions", []),
        "next_position_seq": state.get("next_position_seq", 1),
        "next_trade_seq": state.get("next_trade_seq", 1),
    }
    write_json(smoke_dir / "positions.smoke.json", positions_payload)
    write_json(smoke_dir / "portfolio_state.smoke.json", state.get("portfolio", {}))
    (smoke_dir / "signals.smoke.jsonl").write_text((processed_dir / "signals.jsonl").read_text(encoding="utf-8"), encoding="utf-8")
    (smoke_dir / "trades.smoke.jsonl").write_text((processed_dir / "trades.jsonl").read_text(encoding="utf-8"), encoding="utf-8")

    positions = positions_payload["positions"]
    assert len(positions) == 1
    position = positions[0]
    assert position["base_position_pct"] == replay_row["base_position_pct"]
    assert position["effective_position_pct"] == replay_row["effective_position_pct"]
    assert position["sizing_multiplier"] == replay_row["sizing_multiplier"]
    assert position["sizing_origin"] == replay_row["sizing_origin"]
    assert position["sizing_reason_codes"] == replay_row["sizing_reason_codes"]
    assert position["sizing_confidence"] == replay_row["sizing_confidence"]
    assert position["evidence_quality_score"] == replay_row["evidence_quality_score"]
    assert position["evidence_conflict_flag"] is replay_row["evidence_conflict_flag"]
    assert position["partial_evidence_flag"] is replay_row["partial_evidence_flag"]

    trade_rows = [
        json.loads(line)
        for line in (smoke_dir / "trades.smoke.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    paper_buy = next(row for row in trade_rows if row.get("event") == "paper_buy")
    assert paper_buy["requested_effective_position_pct"] == replay_row["effective_position_pct"]
    assert paper_buy["effective_position_pct"] == replay_row["effective_position_pct"]
    assert paper_buy["base_position_pct"] == replay_row["base_position_pct"]
    assert paper_buy["sizing_multiplier"] == replay_row["sizing_multiplier"]
    assert paper_buy["sizing_origin"] == replay_row["sizing_origin"]
    assert paper_buy["sizing_reason_codes"] == replay_row["sizing_reason_codes"]

    summary = {
        "runtime_signal_origin": runtime_signal["runtime_signal_origin"],
        "runtime_origin_tier": runtime_signal["runtime_origin_tier"],
        "selected_artifact": batch["selected_artifact"],
        "recommended_position_pct": runtime_signal["recommended_position_pct"],
        "base_position_pct": position["base_position_pct"],
        "effective_position_pct": position["effective_position_pct"],
        "requested_effective_position_pct": paper_buy["requested_effective_position_pct"],
        "sizing_origin": paper_buy["sizing_origin"],
        "sizing_reason_codes": paper_buy["sizing_reason_codes"],
    }
    write_json(smoke_dir / "paper_trader_summary.json", summary)
    print(json.dumps(summary, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
