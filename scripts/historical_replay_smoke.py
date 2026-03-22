#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.replay.historical_replay_harness import run_historical_replay
from utils.io import ensure_dir, write_json


def _require_finite_number(value: object, *, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AssertionError(f"historical_replay_smoke: {label} must be numeric, got {value!r}") from exc
    if not math.isfinite(number):
        raise AssertionError(f"historical_replay_smoke: {label} must be finite, got {number!r}")
    return number


def _assert_historical_replay_economic_sanity(result: dict[str, object]) -> dict[str, object]:
    artifacts = result["artifacts"]
    trades = list(artifacts.trades)
    if not trades:
        raise AssertionError("historical_replay_smoke: replay produced no trades")

    trade = trades[0]
    resolution_status = trade.get("replay_resolution_status")
    if resolution_status != "resolved":
        raise AssertionError(
            f"historical_replay_smoke: expected resolved winning fixture, got {resolution_status!r}"
        )

    gross_pnl_pct = _require_finite_number(trade.get("gross_pnl_pct"), label="gross_pnl_pct")
    net_pnl_pct = _require_finite_number(trade.get("net_pnl_pct"), label="net_pnl_pct")
    if net_pnl_pct >= gross_pnl_pct:
        raise AssertionError(
            "historical_replay_smoke: expected net_pnl_pct to stay below gross_pnl_pct "
            f"(gross={gross_pnl_pct}, net={net_pnl_pct})"
        )
    if net_pnl_pct <= 0:
        raise AssertionError(
            "historical_replay_smoke: winning fixture must remain profitable after friction "
            f"(net_pnl_pct={net_pnl_pct})"
        )

    return {
        "gross_pnl_pct": round(gross_pnl_pct, 6),
        "net_pnl_pct": round(net_pnl_pct, 6),
        "economic_sanity_status": "ok",
        "economic_sanity_reason": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Deterministic historical replay smoke runner')
    parser.add_argument('--output-base-dir', default=str(REPO_ROOT / 'data' / 'smoke'), help='Base directory for isolated historical replay smoke artifacts')
    args = parser.parse_args()

    output_base = ensure_dir(Path(args.output_base_dir).expanduser().resolve())
    result = run_historical_replay(
        artifact_dir=REPO_ROOT / 'tests' / 'fixtures' / 'historical_replay' / 'full_win',
        run_id='historical_replay_smoke',
        config_path=REPO_ROOT / 'config' / 'replay.default.yaml',
        wallet_weighting='off',
        dry_run=True,
        output_base_dir=output_base,
        allow_synthetic_smoke=False,
    )
    run_dir = Path(result['outputs']['run_dir'])
    summary = result['summary'].copy()
    summary.update(_assert_historical_replay_economic_sanity(result))
    summary.pop('summary_markdown', None)
    write_json(output_base / 'historical_replay_summary.json', summary)
    (output_base / 'historical_replay_summary.md').write_text(result['summary']['summary_markdown'], encoding='utf-8')
    (output_base / 'historical_trade_feature_matrix.jsonl').write_text(
        (run_dir / 'trade_feature_matrix.jsonl').read_text(encoding='utf-8'),
        encoding='utf-8',
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
