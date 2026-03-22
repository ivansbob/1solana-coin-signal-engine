from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from utils.io import read_json


def test_paper_trader_smoke_confirms_canonical_bridge():
    cmd = [sys.executable, "scripts/paper_trader_smoke.py"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr

    smoke_dir = Path("data/smoke/paper_trader")
    summary = read_json(smoke_dir / "paper_trader_summary.json", default={})
    positions_payload = read_json(smoke_dir / "positions.smoke.json", default={})
    trades_path = smoke_dir / "trades.smoke.jsonl"

    assert summary["runtime_signal_origin"] == "historical_replay"
    assert summary["runtime_origin_tier"] == "canonical"
    assert summary["selected_artifact"].endswith("trade_feature_matrix.jsonl")
    assert trades_path.exists()
    assert (smoke_dir / "positions.smoke.json").exists()

    position = positions_payload["positions"][0]
    assert position["base_position_pct"] == 0.5
    assert position["effective_position_pct"] == 0.3
    assert position["sizing_multiplier"] == 0.6
    assert position["sizing_origin"] == "historical_replay_canonical"
    assert position["sizing_reason_codes"] == [
        "historical_replay_canonical_bridge",
        "preserve_precomputed_effective_position_pct",
    ]

    trades = [json.loads(line) for line in trades_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    paper_buy = next(row for row in trades if row.get("event") == "paper_buy")
    assert paper_buy["requested_effective_position_pct"] == 0.3
    assert paper_buy["effective_position_pct"] == 0.3
    assert paper_buy["sizing_origin"] == "historical_replay_canonical"
    assert paper_buy["sizing_reason_codes"] == [
        "historical_replay_canonical_bridge",
        "preserve_precomputed_effective_position_pct",
    ]

    stdout_payload = json.loads(result.stdout.strip())
    assert stdout_payload["runtime_signal_origin"] == "historical_replay"
    assert stdout_payload["runtime_origin_tier"] == "canonical"
