from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_historical_replay_smoke_script_runs_and_writes_outputs():
    completed = subprocess.run(
        [sys.executable, "scripts/historical_replay_smoke.py"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    summary_path = ROOT / "data" / "smoke" / "historical_replay_summary.json"
    assert payload["historical_rows_used"] == 1
    assert payload["economic_sanity_status"] == "ok"
    assert payload["gross_pnl_pct"] is not None
    assert payload["net_pnl_pct"] is not None
    assert payload["net_pnl_pct"] > 0
    assert payload["net_pnl_pct"] < payload["gross_pnl_pct"]
    assert summary_path.exists()
    assert (ROOT / "data" / "smoke" / "historical_trade_feature_matrix.jsonl").exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["economic_sanity_status"] == "ok"
    assert summary["historical_rows_used"] == 1
    assert summary["net_pnl_pct"] > 0
    assert summary["net_pnl_pct"] < summary["gross_pnl_pct"]
