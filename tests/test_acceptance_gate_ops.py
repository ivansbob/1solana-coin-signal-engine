import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.acceptance_gate import build_blocks, validate_required_operational_outputs


def test_acceptance_gate_detects_missing_runtime_outputs(tmp_path):
    missing = validate_required_operational_outputs(tmp_path, "runtime_signal_smoke")
    assert any("runtime_signal_summary.json" in item for item in missing)


def test_acceptance_gate_accepts_present_runtime_outputs(tmp_path):
    base = tmp_path / "runtime_signal"
    (base / "runs" / "runtime_signal_smoke").mkdir(parents=True, exist_ok=True)
    (base / "runtime_signal_summary.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    (base / "runs" / "runtime_signal_smoke" / "runtime_health.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    (base / "runs" / "runtime_signal_smoke" / "artifact_manifest.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    missing = validate_required_operational_outputs(tmp_path, "runtime_signal_smoke")
    assert missing == []


def test_acceptance_gate_detects_missing_historical_replay_outputs(tmp_path):
    missing = validate_required_operational_outputs(tmp_path, "historical_replay_smoke")
    assert any("historical_replay/historical_replay_smoke/replay_summary.json" in item for item in missing)
    assert any("historical_replay/historical_replay_smoke/manifest.json" in item for item in missing)
    assert any("historical_replay/historical_replay_summary.json" in item for item in missing)


def test_acceptance_gate_accepts_corrected_historical_replay_outputs(tmp_path):
    run_dir = tmp_path / "historical_replay" / "historical_replay_smoke"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "replay_summary.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    (run_dir / "manifest.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    (tmp_path / "historical_replay" / "historical_replay_summary.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    missing = validate_required_operational_outputs(tmp_path, "historical_replay_smoke")
    assert missing == []


def test_acceptance_gate_build_blocks_includes_replay_block_before_runtime(tmp_path):
    names = [block.name for block in build_blocks(tmp_path, skip_smokes=True)]
    assert "historical_replay_sanity" in names
    assert names.index("historical_replay_sanity") < names.index("runtime_replay_integrity")
