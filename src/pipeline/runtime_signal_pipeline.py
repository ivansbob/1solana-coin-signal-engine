from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from collectors.discovery_engine import run_discovery_once
from scoring.unified_score import ensure_list, load_json, score_tokens, write_json, write_jsonl
from utils.clock import utc_now_iso
from utils.io import ensure_dir

from src.pipeline.entry_stage import run_stage as run_entry_stage
from src.pipeline.env import pipeline_env
from src.pipeline.onchain_enrichment_stage import run_stage as run_onchain_enrichment_stage
from src.pipeline.rug_stage import run_stage as run_rug_stage
from src.pipeline.x_validation_stage import run_stage as run_x_validation_stage

StageRunner = Callable[..., dict[str, Any]]


@dataclass(frozen=True)
class StageSpec:
    name: str
    artifact_name: str
    runner: StageRunner


def _artifact_path(processed_dir: Path, filename: str) -> Path:
    return processed_dir / filename


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("tokens", "shortlist", "candidates", "items", "rows", "market_states"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _write_atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _manifest_base(processed_dir: Path) -> dict[str, Any]:
    return {
        "pipeline_run_id": f"runtime_pipeline_{utc_now_iso()}",
        "pipeline_status": "ok",
        "generated_at": utc_now_iso(),
        "processed_dir": str(processed_dir),
        "stage_statuses": {},
        "stage_row_counts": {},
        "artifact_paths": {},
        "warnings": [],
        "selected_wallet_weighting_mode": os.environ.get("WALLET_WEIGHTING_MODE", "shadow"),
        "score_contract_version": None,
        "entry_contract_version": None,
    }


def _record_stage(manifest: dict[str, Any], *, name: str, artifact_path: Path, payload: dict[str, Any] | None, status: str, warning: str | None = None) -> None:
    rows = _extract_rows(payload) if payload is not None else []
    manifest["stage_statuses"][name] = status
    manifest["stage_row_counts"][name] = len(rows)
    manifest["artifact_paths"][name] = str(artifact_path)
    if warning:
        manifest["warnings"].append(f"{name}:{warning}")
    if payload and name == "scoring":
        manifest["score_contract_version"] = payload.get("contract_version")
    if payload and name == "entry":
        manifest["entry_contract_version"] = payload.get("contract_version")
    if status not in {"ok", "skipped"} and manifest["pipeline_status"] == "ok":
        manifest["pipeline_status"] = "partial"


def _load_rows_from_path(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    return ensure_list(payload)


def _run_discovery(*, processed_dir: Path) -> dict[str, Any]:
    with pipeline_env(processed_dir=processed_dir):
        result = run_discovery_once()
    return result["shortlist"]


def _run_scoring(*, processed_dir: Path, shortlist_path: Path, x_validated_path: Path, enriched_path: Path, rug_path: Path) -> dict[str, Any]:
    shortlist = _load_rows_from_path(shortlist_path)
    x_validated = _load_rows_from_path(x_validated_path)
    enriched = _load_rows_from_path(enriched_path)
    rug_assessed = _load_rows_from_path(rug_path)
    scored, events = score_tokens(shortlist, x_validated, enriched, rug_assessed)
    payload = {"generated_at": utc_now_iso(), "contract_version": "scored_tokens_v1", "tokens": scored}
    write_json(processed_dir / "scored_tokens.json", payload)
    write_jsonl(processed_dir / "score_events.jsonl", events)
    return payload


def _row_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        token = str(row.get("token_address") or row.get("mint") or "").strip()
        if token:
            out[token] = row
    return out


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _snapshot_from(*rows: dict[str, Any]) -> dict[str, Any]:
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("entry_snapshot"), dict):
            return dict(row.get("entry_snapshot") or {})
    return {}


def _build_market_states_payload(
    *,
    entry_payload: dict[str, Any],
    scored_payload: dict[str, Any],
    enriched_payload: dict[str, Any],
    x_validated_payload: dict[str, Any],
    shortlist_payload: dict[str, Any],
) -> dict[str, Any]:
    entry_rows = _extract_rows(entry_payload)
    scored_rows = _extract_rows(scored_payload)
    enriched_rows = _extract_rows(enriched_payload)
    x_validated_rows = _extract_rows(x_validated_payload)
    shortlist_rows = _extract_rows(shortlist_payload)

    entry_index = _row_index(entry_rows)
    scored_index = _row_index(scored_rows)
    enriched_index = _row_index(enriched_rows)
    x_validated_index = _row_index(x_validated_rows)
    shortlist_index = _row_index(shortlist_rows)

    ordered_tokens: list[str] = []
    for rows in (entry_rows, scored_rows, enriched_rows, x_validated_rows, shortlist_rows):
        for row in rows:
            token = str(row.get("token_address") or row.get("mint") or "").strip()
            if token and token not in ordered_tokens:
                ordered_tokens.append(token)

    generated_at = utc_now_iso()
    market_states: list[dict[str, Any]] = []
    for token in ordered_tokens:
        entry_row = entry_index.get(token, {})
        scored_row = scored_index.get(token, {})
        enriched_row = enriched_index.get(token, {})
        x_validated_row = x_validated_index.get(token, {})
        shortlist_row = shortlist_index.get(token, {})
        entry_snapshot = _snapshot_from(entry_row, scored_row, enriched_row, x_validated_row, shortlist_row)

        price_now = _first_present(
            entry_row.get("price_usd_now"),
            entry_row.get("price_usd"),
            entry_snapshot.get("price_usd"),
            scored_row.get("price_usd_now"),
            scored_row.get("price_usd"),
            enriched_row.get("price_usd_now"),
            enriched_row.get("price_usd"),
            shortlist_row.get("price_usd_now"),
            shortlist_row.get("price_usd"),
        )
        liquidity_now = _first_present(
            entry_row.get("liquidity_usd_now"),
            entry_row.get("liquidity_usd"),
            entry_snapshot.get("liquidity_usd"),
            scored_row.get("liquidity_usd_now"),
            scored_row.get("liquidity_usd"),
            enriched_row.get("liquidity_usd_now"),
            enriched_row.get("liquidity_usd"),
            shortlist_row.get("liquidity_usd_now"),
            shortlist_row.get("liquidity_usd"),
        )
        buy_pressure = _first_present(
            entry_row.get("buy_pressure_now"),
            entry_row.get("buy_pressure"),
            entry_snapshot.get("buy_pressure"),
            scored_row.get("buy_pressure_now"),
            scored_row.get("buy_pressure"),
            enriched_row.get("buy_pressure_now"),
            enriched_row.get("buy_pressure"),
        )
        volume_velocity = _first_present(
            entry_row.get("volume_velocity_now"),
            entry_row.get("volume_velocity"),
            entry_snapshot.get("volume_velocity"),
            scored_row.get("volume_velocity_now"),
            scored_row.get("volume_velocity"),
            enriched_row.get("volume_velocity_now"),
            enriched_row.get("volume_velocity"),
        )
        x_validation_score = _first_present(
            entry_row.get("x_validation_score_now"),
            entry_row.get("x_validation_score"),
            entry_snapshot.get("x_validation_score"),
            x_validated_row.get("x_validation_score"),
            scored_row.get("x_validation_score"),
        )
        x_status = _first_present(
            entry_row.get("x_status_now"),
            entry_row.get("x_status"),
            entry_snapshot.get("x_status"),
            x_validated_row.get("x_status"),
            scored_row.get("x_status"),
        )
        signal_ts = _first_present(
            entry_row.get("signal_ts"),
            scored_row.get("signal_ts"),
            x_validated_row.get("signal_ts"),
            shortlist_row.get("signal_ts"),
            entry_payload.get("generated_at"),
            scored_payload.get("generated_at"),
            generated_at,
        )

        market_states.append(
            {
                "token_address": token,
                "pair_address": _first_present(entry_row.get("pair_address"), scored_row.get("pair_address"), shortlist_row.get("pair_address")),
                "price_usd": price_now,
                "price_usd_now": price_now,
                "liquidity_usd": liquidity_now,
                "liquidity_usd_now": liquidity_now,
                "buy_pressure": buy_pressure,
                "buy_pressure_now": buy_pressure,
                "volume_velocity": volume_velocity,
                "volume_velocity_now": volume_velocity,
                "x_validation_score": x_validation_score,
                "x_validation_score_now": x_validation_score,
                "x_status": x_status,
                "x_status_now": x_status,
                "signal_ts": signal_ts,
                "generated_at": generated_at,
                "runtime_current_state_origin": "market_states_artifact",
                "runtime_current_state_status": "live_refresh",
                "runtime_current_state_warning": None,
                "runtime_current_state_confidence": 1.0,
            }
        )

    return {
        "generated_at": generated_at,
        "contract_version": "runtime_market_states_v1",
        "market_states": market_states,
    }


def run_runtime_signal_pipeline(
    *,
    processed_dir: str | Path = "data/processed",
    config_path: str | Path | None = None,
    discovery_enabled: bool = True,
    x_validation_enabled: bool = True,
    enrichment_enabled: bool = True,
    rug_enabled: bool = True,
    scoring_enabled: bool = True,
    entry_enabled: bool = True,
    stage_overrides: dict[str, str | Path] | None = None,
) -> dict[str, Any]:
    del config_path  # reserved for future config-aware orchestration
    overrides = {key: Path(value) for key, value in (stage_overrides or {}).items()}
    processed = ensure_dir(processed_dir)
    manifest = _manifest_base(processed)

    shortlist_path = overrides.get("shortlist", _artifact_path(processed, "shortlist.json"))
    x_validated_path = overrides.get("x_validated", _artifact_path(processed, "x_validated.json"))
    enriched_path = overrides.get("enriched", _artifact_path(processed, "enriched_tokens.json"))
    rug_path = overrides.get("rug", _artifact_path(processed, "rug_assessed_tokens.json"))
    scored_path = overrides.get("scored", _artifact_path(processed, "scored_tokens.json"))
    entry_path = overrides.get("entry", _artifact_path(processed, "entry_candidates.json"))
    market_states_path = overrides.get("market_states", _artifact_path(processed, "market_states.json"))

    if discovery_enabled and "shortlist" not in overrides:
        shortlist_payload = _run_discovery(processed_dir=processed)
        _record_stage(manifest, name="discovery", artifact_path=shortlist_path, payload=shortlist_payload, status="ok")
    elif shortlist_path.exists():
        shortlist_payload = load_json(shortlist_path)
        _record_stage(manifest, name="discovery", artifact_path=shortlist_path, payload=shortlist_payload, status="skipped")
    else:
        _record_stage(manifest, name="discovery", artifact_path=shortlist_path, payload=None, status="failed", warning="missing_shortlist_input")
        _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
        return manifest

    if x_validation_enabled and "x_validated" not in overrides:
        x_validated_payload = run_x_validation_stage(processed_dir=processed, shortlist_path=shortlist_path)
        _record_stage(manifest, name="x_validation", artifact_path=x_validated_path, payload=x_validated_payload, status="ok")
    elif x_validated_path.exists():
        x_validated_payload = load_json(x_validated_path)
        _record_stage(manifest, name="x_validation", artifact_path=x_validated_path, payload=x_validated_payload, status="skipped")
    else:
        _record_stage(manifest, name="x_validation", artifact_path=x_validated_path, payload=None, status="failed", warning="missing_x_validated_input")
        _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
        return manifest

    if enrichment_enabled and "enriched" not in overrides:
        enriched_payload = run_onchain_enrichment_stage(processed_dir=processed, shortlist_path=shortlist_path, x_validated_path=x_validated_path)
        _record_stage(manifest, name="enrichment", artifact_path=enriched_path, payload=enriched_payload, status="ok")
    elif enriched_path.exists():
        enriched_payload = load_json(enriched_path)
        _record_stage(manifest, name="enrichment", artifact_path=enriched_path, payload=enriched_payload, status="skipped")
    else:
        _record_stage(manifest, name="enrichment", artifact_path=enriched_path, payload=None, status="failed", warning="missing_enriched_input")
        _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
        return manifest

    if rug_enabled and "rug" not in overrides:
        rug_payload = run_rug_stage(processed_dir=processed, enriched_path=enriched_path)
        _record_stage(manifest, name="rug", artifact_path=rug_path, payload=rug_payload, status="ok")
    elif rug_path.exists():
        rug_payload = load_json(rug_path)
        _record_stage(manifest, name="rug", artifact_path=rug_path, payload=rug_payload, status="skipped")
    else:
        _record_stage(manifest, name="rug", artifact_path=rug_path, payload=None, status="failed", warning="missing_rug_input")
        _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
        return manifest

    if scoring_enabled and "scored" not in overrides:
        scored_payload = _run_scoring(processed_dir=processed, shortlist_path=shortlist_path, x_validated_path=x_validated_path, enriched_path=enriched_path, rug_path=rug_path)
        _record_stage(manifest, name="scoring", artifact_path=scored_path, payload=scored_payload, status="ok")
    elif scored_path.exists():
        scored_payload = load_json(scored_path)
        _record_stage(manifest, name="scoring", artifact_path=scored_path, payload=scored_payload, status="skipped")
    else:
        _record_stage(manifest, name="scoring", artifact_path=scored_path, payload=None, status="failed", warning="missing_scored_input")
        _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
        return manifest

    if entry_enabled and "entry" not in overrides:
        entry_payload = run_entry_stage(processed_dir=processed, scored_path=scored_path)
        _record_stage(manifest, name="entry", artifact_path=entry_path, payload=entry_payload, status="ok")
    elif entry_path.exists():
        entry_payload = load_json(entry_path)
        _record_stage(manifest, name="entry", artifact_path=entry_path, payload=entry_payload, status="skipped")
    else:
        _record_stage(manifest, name="entry", artifact_path=entry_path, payload=None, status="failed", warning="missing_entry_input")
        _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
        return manifest

    market_states_payload = _build_market_states_payload(
        entry_payload=entry_payload,
        scored_payload=scored_payload,
        enriched_payload=enriched_payload,
        x_validated_payload=x_validated_payload,
        shortlist_payload=shortlist_payload,
    )
    write_json(market_states_path, market_states_payload)
    _record_stage(manifest, name="market_states", artifact_path=market_states_path, payload=market_states_payload, status="ok")

    _write_atomic_json(processed / "runtime_signal_pipeline_manifest.json", manifest)
    return manifest
