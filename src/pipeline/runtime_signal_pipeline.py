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


async def run_live_pipeline(
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
    helius_api_key: str | None = None,
    jupiter_client: Any | None = None,
    live_trader: Any | None = None,
    trading_enabled: bool = False,
) -> dict[str, Any]:
    """Async live pipeline with real-time streaming and trading."""
    import asyncio
    import logging
    from collectors.helius_ws_streamer import HeliusWsStreamer
    from src.ingest.jupiter_api_client import JupiterClient
    from trading.live_executor import LiveTrader
    from src.ingest.jito_priority_context import JitoPriorityContextAdapter
    from solders.keypair import Keypair
    import os

    logger = logging.getLogger(__name__)
    del config_path  # reserved for future config-aware orchestration
    overrides = {key: Path(value) for key, value in (stage_overrides or {}).items()}
    processed = ensure_dir(processed_dir)
    manifest = _manifest_base(processed)

    # Initialize live trading components
    helius_streamer = None
    jito_adapter = JitoPriorityContextAdapter()
    trader_keypair = None

    if trading_enabled:
        if not helius_api_key:
            helius_api_key = os.environ.get("HELIUS_API_KEY")

        if not jupiter_client:
            jupiter_client = JupiterClient()

        if not live_trader:
            # Initialize trader with keypair from environment
            private_key = os.environ.get("TRADER_PRIVATE_KEY")
            if private_key:
                trader_keypair = Keypair.from_base58_string(private_key)
                live_trader = LiveTrader(
                    payer_keypair=trader_keypair,
                    jito_client=None,  # Will be initialized in LiveTrader
                    jito_adapter=jito_adapter
                )

        # Initialize Helius streamer for real-time data
        if helius_api_key:
            helius_streamer = HeliusWsStreamer(api_key=helius_api_key)
            # Start streaming in background
            stream_task = asyncio.create_task(helius_streamer.start_stream())
            logger.info("Started Helius WebSocket streamer")

    try:
        # Run the standard pipeline stages (discovery, validation, etc.)
        manifest = await _run_live_pipeline_stages(
            manifest=manifest,
            processed=processed,
            overrides=overrides,
            discovery_enabled=discovery_enabled,
            x_validation_enabled=x_validation_enabled,
            enrichment_enabled=enrichment_enabled,
            rug_enabled=rug_enabled,
            scoring_enabled=scoring_enabled,
            entry_enabled=entry_enabled,
        )

        # If trading is enabled, process signals in real-time
        if trading_enabled and live_trader and helius_streamer:
            logger.info("Starting live trading mode...")
            await _run_live_trading_loop(
                manifest=manifest,
                processed=processed,
                helius_streamer=helius_streamer,
                live_trader=live_trader,
                jupiter_client=jupiter_client,
                jito_adapter=jito_adapter,
            )

        return manifest

    finally:
        # Cleanup
        if helius_streamer:
            await helius_streamer.stop_stream()
        if jupiter_client and hasattr(jupiter_client, '__aexit__'):
            await jupiter_client.__aexit__(None, None, None)


async def _run_live_pipeline_stages(
    manifest: dict[str, Any],
    processed: Path,
    overrides: dict[str, Path],
    discovery_enabled: bool,
    x_validation_enabled: bool,
    enrichment_enabled: bool,
    rug_enabled: bool,
    scoring_enabled: bool,
    entry_enabled: bool,
) -> dict[str, Any]:
    """Run the standard pipeline stages asynchronously."""
    # This is the same logic as the synchronous version but can be made async if needed
    shortlist_path = overrides.get("shortlist", _artifact_path(processed, "shortlist.json"))
    x_validated_path = overrides.get("x_validated", _artifact_path(processed, "x_validated.json"))
    enriched_path = overrides.get("enriched", _artifact_path(processed, "enriched_tokens.json"))
    rug_path = overrides.get("rug", _artifact_path(processed, "rug_assessed_tokens.json"))
    scored_path = overrides.get("scored", _artifact_path(processed, "scored_tokens.json"))
    entry_path = overrides.get("entry", _artifact_path(processed, "entry_candidates.json"))
    market_states_path = overrides.get("market_states", _artifact_path(processed, "market_states.json"))

    # Discovery stage
    if discovery_enabled and "shortlist" not in overrides:
        shortlist_payload = _run_discovery(processed_dir=processed)
        _record_stage(manifest, name="discovery", artifact_path=shortlist_path, payload=shortlist_payload, status="ok")
    elif shortlist_path.exists():
        shortlist_payload = load_json(shortlist_path)
        _record_stage(manifest, name="discovery", artifact_path=shortlist_path, payload=shortlist_payload, status="skipped")
    else:
        _record_stage(manifest, name="discovery", artifact_path=shortlist_path, payload=None, status="failed", warning="missing_shortlist_input")
        return manifest

    # X validation stage
    if x_validation_enabled and "x_validated" not in overrides:
        x_validated_payload = run_x_validation_stage(processed_dir=processed, shortlist_path=shortlist_path)
        _record_stage(manifest, name="x_validation", artifact_path=x_validated_path, payload=x_validated_payload, status="ok")
    elif x_validated_path.exists():
        x_validated_payload = load_json(x_validated_path)
        _record_stage(manifest, name="x_validation", artifact_path=x_validated_path, payload=x_validated_payload, status="skipped")
    else:
        _record_stage(manifest, name="x_validation", artifact_path=x_validated_path, payload=None, status="failed", warning="missing_x_validated_input")
        return manifest

    # Enrichment stage
    if enrichment_enabled and "enriched" not in overrides:
        enriched_payload = run_onchain_enrichment_stage(processed_dir=processed, shortlist_path=shortlist_path, x_validated_path=x_validated_path)
        _record_stage(manifest, name="enrichment", artifact_path=enriched_path, payload=enriched_payload, status="ok")
    elif enriched_path.exists():
        enriched_payload = load_json(enriched_path)
        _record_stage(manifest, name="enrichment", artifact_path=enriched_path, payload=enriched_payload, status="skipped")
    else:
        _record_stage(manifest, name="enrichment", artifact_path=enriched_path, payload=None, status="failed", warning="missing_enriched_input")
        return manifest

    # Rug stage
    if rug_enabled and "rug" not in overrides:
        rug_payload = run_rug_stage(processed_dir=processed, enriched_path=enriched_path)
        _record_stage(manifest, name="rug", artifact_path=rug_path, payload=rug_payload, status="ok")
    elif rug_path.exists():
        rug_payload = load_json(rug_path)
        _record_stage(manifest, name="rug", artifact_path=rug_path, payload=rug_payload, status="skipped")
    else:
        _record_stage(manifest, name="rug", artifact_path=rug_path, payload=None, status="failed", warning="missing_rug_input")
        return manifest

    # Scoring stage
    if scoring_enabled and "scored" not in overrides:
        scored_payload = _run_scoring(processed_dir=processed, shortlist_path=shortlist_path, x_validated_path=x_validated_path, enriched_path=enriched_path, rug_path=rug_path)
        _record_stage(manifest, name="scoring", artifact_path=scored_path, payload=scored_payload, status="ok")
    elif scored_path.exists():
        scored_payload = load_json(scored_path)
        _record_stage(manifest, name="scoring", artifact_path=scored_path, payload=scored_payload, status="skipped")
    else:
        _record_stage(manifest, name="scoring", artifact_path=scored_path, payload=None, status="failed", warning="missing_scored_input")
        return manifest

    # Entry stage
    if entry_enabled and "entry" not in overrides:
        entry_payload = run_entry_stage(processed_dir=processed, scored_path=scored_path)
        _record_stage(manifest, name="entry", artifact_path=entry_path, payload=entry_payload, status="ok")
    elif entry_path.exists():
        entry_payload = load_json(entry_path)
        _record_stage(manifest, name="entry", artifact_path=entry_path, payload=entry_payload, status="skipped")
    else:
        _record_stage(manifest, name="entry", artifact_path=entry_path, payload=None, status="failed", warning="missing_entry_input")
        return manifest

    # Build market states
    shortlist_payload = load_json(shortlist_path) if shortlist_path.exists() else {"shortlist": []}
    scored_payload = load_json(scored_path) if scored_path.exists() else {"tokens": []}
    enriched_payload = load_json(enriched_path) if enriched_path.exists() else {"tokens": []}
    x_validated_payload = load_json(x_validated_path) if x_validated_path.exists() else {"tokens": []}

    market_states_payload = _build_market_states_payload(
        entry_payload=entry_payload,
        scored_payload=scored_payload,
        enriched_payload=enriched_payload,
        x_validated_payload=x_validated_payload,
        shortlist_payload=shortlist_payload,
    )
    write_json(market_states_path, market_states_payload)
    _record_stage(manifest, name="market_states", artifact_path=market_states_path, payload=market_states_payload, status="ok")

    return manifest


async def _run_live_trading_loop(
    manifest: dict[str, Any],
    processed: Path,
    helius_streamer: Any,
    live_trader: Any,
    jupiter_client: Any,
    jito_adapter: Any,
) -> None:
    """Run the live trading loop with real-time data streaming."""
    import logging
    logger = logging.getLogger(__name__)

    # Load initial market states and entry signals
    market_states_path = processed / "market_states.json"
    entry_path = processed / "entry_candidates.json"

    if not market_states_path.exists() or not entry_path.exists():
        logger.warning("Market states or entry signals not found, skipping live trading")
        return

    market_states = load_json(market_states_path).get("market_states", [])
    entry_signals = load_json(entry_path).get("signals", [])

    logger.info(f"Starting live trading with {len(entry_signals)} signals and {len(market_states)} market states")

    # Initialize trading state
    state = {
        "portfolio": {
            "total_value_sol": 100.0,  # Starting capital
            "free_capital_sol": 100.0,
            "open_positions": 0,
            "total_positions_ever": 0,
        },
        "positions": [],
        "paths": {
            "base": processed,
            "trades": processed / "trades",
            "logs": processed / "logs",
        },
    }

    # Process initial signals
    if entry_signals:
        logger.info(f"Processing {len(entry_signals)} initial entry signals")
        await live_trader.process_live_entry_signals(
            entry_signals=entry_signals,
            market_states=market_states,
            state=state,
            settings=type('Settings', (), {
                'LIVE_MAX_CONCURRENT_POSITIONS': 5,
                'LIVE_CONTRACT_VERSION': 'live_trading_v1',
            })(),
            token_contexts=[],  # Can be enhanced later
        )

    # Main live trading loop
    logger.info("Entering live trading loop...")
    while True:
        try:
            # Get real-time update from WebSocket streamer
            update = await helius_streamer.get_next_update()
            if update:
                logger.debug(f"Received update: {update.get('pubkey', 'unknown')}")

                # Update market states with real-time data
                # This would need more sophisticated logic to merge WebSocket updates
                # with existing market states

            # Check for exit signals (simplified - in real implementation would have
            # exit logic based on real-time conditions)
            # exit_signals = await check_exit_conditions(state, market_states)
            # if exit_signals:
            #     await live_trader.process_live_exit_signals(exit_signals, market_states, state, settings)

            # Small delay to prevent busy looping
            await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Error in live trading loop: {e}")
            await asyncio.sleep(1.0)  # Longer delay on error


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
