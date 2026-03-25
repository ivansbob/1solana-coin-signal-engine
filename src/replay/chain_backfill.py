from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from collectors.price_history_client import PriceHistoryClient, validate_price_history_provider_config
from collectors.solana_rpc_client import SolanaRpcClient


class RateLimiter:
    def __init__(self, max_rps: float) -> None:
        self.interval = 1.0 / max(max_rps, 0.1)
        self.last = 0.0

    def acquire(self) -> None:
        now = time.monotonic()
        wait = self.interval - (now - self.last)
        if wait > 0:
            time.sleep(wait)
        self.last = time.monotonic()



def _cache_key(prefix: str, payload: dict[str, Any]) -> str:
    return prefix + "_" + hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]



def _cache_read(cache_dir: Path, key: str) -> Any:
    path = cache_dir / f"{key}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None



def _cache_write(cache_dir: Path, key: str, value: Any) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{key}.json").write_text(json.dumps(value, sort_keys=True), encoding="utf-8")



def _retry(func: Any, attempts: int = 3, delay: float = 0.2) -> Any:
    err: Exception | None = None
    for i in range(attempts):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            err = exc
            if i == attempts - 1:
                raise
            time.sleep(delay * (2**i))
    if err:
        raise err
    return None



def _iso_to_ts(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


_DEFAULT_TIME_ANCHOR_FIELD_PRIORITY = (
    "price_path_start_ts",
    "price_path_start",
    "replay_entry_time",
    "entry_time",
    "entry_ts",
    "opened_at",
    "signal_timestamp",
    "signal_time",
    "created_at",
    "created_ts",
    "first_seen_at",
    "first_seen_ts",
    "launch_ts",
    "launch_time",
    "discovered_at",
    "discovered_at_utc",
    "discovered_ts",
    "pair_created_at_ts",
    "pair_created_ts",
    "pair_created_at",
    "first_trade_at",
    "event_time",
    "timestamp",
    "block_times",
    "signatures[].blockTime",
)

_TIME_ANCHOR_METADATA_EXCLUDE = {"signatures", "block_times", "transactions", "price_paths", "price_path"}


def _time_anchor_field_priority(config: dict[str, Any]) -> tuple[str, ...]:
    bcfg = config.get("backfill", {})
    configured = bcfg.get("time_anchor_field_priority") or _DEFAULT_TIME_ANCHOR_FIELD_PRIORITY
    fields: list[str] = []
    for value in configured:
        field = str(value or "").strip()
        if field and field not in fields:
            fields.append(field)
    return tuple(fields or _DEFAULT_TIME_ANCHOR_FIELD_PRIORITY)


def _value_to_ts(value: Any) -> int | None:
    if isinstance(value, (int, float)):
        result = int(value)
        return result if result > 0 else None
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        try:
            result = int(text)
        except ValueError:
            return None
        return result if result > 0 else None
    return _iso_to_ts(text)


def _iter_timestamp_candidates(value: Any, *, field_priority: tuple[str, ...], prefix: str = "") -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key in field_priority:
                ts = _value_to_ts(nested)
                if ts is not None:
                    found.append((path, ts))
            if isinstance(nested, (dict, list, tuple)):
                found.extend(_iter_timestamp_candidates(nested, field_priority=field_priority, prefix=path))
    elif isinstance(value, (list, tuple)):
        for idx, nested in enumerate(value[:32]):
            path = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            if isinstance(nested, (dict, list, tuple)):
                found.extend(_iter_timestamp_candidates(nested, field_priority=field_priority, prefix=path))
    return found


def _preferred_start_ts(candidates: list[tuple[str, int]], field_priority: tuple[str, ...]) -> tuple[int | None, str | None]:
    if not candidates:
        return None, None
    for field_name in field_priority:
        matches = [(path, ts) for path, ts in candidates if path.split(".")[-1] == field_name]
        if matches:
            path, ts = min(matches, key=lambda item: item[1])
            return ts, path
    path, ts = min(candidates, key=lambda item: item[1])
    return ts, path


def _anchor_priority(field_name: str, field_priority: tuple[str, ...]) -> int:
    try:
        return field_priority.index(field_name)
    except ValueError:
        return len(field_priority) + 100


def _anchor_field_from_path(path: str) -> str:
    if path == "signatures[].blockTime":
        return path
    if path == "block_times":
        return path
    return path.split(".")[-1]


def _build_time_anchor_candidate(
    *,
    source: str,
    anchor_field: str,
    ts: int,
    derived: bool,
    field_priority: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "source": source,
        "anchor_field": anchor_field,
        "resolved_ts": int(ts),
        "derived": bool(derived),
        "priority_rank": _anchor_priority(anchor_field, field_priority),
    }



def _choose_preferred_time_anchor(
    candidates: list[dict[str, Any]],
    *,
    field_priority: tuple[str, ...],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], bool]:
    if not candidates:
        return None, [], False
    ordered = sorted(
        candidates,
        key=lambda item: (
            int(item.get("priority_rank", len(field_priority) + 100)),
            1 if bool(item.get("derived")) else 0,
            int(item.get("resolved_ts") or 0),
            str(item.get("anchor_field") or ""),
            str(item.get("source") or ""),
        ),
    )
    chosen = ordered[0]
    discarded: list[dict[str, Any]] = []
    for item in ordered[1:]:
        discarded.append(
            {
                "source": item.get("source"),
                "field": item.get("anchor_field"),
                "ts": item.get("resolved_ts"),
                "reason": f"lower_preference_than_{chosen.get('anchor_field')}",
            }
        )
    return chosen, discarded, bool(discarded)


def _top_level_time_candidates(cand: dict[str, Any], field_priority: tuple[str, ...]) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    for field in field_priority:
        if field not in cand:
            continue
        ts = _value_to_ts(cand.get(field))
        if ts is not None:
            found.append((field, ts))
    return found


def _nested_metadata_time_candidates(cand: dict[str, Any], field_priority: tuple[str, ...]) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    for key, value in cand.items():
        if key in _TIME_ANCHOR_METADATA_EXCLUDE or key in field_priority:
            continue
        if isinstance(value, (dict, list, tuple)):
            found.extend(_iter_timestamp_candidates(value, field_priority=field_priority, prefix=str(key)))
    return found


def _extract_block_time_values(value: Any) -> list[int]:
    values: list[int] = []
    if isinstance(value, dict):
        for nested in value.values():
            ts = _value_to_ts(nested)
            if ts is not None:
                values.append(ts)
    elif isinstance(value, list):
        for nested in value:
            ts = _value_to_ts(nested)
            if ts is not None:
                values.append(ts)
    return values


def _normalize_signature_entries(signatures: Any) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    if not isinstance(signatures, list):
        return output
    for item in signatures:
        if isinstance(item, str):
            sig = item.strip()
            if sig:
                output.append({"signature": sig})
            continue
        if not isinstance(item, dict):
            continue
        signature = str(item.get("signature") or item.get("sig") or item.get("id") or "").strip()
        if not signature:
            continue
        normalized = dict(item)
        normalized["signature"] = signature
        output.append(normalized)
    return output


def _signature_time_candidates(signatures: list[dict[str, Any]]) -> list[int]:
    values: list[int] = []
    for item in signatures:
        ts = _value_to_ts(item.get("blockTime") if isinstance(item, dict) else None)
        if ts is not None:
            values.append(ts)
    return values


def _hydrate_signature_block_times(
    client: SolanaRpcClient | None,
    signatures: list[str],
    *,
    limit: int,
    limiter: RateLimiter | None,
) -> list[int]:
    if client is None or limiter is None or not signatures or limit <= 0:
        return []
    txs = fetch_transactions_for_signatures(client, signatures[:limit], limiter=limiter)
    slots = [int(tx.get("slot", 0) or 0) for tx in txs if int(tx.get("slot", 0) or 0) > 0]
    block_times = fetch_block_times(client, slots, limiter=limiter)
    return sorted({int(ts) for ts in block_times.values() if int(ts) > 0})


def _resolve_time_anchor(
    cand: dict[str, Any],
    block_times: dict[int, int],
    config: dict[str, Any],
    *,
    signature_block_times: list[int] | None = None,
    rpc_client: SolanaRpcClient | None = None,
    limiter: RateLimiter | None = None,
) -> dict[str, Any]:
    bcfg = config.get("backfill", {})
    field_priority = _time_anchor_field_priority(config)
    time_anchor_attempts: list[dict[str, Any]] = []
    missing_required_fields = list(field_priority)
    for field in ("block_times", "signatures[].blockTime"):
        if field not in missing_required_fields:
            missing_required_fields.append(field)
    signature_hydration_attempted = False
    signature_hydration_count = 0
    time_anchor_candidates: list[dict[str, Any]] = []

    def _record_missing(source: str, anchor_field: str | None) -> None:
        time_anchor_attempts.append({"source": source, "status": "missing", "anchor_field": anchor_field})

    def _record_candidates(source: str, candidates: list[dict[str, Any]]) -> None:
        if not candidates:
            _record_missing(source, None)
            return
        for item in candidates:
            payload = dict(item)
            payload["status"] = "resolved"
            time_anchor_attempts.append(payload)
            time_anchor_candidates.append(item)

    def _resolved_payload(chosen: dict[str, Any], discarded: list[dict[str, Any]], *, status: str = "resolved") -> dict[str, Any]:
        ts = int(chosen["resolved_ts"])
        return {
            "start_ts": ts,
            "price_path_time_source": chosen.get("source"),
            "price_path_time_derived": bool(chosen.get("derived")),
            "price_path_anchor_field": chosen.get("anchor_field"),
            "time_anchor_resolution_status": status,
            "time_anchor_attempts": time_anchor_attempts,
            "missing_required_fields": [],
            "signature_hydration_attempted": signature_hydration_attempted,
            "signature_hydration_count": signature_hydration_count,
            "resolved_time_anchor_ts": ts,
            "time_anchor_candidates": [dict(item) for item in sorted(time_anchor_candidates, key=lambda item: (int(item.get("priority_rank", len(field_priority) + 100)), int(item.get("resolved_ts") or 0), str(item.get("anchor_field") or "")))],
            "time_anchor_discarded_candidates": discarded,
            "time_anchor_preference_applied": bool(discarded),
        }

    direct_candidates: list[dict[str, Any]] = []
    for anchor_field, ts in _top_level_time_candidates(cand, field_priority):
        direct_candidates.append(
            _build_time_anchor_candidate(
                source="candidate_field",
                anchor_field=anchor_field,
                ts=ts,
                derived=anchor_field not in {"price_path_start_ts", "price_path_start"},
                field_priority=field_priority,
            )
        )
    _record_candidates("candidate_field", direct_candidates)

    metadata_candidates: list[dict[str, Any]] = []
    for path, ts in _nested_metadata_time_candidates(cand, field_priority):
        metadata_candidates.append(
            _build_time_anchor_candidate(
                source="local_metadata",
                anchor_field=_anchor_field_from_path(path),
                ts=ts,
                derived=True,
                field_priority=field_priority,
            )
        )
    _record_candidates("local_metadata", metadata_candidates)

    block_time_values = sorted({
        *[int(ts) for ts in _extract_block_time_values(cand.get("block_times")) if int(ts) > 0],
        *[int(ts) for ts in block_times.values() if int(ts) > 0],
    })
    block_time_candidates: list[dict[str, Any]] = []
    if block_time_values and bool(bcfg.get("time_anchor_use_block_times", True)):
        block_time_candidates.append(
            _build_time_anchor_candidate(
                source="block_times",
                anchor_field="block_times",
                ts=block_time_values[0],
                derived=True,
                field_priority=field_priority,
            )
        )
    _record_candidates("block_times", block_time_candidates)

    normalized_signatures = _normalize_signature_entries(cand.get("signatures"))
    embedded_signature_times = sorted({
        *[int(ts) for ts in _signature_time_candidates(normalized_signatures) if int(ts) > 0],
        *[int(ts) for ts in (signature_block_times or []) if int(ts) > 0],
    })
    embedded_signature_candidates: list[dict[str, Any]] = []
    if embedded_signature_times:
        embedded_signature_candidates.append(
            _build_time_anchor_candidate(
                source="signature_block_time",
                anchor_field="signatures[].blockTime",
                ts=embedded_signature_times[0],
                derived=True,
                field_priority=field_priority,
            )
        )
    if embedded_signature_candidates:
        for item in embedded_signature_candidates:
            payload = dict(item)
            payload["status"] = "resolved"
            time_anchor_attempts.append(payload)
            time_anchor_candidates.append(item)
    else:
        time_anchor_attempts.append({"source": "signatures[].blockTime", "status": "missing", "anchor_field": "signatures[].blockTime"})

    chosen, discarded, preference_applied = _choose_preferred_time_anchor(time_anchor_candidates, field_priority=field_priority)
    if chosen is not None:
        payload = _resolved_payload(chosen, discarded)
        payload["time_anchor_preference_applied"] = preference_applied
        return payload

    if bool(bcfg.get("time_anchor_use_signature_hydration", True)):
        hydration_limit = max(int(bcfg.get("time_anchor_signature_limit", 25) or 25), 1)
        signature_values = [item["signature"] for item in normalized_signatures if str(item.get("signature") or "").strip()][:hydration_limit]
        signature_hydration_attempted = bool(signature_values)
        signature_hydration_count = len(signature_values)
        hydrated_times = _hydrate_signature_block_times(rpc_client, signature_values, limit=hydration_limit, limiter=limiter)
        if hydrated_times:
            hydrated_candidate = _build_time_anchor_candidate(
                source="signature_block_time",
                anchor_field="signatures[].blockTime",
                ts=hydrated_times[0],
                derived=True,
                field_priority=field_priority,
            )
            hydrated_payload = dict(hydrated_candidate)
            hydrated_payload["status"] = "resolved"
            time_anchor_attempts.append(hydrated_payload)
            time_anchor_candidates.append(hydrated_candidate)
            chosen, discarded, preference_applied = _choose_preferred_time_anchor(time_anchor_candidates, field_priority=field_priority)
            if chosen is not None:
                payload = _resolved_payload(chosen, discarded)
                payload["time_anchor_preference_applied"] = preference_applied
                return payload
    time_anchor_attempts.append({"source": "signature_block_time", "status": "missing", "anchor_field": "signatures[].blockTime"})

    token_fallback_candidates: list[dict[str, Any]] = []
    for path, ts in _iter_timestamp_candidates(
        {
            "seed_metadata": cand.get("seed_metadata"),
            "discovery_context": cand.get("discovery_context"),
            "replay_context": cand.get("replay_context"),
        },
        field_priority=field_priority,
    ):
        token_fallback_candidates.append(
            _build_time_anchor_candidate(
                source="token_first_seen",
                anchor_field=_anchor_field_from_path(path),
                ts=ts,
                derived=True,
                field_priority=field_priority,
            )
        )
    _record_candidates("token_first_seen", token_fallback_candidates)

    chosen, discarded, preference_applied = _choose_preferred_time_anchor(time_anchor_candidates, field_priority=field_priority)
    if chosen is not None:
        payload = _resolved_payload(chosen, discarded)
        payload["time_anchor_preference_applied"] = preference_applied
        return payload

    return {
        "start_ts": None,
        "price_path_time_source": None,
        "price_path_time_derived": False,
        "price_path_anchor_field": None,
        "time_anchor_resolution_status": "missing",
        "time_anchor_attempts": time_anchor_attempts,
        "missing_required_fields": missing_required_fields,
        "signature_hydration_attempted": signature_hydration_attempted,
        "signature_hydration_count": signature_hydration_count,
        "resolved_time_anchor_ts": None,
        "time_anchor_candidates": [],
        "time_anchor_discarded_candidates": [],
        "time_anchor_preference_applied": False,
    }


def fetch_signatures_for_address(client: SolanaRpcClient, address: str, *, limit: int, limiter: RateLimiter) -> list[dict[str, Any]]:
    limiter.acquire()
    return _retry(lambda: client.get_signatures_for_address(address, limit=limit)) or []



def fetch_transactions_for_signatures(client: SolanaRpcClient, signatures: list[str], *, limiter: RateLimiter) -> list[dict[str, Any]]:
    txs: list[dict[str, Any]] = []
    for sig in signatures:
        limiter.acquire()
        result = _retry(lambda: client._rpc("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]))
        if isinstance(result, dict):
            txs.append(result)
    return txs



def fetch_block_times(client: SolanaRpcClient, slots: list[int], *, limiter: RateLimiter) -> dict[int, int]:
    out: dict[int, int] = {}
    for slot in slots:
        limiter.acquire()
        value = _retry(lambda: client._rpc("getBlockTime", [slot]))
        if isinstance(value, int):
            out[int(slot)] = value
    return out




def _build_missing_price_path(
    token: str,
    pair_address: str | None,
    *,
    warning: str,
    start_ts: int | None = None,
    end_ts: int | None = None,
    interval_sec: int | None = None,
    attempts: list[dict[str, Any]] | None = None,
    attempt_strategy: str = "exhausted",
    price_history_provider: str | None = None,
    price_history_provider_status: str | None = None,
    provider_bootstrap_ok: bool = False,
    provider_config_source: str | None = None,
    provider_request_summary: dict[str, Any] | None = None,
    price_path_time_source: str | None = None,
    price_path_time_derived: bool = False,
    price_path_anchor_field: str | None = None,
    missing_required_fields: list[str] | None = None,
    time_anchor_resolution_status: str | None = None,
    time_anchor_attempts: list[dict[str, Any]] | None = None,
    signature_hydration_attempted: bool = False,
    signature_hydration_count: int = 0,
    resolved_time_anchor_ts: int | None = None,
    time_anchor_candidates: list[dict[str, Any]] | None = None,
    time_anchor_discarded_candidates: list[dict[str, Any]] | None = None,
    time_anchor_preference_applied: bool = False,
    selected_pool_address: str | None = None,
    pool_resolver_source: str | None = None,
    pool_resolver_confidence: str | None = None,
    pool_candidates_seen: int | None = None,
    pool_resolution_status: str | None = None,
    gap_fill_applied: bool = False,
    gap_fill_count: int = 0,
    observed_row_count: int = 0,
    densified_row_count: int = 0,
) -> dict[str, Any]:
    return {
        "token_address": token,
        "pair_address": pair_address,
        "source_provider": price_history_provider or "price_history",
        "price_history_provider": price_history_provider,
        "price_history_provider_status": price_history_provider_status,
        "provider_bootstrap_ok": bool(provider_bootstrap_ok),
        "provider_config_source": provider_config_source,
        "provider_request_summary": provider_request_summary or {},
        "requested_start_ts": start_ts,
        "requested_end_ts": end_ts,
        "interval_sec": interval_sec,
        "price_path": [],
        "truncated": False,
        "missing": True,
        "price_path_status": "missing",
        "warning": warning,
        "attempt_count": len(attempts or []),
        "attempt_strategy": attempt_strategy,
        "attempts": attempts or [],
        "resolved_via_fallback": False,
        "fallback_mode": None,
        "price_path_time_source": price_path_time_source,
        "price_path_time_derived": bool(price_path_time_derived),
        "price_path_anchor_field": price_path_anchor_field,
        "missing_required_fields": missing_required_fields or [],
        "time_anchor_resolution_status": time_anchor_resolution_status,
        "time_anchor_attempts": time_anchor_attempts or [],
        "signature_hydration_attempted": bool(signature_hydration_attempted),
        "signature_hydration_count": int(signature_hydration_count or 0),
        "resolved_time_anchor_ts": resolved_time_anchor_ts,
        "time_anchor_candidates": time_anchor_candidates or [],
        "time_anchor_discarded_candidates": time_anchor_discarded_candidates or [],
        "time_anchor_preference_applied": bool(time_anchor_preference_applied),
        "selected_pool_address": selected_pool_address,
        "pool_resolver_source": pool_resolver_source,
        "pool_resolver_confidence": pool_resolver_confidence,
        "pool_candidates_seen": pool_candidates_seen,
        "pool_resolution_status": pool_resolution_status,
        "gap_fill_applied": bool(gap_fill_applied),
        "gap_fill_count": int(gap_fill_count or 0),
        "observed_row_count": int(observed_row_count or 0),
        "densified_row_count": int(densified_row_count or 0),
    }


def _price_path_points(row: dict[str, Any]) -> int:
    return len(row.get("price_path") or []) if isinstance(row, dict) else 0



def _price_path_rank(row: dict[str, Any], min_points: int) -> tuple[int, int, int]:
    points = _price_path_points(row)
    status = str(row.get("price_path_status") or "")
    missing = bool(row.get("missing"))
    if not missing and status == "complete" and points >= min_points:
        base = 3
    elif not missing and points > 0:
        base = 2
    else:
        base = 1 if points > 0 else 0
    truncated_bonus = 0 if bool(row.get("truncated")) else 1
    return (base, points, truncated_bonus)



def _choose_best_price_path(paths: list[dict[str, Any]], *, min_points: int) -> dict[str, Any] | None:
    if not paths:
        return None
    return max(paths, key=lambda row: _price_path_rank(row, min_points))



def _attempt_summary(path: dict[str, Any], *, strategy: str, fallback_mode: str | None) -> dict[str, Any]:
    return {
        "strategy": strategy,
        "fallback_mode": fallback_mode,
        "pair_address": path.get("pair_address"),
        "requested_start_ts": path.get("requested_start_ts"),
        "requested_end_ts": path.get("requested_end_ts"),
        "interval_sec": path.get("interval_sec"),
        "price_path_status": path.get("price_path_status"),
        "missing": bool(path.get("missing")),
        "warning": path.get("warning"),
        "point_count": _price_path_points(path),
        "selected_pool_address": path.get("selected_pool_address") or path.get("pool_address"),
        "pool_resolver_source": path.get("pool_resolver_source"),
        "pool_resolver_confidence": path.get("pool_resolver_confidence"),
        "pool_candidates_seen": path.get("pool_candidates_seen"),
        "pool_resolution_status": path.get("pool_resolution_status"),
        "provider_request_summary": path.get("provider_request_summary") or {},
    }



def _iter_price_path_attempts(token: str, pair_address: str | None, start_ts: int, config: dict[str, Any]) -> list[dict[str, Any]]:
    bcfg = config.get("backfill", {})
    base_window = max(int(bcfg.get("price_path_window_sec", 900) or 900), 60)
    max_window = max(int(bcfg.get("price_path_window_max_sec", base_window) or base_window), base_window)
    base_interval = max(int(bcfg.get("price_interval_sec", 60) or 60), 1)
    interval_fallbacks = [max(int(value or 0), 1) for value in (bcfg.get("price_interval_fallbacks") or [])]
    multipliers = [max(int(value or 1), 1) for value in (bcfg.get("price_path_window_fallback_multipliers") or [])]
    prelaunch_buffer_sec = max(int(bcfg.get("price_path_prelaunch_buffer_sec", 0) or 0), 0)
    try_pairless = bool(bcfg.get("price_path_try_pairless", True))

    intervals: list[int] = []
    for value in [base_interval, *interval_fallbacks]:
        if value not in intervals:
            intervals.append(value)

    windows: list[tuple[str, int]] = [("primary", base_window)]
    for multiplier in multipliers:
        window_sec = min(base_window * multiplier, max_window)
        label = f"wider_window_x{multiplier}"
        if (label, window_sec) not in windows:
            windows.append((label, window_sec))

    shifted_start = max(0, start_ts - prelaunch_buffer_sec) if prelaunch_buffer_sec > 0 else start_ts
    attempts: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str | None, int]] = set()

    def append_attempt(strategy: str, window_sec: int, interval_sec: int, attempt_pair: str | None, attempt_start_ts: int) -> None:
        end_ts = attempt_start_ts + window_sec
        key = (attempt_start_ts, end_ts, interval_sec, attempt_pair)
        if key in seen:
            return
        seen.add(key)
        attempts.append(
            {
                "strategy": strategy,
                "start_ts": attempt_start_ts,
                "end_ts": end_ts,
                "window_sec": window_sec,
                "interval_sec": interval_sec,
                "pair_address": attempt_pair,
            }
        )

    primary_pair = pair_address or None
    append_attempt("primary", base_window, base_interval, primary_pair, start_ts)
    for label, window_sec in windows[1:]:
        append_attempt(label, window_sec, base_interval, primary_pair, start_ts)
    for interval_sec in intervals[1:]:
        append_attempt("coarser_interval", base_window, interval_sec, primary_pair, start_ts)
    if try_pairless and primary_pair:
        append_attempt("pairless", base_window, base_interval, None, start_ts)
    if shifted_start != start_ts:
        append_attempt("shifted_start", base_window, base_interval, primary_pair, shifted_start)

    for label, window_sec in windows[1:]:
        for interval_sec in intervals[1:]:
            append_attempt(f"{label}_coarser_interval", window_sec, interval_sec, primary_pair, start_ts)
    if try_pairless and primary_pair:
        for label, window_sec in windows[1:]:
            append_attempt(f"{label}_pairless", window_sec, base_interval, None, start_ts)
        for interval_sec in intervals[1:]:
            append_attempt("coarser_interval_pairless", base_window, interval_sec, None, start_ts)
    if shifted_start != start_ts:
        for label, window_sec in windows[1:]:
            append_attempt(f"shifted_start_{label}", window_sec, base_interval, primary_pair, shifted_start)
        for interval_sec in intervals[1:]:
            append_attempt("shifted_start_coarser_interval", base_window, interval_sec, primary_pair, shifted_start)
        if try_pairless and primary_pair:
            append_attempt("shifted_start_pairless", base_window, base_interval, None, shifted_start)

    return attempts



def _collect_price_paths(
    cand: dict[str, Any],
    block_times: dict[int, int],
    config: dict[str, Any],
    *,
    signature_block_times: list[int] | None = None,
    rpc_client: SolanaRpcClient | None = None,
    limiter: RateLimiter | None = None,
) -> list[dict[str, Any]]:
    bcfg = config.get("backfill", {})
    token = str(cand.get("token_address") or "")
    raw_pair_address = cand.get("pair_address")
    pair_address = str(raw_pair_address or "") or None
    if not bcfg.get("collect_price_paths", True):
        return []

    provider_config = validate_price_history_provider_config(config)
    provider_name = provider_config.get("price_history_provider")
    provider_status = provider_config.get("price_history_provider_status")
    provider_bootstrap_ok = bool(provider_config.get("provider_bootstrap_ok"))
    provider_config_source = provider_config.get("provider_config_source")
    provider_request_summary = dict(provider_config.get("provider_request_summary") or {})

    start_context = _resolve_time_anchor(
        cand,
        block_times,
        config,
        signature_block_times=signature_block_times,
        rpc_client=rpc_client,
        limiter=limiter,
    )
    start_ts = start_context["start_ts"]
    if not start_ts:
        return [
            _build_missing_price_path(
                token,
                pair_address,
                warning="price_path_start_ts_missing",
                price_history_provider=provider_name,
                price_history_provider_status=provider_status,
                provider_bootstrap_ok=provider_bootstrap_ok,
                provider_config_source=provider_config_source,
                provider_request_summary=provider_request_summary,
                price_path_time_source=start_context.get("price_path_time_source"),
                price_path_time_derived=bool(start_context.get("price_path_time_derived")),
                price_path_anchor_field=start_context.get("price_path_anchor_field"),
                missing_required_fields=list(start_context.get("missing_required_fields") or []),
                time_anchor_resolution_status=start_context.get("time_anchor_resolution_status"),
                time_anchor_attempts=list(start_context.get("time_anchor_attempts") or []),
                signature_hydration_attempted=bool(start_context.get("signature_hydration_attempted")),
                signature_hydration_count=int(start_context.get("signature_hydration_count") or 0),
                resolved_time_anchor_ts=start_context.get("resolved_time_anchor_ts"),
            )
        ]

    if not provider_bootstrap_ok:
        bootstrap_warning = str(provider_config.get("warning") or "price_history_provider_unconfigured")
        bootstrap_attempt = {
            "strategy": "provider_bootstrap_failed",
            "fallback_mode": None,
            "pair_address": pair_address,
            "requested_start_ts": start_ts,
            "requested_end_ts": None,
            "interval_sec": int(bcfg.get("price_interval_sec", 60) or 60),
            "price_path_status": "missing",
            "missing": True,
            "warning": bootstrap_warning,
            "point_count": 0,
            "provider_request_summary": provider_request_summary,
        }
        return [
            _build_missing_price_path(
                token,
                pair_address,
                warning=bootstrap_warning,
                start_ts=start_ts,
                interval_sec=int(bcfg.get("price_interval_sec", 60) or 60),
                attempts=[bootstrap_attempt],
                attempt_strategy="provider_bootstrap_failed",
                price_history_provider=provider_name,
                price_history_provider_status=provider_status,
                provider_bootstrap_ok=False,
                provider_config_source=provider_config_source,
                provider_request_summary=provider_request_summary,
                price_path_time_source=start_context.get("price_path_time_source"),
                price_path_time_derived=bool(start_context.get("price_path_time_derived")),
                price_path_anchor_field=start_context.get("price_path_anchor_field"),
                missing_required_fields=list(start_context.get("missing_required_fields") or []),
                time_anchor_resolution_status=start_context.get("time_anchor_resolution_status"),
                time_anchor_attempts=list(start_context.get("time_anchor_attempts") or []),
                signature_hydration_attempted=bool(start_context.get("signature_hydration_attempted")),
                signature_hydration_count=int(start_context.get("signature_hydration_count") or 0),
                resolved_time_anchor_ts=start_context.get("resolved_time_anchor_ts"),
                time_anchor_candidates=list(start_context.get("time_anchor_candidates") or []),
                time_anchor_discarded_candidates=list(start_context.get("time_anchor_discarded_candidates") or []),
                time_anchor_preference_applied=bool(start_context.get("time_anchor_preference_applied")),
            )
        ]

    min_points = max(int(bcfg.get("price_path_min_points", 2) or 2), 1)
    retry_attempts = max(int(bcfg.get("price_path_retry_attempts", 3) or 3), 1)
    limit = max(int(bcfg.get("price_path_limit", 256) or 256), 1)
    client = PriceHistoryClient(
        base_url=provider_config.get("base_url"),
        api_key=provider_config.get("api_key"),
        provider=str(provider_name or "price_history"),
        token_endpoint=provider_config.get("token_endpoint"),
        pair_endpoint=provider_config.get("pair_endpoint"),
        chain=provider_config.get("chain"),
        require_pair_address=bool(provider_config.get("require_pair_address")),
        allow_pairless_token_lookup=bool(provider_config.get("allow_pairless_token_lookup", True)),
        provider_status=str(provider_status or "configured"),
        provider_config_source=provider_config_source,
        auth_header=str(provider_config.get("auth_header") or "X-API-KEY"),
        request_kind=str(provider_config.get("request_kind") or "ohlcv"),
        request_version=provider_config.get("request_version"),
        currency=str(provider_config.get("currency") or "usd"),
        token_side=str(provider_config.get("token_side") or "token"),
        include_empty_intervals=bool(provider_config.get("include_empty_intervals", True)),
        pool_resolver=provider_config.get("pool_resolver"),
        resolver_cache_ttl_sec=int(provider_config.get("resolver_cache_ttl_sec") or 0),
        max_ohlcv_limit=int(provider_config.get("max_ohlcv_limit") or 1000),
    )

    attempts = _iter_price_path_attempts(token, pair_address, start_ts, config)
    if not provider_config.get("allow_pairless_token_lookup", True):
        attempts = [attempt for attempt in attempts if attempt.get("pair_address") is not None]
    if retry_attempts < len(attempts):
        attempts = attempts[:retry_attempts]

    if not attempts:
        return [
            _build_missing_price_path(
                token,
                pair_address,
                warning="provider_pair_address_required",
                start_ts=start_ts,
                interval_sec=int(bcfg.get("price_interval_sec", 60) or 60),
                attempt_strategy="provider_bootstrap_failed",
                price_history_provider=provider_name,
                price_history_provider_status=provider_status,
                provider_bootstrap_ok=True,
                provider_config_source=provider_config_source,
                provider_request_summary=provider_request_summary,
                price_path_time_source=start_context.get("price_path_time_source"),
                price_path_time_derived=bool(start_context.get("price_path_time_derived")),
                price_path_anchor_field=start_context.get("price_path_anchor_field"),
                missing_required_fields=list(start_context.get("missing_required_fields") or []),
                time_anchor_resolution_status=start_context.get("time_anchor_resolution_status"),
                time_anchor_attempts=list(start_context.get("time_anchor_attempts") or []),
                signature_hydration_attempted=bool(start_context.get("signature_hydration_attempted")),
                signature_hydration_count=int(start_context.get("signature_hydration_count") or 0),
                resolved_time_anchor_ts=start_context.get("resolved_time_anchor_ts"),
            )
        ]

    attempt_summaries: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for attempt in attempts:
        path = client.fetch_price_path(
            token_address=token,
            pair_address=attempt["pair_address"],
            start_ts=attempt["start_ts"],
            end_ts=attempt["end_ts"],
            interval_sec=attempt["interval_sec"],
            limit=limit,
        )
        if attempt.get("pair_address") and not path.get("pool_resolution_status"):
            path["pool_resolution_status"] = "seed_pair_address"
        if attempt.get("pair_address") and not path.get("selected_pool_address"):
            path["selected_pool_address"] = path.get("pool_address")
        if attempt.get("pair_address") and not path.get("pool_resolver_source"):
            path["pool_resolver_source"] = "seed_pair_address"
        summary = _attempt_summary(path, strategy=attempt["strategy"], fallback_mode=attempt["strategy"] if attempt["strategy"] != "primary" else None)
        attempt_summaries.append(summary)
        results.append(path)

    best = _choose_best_price_path(results, min_points=min_points)
    if best is None:
        return [
            _build_missing_price_path(
                token,
                pair_address,
                warning="price_path_attempts_exhausted",
                start_ts=start_ts,
                attempts=attempt_summaries,
                price_history_provider=provider_name,
                price_history_provider_status=provider_status,
                provider_bootstrap_ok=True,
                provider_config_source=provider_config_source,
                provider_request_summary=provider_request_summary,
                price_path_time_source=start_context.get("price_path_time_source"),
                price_path_time_derived=bool(start_context.get("price_path_time_derived")),
                price_path_anchor_field=start_context.get("price_path_anchor_field"),
                missing_required_fields=list(start_context.get("missing_required_fields") or []),
                time_anchor_resolution_status=start_context.get("time_anchor_resolution_status"),
                time_anchor_attempts=list(start_context.get("time_anchor_attempts") or []),
                signature_hydration_attempted=bool(start_context.get("signature_hydration_attempted")),
                signature_hydration_count=int(start_context.get("signature_hydration_count") or 0),
                resolved_time_anchor_ts=start_context.get("resolved_time_anchor_ts"),
                time_anchor_candidates=list(start_context.get("time_anchor_candidates") or []),
                time_anchor_discarded_candidates=list(start_context.get("time_anchor_discarded_candidates") or []),
                time_anchor_preference_applied=bool(start_context.get("time_anchor_preference_applied")),
            )
        ]

    enriched = dict(best)
    best_summary = next((item for item in attempt_summaries if item["requested_start_ts"] == enriched.get("requested_start_ts") and item["requested_end_ts"] == enriched.get("requested_end_ts") and item["interval_sec"] == enriched.get("interval_sec") and item["pair_address"] == enriched.get("pair_address")), None)
    fallback_mode = None
    if best_summary and best_summary.get("strategy") != "primary":
        fallback_mode = str(best_summary.get("strategy"))
    enriched.update(
        {
            "price_history_provider": enriched.get("price_history_provider") or provider_name,
            "price_history_provider_status": enriched.get("price_history_provider_status") or provider_status,
            "provider_bootstrap_ok": bool(enriched.get("provider_bootstrap_ok", True)),
            "provider_config_source": enriched.get("provider_config_source") or provider_config_source,
            "provider_request_summary": enriched.get("provider_request_summary") or provider_request_summary,
            "attempt_count": len(attempt_summaries),
            "attempt_strategy": "staged_fallback",
            "attempts": attempt_summaries,
            "resolved_via_fallback": fallback_mode is not None,
            "fallback_mode": fallback_mode,
            "pair_address": enriched.get("pair_address"),
            "selected_pool_address": enriched.get("selected_pool_address") or enriched.get("pool_address"),
            "pool_resolver_source": enriched.get("pool_resolver_source"),
            "pool_resolver_confidence": enriched.get("pool_resolver_confidence"),
            "pool_candidates_seen": enriched.get("pool_candidates_seen"),
            "pool_resolution_status": enriched.get("pool_resolution_status") or ("seed_pair_address" if enriched.get("pair_address") else None),
            "price_path_time_source": start_context.get("price_path_time_source"),
            "price_path_time_derived": bool(start_context.get("price_path_time_derived")),
            "price_path_anchor_field": start_context.get("price_path_anchor_field"),
            "missing_required_fields": list(start_context.get("missing_required_fields") or []),
            "time_anchor_resolution_status": start_context.get("time_anchor_resolution_status"),
            "time_anchor_attempts": list(start_context.get("time_anchor_attempts") or []),
            "signature_hydration_attempted": bool(start_context.get("signature_hydration_attempted")),
            "signature_hydration_count": int(start_context.get("signature_hydration_count") or 0),
            "resolved_time_anchor_ts": start_context.get("resolved_time_anchor_ts"),
            "time_anchor_candidates": list(start_context.get("time_anchor_candidates") or []),
            "time_anchor_discarded_candidates": list(start_context.get("time_anchor_discarded_candidates") or []),
            "time_anchor_preference_applied": bool(start_context.get("time_anchor_preference_applied")),
            "gap_fill_applied": bool(enriched.get("gap_fill_applied")),
            "gap_fill_count": int(enriched.get("gap_fill_count") or 0),
            "observed_row_count": int(enriched.get("observed_row_count") or 0),
            "densified_row_count": int(enriched.get("densified_row_count") or 0),
        }
    )
    if _price_path_points(enriched) == 0 and int(enriched.get("obs_len") or 0) <= 0:
        enriched["missing"] = True
        enriched["price_path_status"] = "missing"
        enriched["warning"] = enriched.get("warning") or "price_path_attempts_exhausted"
    return [enriched]


def _time_anchor_cache_fingerprint(cand: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    fields = _time_anchor_field_priority(config)
    payload: dict[str, Any] = {field: cand.get(field) for field in fields if cand.get(field) not in (None, "", [], {})}
    for key in ("signatures", "block_times", "seed_metadata", "replay_context", "discovery_context"):
        value = cand.get(key)
        if value not in (None, "", [], {}):
            payload[key] = value
    return payload



def build_chain_context(candidates: list[dict[str, Any]], config: dict[str, Any], *, dry_run: bool) -> list[dict[str, Any]]:
    bcfg = config.get("backfill", {})
    if dry_run:
        rows = []
        for row in candidates:
            i = int(str(row.get("token_address", "0")).split("_")[-1] or 0)
            start_ts = 1_700_000_000 + i * 60
            rows.append(
                {
                    "token_address": row["token_address"],
                    "pair_address": row.get("pair_address", ""),
                    "signatures": [f"sig_{i}_{j}" for j in range(3)],
                    "transactions": [{"slot": 1_000_000 + i * 10 + j, "meta": {"fee": 5_000 + j}} for j in range(3)],
                    "block_times": {str(1_000_000 + i * 10 + j): start_ts + j for j in range(3)},
                    "buyer_snapshot": {"buyers_5m": 10 + i, "holders": 100 + i * 2},
                    "price_paths": [{
                        "token_address": row["token_address"],
                        "pair_address": row.get("pair_address", ""),
                        "source_provider": "dry_run",
                        "price_path": [
                            {"timestamp": start_ts + j * 60, "offset_sec": j * 60, "price": round(1.0 + j * 0.02, 4)}
                            for j in range(6)
                        ],
                        "truncated": False,
                        "missing": False,
                        "price_path_status": "complete",
                        "warning": None,
                        "price_path_time_source": "candidate_field",
                        "price_path_anchor_field": "price_path_start_ts",
                        "price_path_time_derived": False,
                        "time_anchor_resolution_status": "resolved",
                        "time_anchor_attempts": [{"source": "candidate_field", "status": "resolved", "anchor_field": "price_path_start_ts", "resolved_ts": start_ts}],
                        "resolved_time_anchor_ts": start_ts,
                        "time_anchor_candidates": [{"source": "candidate_field", "anchor_field": "price_path_start_ts", "resolved_ts": start_ts, "derived": False, "priority_rank": 0}],
                        "time_anchor_discarded_candidates": [],
                        "time_anchor_preference_applied": False,
                    }],
                }
            )
        return rows

    rpc_url = "https://api.mainnet-beta.solana.com"
    if str(bcfg.get("provider", "")).startswith("helius"):
        key = str(config.get("helius_api_key", "")).strip()
        if key:
            rpc_url = f"https://mainnet.helius-rpc.com/?api-key={key}"
    client = SolanaRpcClient(rpc_url=rpc_url)
    limiter = RateLimiter(float(bcfg.get("max_rps", 5)))
    cache_dir = Path(".cache/replay")

    rows: list[dict[str, Any]] = []
    for cand in candidates:
        token = str(cand.get("token_address", ""))
        key = _cache_key(
            "backfill",
            {
                "token": token,
                "limit": int(bcfg.get("max_signatures_per_address", 200)),
                "price_window_sec": int(bcfg.get("price_path_window_sec", 900) or 900),
                "price_window_max_sec": int(bcfg.get("price_path_window_max_sec", bcfg.get("price_path_window_sec", 900)) or bcfg.get("price_path_window_sec", 900)),
                "price_interval_sec": int(bcfg.get("price_interval_sec", 60) or 60),
                "price_interval_fallbacks": bcfg.get("price_interval_fallbacks") or [],
                "price_path_window_fallback_multipliers": bcfg.get("price_path_window_fallback_multipliers") or [],
                "price_path_prelaunch_buffer_sec": int(bcfg.get("price_path_prelaunch_buffer_sec", 0) or 0),
                "price_path_try_pairless": bool(bcfg.get("price_path_try_pairless", True)),
                "price_path_min_points": int(bcfg.get("price_path_min_points", 2) or 2),
                "price_path_require_nonempty": bool(bcfg.get("price_path_require_nonempty", True)),
                "price_path_retry_attempts": int(bcfg.get("price_path_retry_attempts", 3) or 3),
                "enrich_time_anchor": bool(bcfg.get("enrich_time_anchor", True)),
                "time_anchor_field_priority": list(_time_anchor_field_priority(config)),
                "time_anchor_use_block_times": bool(bcfg.get("time_anchor_use_block_times", True)),
                "time_anchor_use_signature_hydration": bool(bcfg.get("time_anchor_use_signature_hydration", True)),
                "time_anchor_signature_limit": int(bcfg.get("time_anchor_signature_limit", 25) or 25),
                "time_anchor_allow_replay_entry_time": bool(bcfg.get("time_anchor_allow_replay_entry_time", True)),
                "time_anchor_fingerprint": _time_anchor_cache_fingerprint(cand, config),
            },
        )
        cached = _cache_read(cache_dir, key) if bcfg.get("cache_enabled", True) else None
        if cached is not None:
            rows.append(cached)
            continue
        candidate_signatures = _normalize_signature_entries(cand.get("signatures"))
        signatures_raw = fetch_signatures_for_address(client, token, limit=int(bcfg.get("max_signatures_per_address", 200)), limiter=limiter)
        merged_signature_entries = candidate_signatures + [item for item in signatures_raw if isinstance(item, dict) and str(item.get("signature") or "").strip()]
        seen_signatures: set[str] = set()
        normalized_signatures: list[dict[str, Any]] = []
        for item in merged_signature_entries:
            normalized = _normalize_signature_entries([item])
            if not normalized:
                continue
            entry = normalized[0]
            signature = entry["signature"]
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            normalized_signatures.append(entry)
        signatures = [item["signature"] for item in normalized_signatures]
        signature_block_times = sorted({int(ts) for ts in _signature_time_candidates(normalized_signatures) if int(ts) > 0})
        txs = fetch_transactions_for_signatures(client, signatures[:25], limiter=limiter)
        slots = [int(tx.get("slot", 0) or 0) for tx in txs if int(tx.get("slot", 0) or 0) > 0]
        block_times = fetch_block_times(client, slots, limiter=limiter)
        candidate_context = dict(cand)
        if normalized_signatures:
            candidate_context["signatures"] = normalized_signatures
        if block_times and not candidate_context.get("block_times"):
            candidate_context["block_times"] = block_times
        price_paths = _collect_price_paths(
            candidate_context,
            block_times,
            config,
            signature_block_times=signature_block_times,
            rpc_client=client,
            limiter=limiter,
        )
        row = {
            "token_address": token,
            "pair_address": cand.get("pair_address", ""),
            "signatures": signatures,
            "transactions": txs,
            "block_times": block_times,
            "buyer_snapshot": {"buyers_5m": len(signatures[:20]), "holders": len(signatures[:100])},
            "price_paths": price_paths,
        }
        if bcfg.get("cache_enabled", True):
            _cache_write(cache_dir, key, row)
        rows.append(row)
    return rows
