from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from collectors.price_history_client import PriceHistoryClient
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


_START_TS_FIELD_PRIORITY = (
    "price_path_start_ts",
    "price_path_start",
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
)


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


def _iter_timestamp_candidates(value: Any, *, prefix: str = "") -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key in _START_TS_FIELD_PRIORITY:
                ts = _value_to_ts(nested)
                if ts is not None:
                    found.append((path, ts))
            if isinstance(nested, (dict, list, tuple)):
                found.extend(_iter_timestamp_candidates(nested, prefix=path))
    elif isinstance(value, (list, tuple)):
        for idx, nested in enumerate(value[:32]):
            path = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            if isinstance(nested, (dict, list, tuple)):
                found.extend(_iter_timestamp_candidates(nested, prefix=path))
    return found


def _preferred_start_ts(candidates: list[tuple[str, int]]) -> tuple[int | None, str | None]:
    if not candidates:
        return None, None
    for field_name in _START_TS_FIELD_PRIORITY:
        matches = [(path, ts) for path, ts in candidates if path.split(".")[-1] == field_name]
        if matches:
            path, ts = min(matches, key=lambda item: item[1])
            return ts, path
    path, ts = min(candidates, key=lambda item: item[1])
    return ts, path


def _candidate_start_context(cand: dict[str, Any], block_times: dict[int, int], signature_block_times: list[int] | None = None) -> dict[str, Any]:
    candidate_fields = _iter_timestamp_candidates(cand)
    ts, anchor_field = _preferred_start_ts(candidate_fields)
    if ts is not None:
        root_field = str(anchor_field or "").split(".")[-1]
        return {
            "start_ts": ts,
            "price_path_time_source": "candidate_field",
            "price_path_time_derived": root_field not in {"price_path_start_ts", "price_path_start"},
            "price_path_anchor_field": anchor_field,
            "missing_required_fields": [],
        }

    block_time_values = sorted({int(ts) for ts in block_times.values() if int(ts) > 0})
    if block_time_values:
        return {
            "start_ts": block_time_values[0],
            "price_path_time_source": "block_times",
            "price_path_time_derived": True,
            "price_path_anchor_field": "block_times",
            "missing_required_fields": [],
        }

    signature_values = sorted({int(ts) for ts in (signature_block_times or []) if int(ts) > 0})
    if signature_values:
        return {
            "start_ts": signature_values[0],
            "price_path_time_source": "signature_block_time",
            "price_path_time_derived": True,
            "price_path_anchor_field": "signatures[].blockTime",
            "missing_required_fields": list(_START_TS_FIELD_PRIORITY),
        }

    return {
        "start_ts": None,
        "price_path_time_source": None,
        "price_path_time_derived": False,
        "price_path_anchor_field": None,
        "missing_required_fields": list(_START_TS_FIELD_PRIORITY),
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
    price_path_time_source: str | None = None,
    price_path_time_derived: bool = False,
    price_path_anchor_field: str | None = None,
    missing_required_fields: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "token_address": token,
        "pair_address": pair_address,
        "source_provider": "price_history",
        "requested_start_ts": start_ts,
        "requested_end_ts": end_ts,
        "interval_sec": interval_sec,
        "price_path": [],
        "truncated": False,
        "missing": True,
        "price_path_status": "missing",
        "warning": warning,
        "attempt_count": len(attempts or []),
        "attempt_strategy": "exhausted",
        "attempts": attempts or [],
        "resolved_via_fallback": False,
        "fallback_mode": None,
        "price_path_time_source": price_path_time_source,
        "price_path_time_derived": bool(price_path_time_derived),
        "price_path_anchor_field": price_path_anchor_field,
        "missing_required_fields": missing_required_fields or [],
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
) -> list[dict[str, Any]]:
    bcfg = config.get("backfill", {})
    token = str(cand.get("token_address") or "")
    raw_pair_address = cand.get("pair_address")
    pair_address = str(raw_pair_address or "") or None
    if not bcfg.get("collect_price_paths", True):
        return []
    start_context = _candidate_start_context(cand, block_times, signature_block_times)
    start_ts = start_context["start_ts"]
    if not start_ts:
        return [
            _build_missing_price_path(
                token,
                pair_address,
                warning="price_path_start_ts_missing",
                price_path_time_source=start_context.get("price_path_time_source"),
                price_path_time_derived=bool(start_context.get("price_path_time_derived")),
                price_path_anchor_field=start_context.get("price_path_anchor_field"),
                missing_required_fields=list(start_context.get("missing_required_fields") or []),
            )
        ]

    min_points = max(int(bcfg.get("price_path_min_points", 2) or 2), 1)
    retry_attempts = max(int(bcfg.get("price_path_retry_attempts", 3) or 3), 1)
    limit = max(int(bcfg.get("price_path_limit", 256) or 256), 1)
    client = PriceHistoryClient(
        base_url=bcfg.get("price_history_base_url"),
        api_key=config.get("price_history_api_key"),
        provider=str(bcfg.get("price_provider") or "price_history"),
    )

    attempts = _iter_price_path_attempts(token, pair_address, start_ts, config)
    if retry_attempts < len(attempts):
        attempts = attempts[:retry_attempts]

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
                price_path_time_source=start_context.get("price_path_time_source"),
                price_path_time_derived=bool(start_context.get("price_path_time_derived")),
                price_path_anchor_field=start_context.get("price_path_anchor_field"),
            )
        ]

    enriched = dict(best)
    best_summary = next((item for item in attempt_summaries if item["requested_start_ts"] == enriched.get("requested_start_ts") and item["requested_end_ts"] == enriched.get("requested_end_ts") and item["interval_sec"] == enriched.get("interval_sec") and item["pair_address"] == enriched.get("pair_address")), None)
    fallback_mode = None
    if best_summary and best_summary.get("strategy") != "primary":
        fallback_mode = str(best_summary.get("strategy"))
    enriched.update(
        {
            "attempt_count": len(attempt_summaries),
            "attempt_strategy": "staged_fallback",
            "attempts": attempt_summaries,
            "resolved_via_fallback": fallback_mode is not None,
            "fallback_mode": fallback_mode,
            "pair_address": enriched.get("pair_address"),
            "price_path_time_source": start_context.get("price_path_time_source"),
            "price_path_time_derived": bool(start_context.get("price_path_time_derived")),
            "price_path_anchor_field": start_context.get("price_path_anchor_field"),
            "missing_required_fields": list(start_context.get("missing_required_fields") or []),
        }
    )
    if _price_path_points(enriched) == 0:
        enriched["missing"] = True
        enriched["price_path_status"] = "missing"
        enriched["warning"] = enriched.get("warning") or "price_path_attempts_exhausted"
    return [enriched]



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
            },
        )
        cached = _cache_read(cache_dir, key) if bcfg.get("cache_enabled", True) else None
        if cached is not None:
            rows.append(cached)
            continue
        signatures_raw = fetch_signatures_for_address(client, token, limit=int(bcfg.get("max_signatures_per_address", 200)), limiter=limiter)
        signatures = [str(item.get("signature", "")) for item in signatures_raw if isinstance(item, dict) and item.get("signature")]
        signature_block_times = sorted(
            {
                int(item.get("blockTime") or 0)
                for item in signatures_raw
                if isinstance(item, dict) and int(item.get("blockTime") or 0) > 0
            }
        )
        txs = fetch_transactions_for_signatures(client, signatures[:25], limiter=limiter)
        slots = [int(tx.get("slot", 0) or 0) for tx in txs if int(tx.get("slot", 0) or 0) > 0]
        block_times = fetch_block_times(client, slots, limiter=limiter)
        price_paths = _collect_price_paths(cand, block_times, config, signature_block_times=signature_block_times)
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
