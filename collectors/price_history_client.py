"""Canonical historical price-path collection helpers for replay backfill."""

from __future__ import annotations

import json
import math
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_PROVIDER_ALIASES = {
    "birdeye": "birdeye_ohlcv_v3",
    "birdeye_v3": "birdeye_ohlcv_v3",
    "birdeye_ohlcv": "birdeye_ohlcv_v3",
    "birdeye_ohlcv_v3": "birdeye_ohlcv_v3",
    "birdeye_history": "birdeye_price_history",
    "birdeye_price_history": "birdeye_price_history",
}

_PROVIDER_DEFAULTS: dict[str, dict[str, Any]] = {
    "birdeye_ohlcv_v3": {
        "base_url": "https://public-api.birdeye.so",
        "token_endpoint": "defi/v3/ohlcv",
        "pair_endpoint": "defi/v3/ohlcv/pair",
        "chain": "solana",
        "auth_header": "X-API-KEY",
        "require_pair_address": False,
        "allow_pairless_token_lookup": True,
        "request_kind": "ohlcv",
    },
    "birdeye_price_history": {
        "base_url": "https://public-api.birdeye.so",
        "token_endpoint": "defi/history_price",
        "pair_endpoint": "defi/history_price",
        "chain": "solana",
        "auth_header": "X-API-KEY",
        "require_pair_address": False,
        "allow_pairless_token_lookup": True,
        "request_kind": "history_price",
    },
}

_PROVIDER_KEY_PATHS = (
    ("backfill", "price_history_provider"),
    ("providers", "price_history", "provider"),
    ("price_history", "provider"),
    ("backfill", "price_provider"),
)

_BASE_URL_KEY_PATHS = (
    ("providers", "price_history", "base_url"),
    ("price_history", "base_url"),
    ("backfill", "price_history_base_url"),
)

_TOKEN_ENDPOINT_KEY_PATHS = (
    ("providers", "price_history", "token_endpoint"),
    ("price_history", "token_endpoint"),
)

_PAIR_ENDPOINT_KEY_PATHS = (
    ("providers", "price_history", "pair_endpoint"),
    ("price_history", "pair_endpoint"),
)

_API_KEY_KEY_PATHS = (
    ("providers", "price_history", "api_key"),
    ("price_history", "api_key"),
    ("price_history_api_key",),
)

_CHAIN_KEY_PATHS = (
    ("providers", "price_history", "chain"),
    ("price_history", "chain"),
)

_REQUIRE_PAIR_KEY_PATHS = (
    ("providers", "price_history", "require_pair_address"),
    ("price_history", "require_pair_address"),
)

_PAIRLESS_KEY_PATHS = (
    ("providers", "price_history", "allow_pairless_token_lookup"),
    ("price_history", "allow_pairless_token_lookup"),
)

_DISABLED_PROVIDER_NAMES = {"disabled", "off", "none", "false", "0"}

_INTERVAL_LABELS = {
    1: "1s",
    15: "15s",
    30: "30s",
    60: "1m",
    180: "3m",
    300: "5m",
    900: "15m",
    1800: "30m",
    3600: "1H",
    7200: "2H",
    14400: "4H",
    21600: "6H",
    28800: "8H",
    43200: "12H",
    86400: "1D",
    259200: "3D",
    604800: "1W",
}


def _coerce_int(value: Any) -> int | None:
    try:
        result = int(float(value))
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None



def _coerce_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result



def _get_nested(config: dict[str, Any], path: tuple[str, ...]) -> Any:
    node: Any = config
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node.get(key)
    return node



def _first_config_value(config: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> tuple[Any, str | None]:
    for path in paths:
        value = _get_nested(config, path)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value, ".".join(path)
    return None, None



def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default



def _normalize_provider_name(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if text in _DISABLED_PROVIDER_NAMES:
        return "disabled"
    return _PROVIDER_ALIASES.get(text, text)



def resolve_price_history_provider(config: dict[str, Any]) -> dict[str, Any]:
    provider_value, provider_source = _first_config_value(config, _PROVIDER_KEY_PATHS)
    if provider_value is None:
        return {
            "price_history_provider": None,
            "price_history_provider_status": "unconfigured",
            "provider_bootstrap_ok": False,
            "provider_config_source": None,
            "warning": "price_history_provider_unconfigured",
        }

    canonical_provider = _normalize_provider_name(provider_value)
    if canonical_provider == "disabled":
        return {
            "price_history_provider": None,
            "price_history_provider_status": "disabled",
            "provider_bootstrap_ok": False,
            "provider_config_source": provider_source,
            "warning": "price_history_provider_disabled",
        }
    if canonical_provider not in _PROVIDER_DEFAULTS:
        return {
            "price_history_provider": canonical_provider,
            "price_history_provider_status": "invalid",
            "provider_bootstrap_ok": False,
            "provider_config_source": provider_source,
            "warning": "price_history_provider_invalid",
        }
    return {
        "price_history_provider": canonical_provider,
        "price_history_provider_status": "configured",
        "provider_bootstrap_ok": True,
        "provider_config_source": provider_source,
        "warning": None,
    }



def validate_price_history_provider_config(config: dict[str, Any]) -> dict[str, Any]:
    resolved = resolve_price_history_provider(config)
    provider_name = resolved.get("price_history_provider")
    defaults = dict(_PROVIDER_DEFAULTS.get(str(provider_name), {})) if provider_name else {}

    base_url, _ = _first_config_value(config, _BASE_URL_KEY_PATHS)
    token_endpoint, _ = _first_config_value(config, _TOKEN_ENDPOINT_KEY_PATHS)
    pair_endpoint, _ = _first_config_value(config, _PAIR_ENDPOINT_KEY_PATHS)
    api_key, _ = _first_config_value(config, _API_KEY_KEY_PATHS)
    chain, _ = _first_config_value(config, _CHAIN_KEY_PATHS)
    require_pair_address, _ = _first_config_value(config, _REQUIRE_PAIR_KEY_PATHS)
    allow_pairless_token_lookup, _ = _first_config_value(config, _PAIRLESS_KEY_PATHS)

    merged = {
        "price_history_provider": provider_name,
        "price_history_provider_status": resolved.get("price_history_provider_status"),
        "provider_bootstrap_ok": bool(resolved.get("provider_bootstrap_ok")),
        "provider_config_source": resolved.get("provider_config_source"),
        "warning": resolved.get("warning"),
        "base_url": str(base_url or defaults.get("base_url") or "").strip().rstrip("/"),
        "token_endpoint": str(token_endpoint or defaults.get("token_endpoint") or "").strip().lstrip("/"),
        "pair_endpoint": str(pair_endpoint or defaults.get("pair_endpoint") or token_endpoint or defaults.get("token_endpoint") or "").strip().lstrip("/"),
        "request_kind": str(defaults.get("request_kind") or "ohlcv"),
        "chain": str(chain or defaults.get("chain") or "solana").strip() or "solana",
        "api_key": str(api_key or "").strip(),
        "auth_header": str(defaults.get("auth_header") or "X-API-KEY"),
        "require_pair_address": _coerce_bool(require_pair_address, bool(defaults.get("require_pair_address", False))),
        "allow_pairless_token_lookup": _coerce_bool(allow_pairless_token_lookup, bool(defaults.get("allow_pairless_token_lookup", True))),
    }
    merged["provider_request_summary"] = {
        "provider": merged.get("price_history_provider"),
        "provider_status": merged.get("price_history_provider_status"),
        "provider_config_source": merged.get("provider_config_source"),
        "base_url": merged.get("base_url") or None,
        "token_endpoint": merged.get("token_endpoint") or None,
        "pair_endpoint": merged.get("pair_endpoint") or None,
        "chain": merged.get("chain"),
        "api_key_present": bool(merged.get("api_key")),
        "require_pair_address": bool(merged.get("require_pair_address")),
        "allow_pairless_token_lookup": bool(merged.get("allow_pairless_token_lookup")),
    }
    return merged



def _interval_label(interval_sec: int) -> str:
    return _INTERVAL_LABELS.get(int(interval_sec or 60), "1m")



def build_price_history_request(
    provider_config: dict[str, Any],
    *,
    token_address: str,
    pair_address: str | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
    interval_sec: int = 60,
    limit: int = 256,
) -> dict[str, Any]:
    provider_name = str(provider_config.get("price_history_provider") or "")
    provider_status = str(provider_config.get("price_history_provider_status") or "unconfigured")
    if not provider_config.get("provider_bootstrap_ok"):
        warning = provider_config.get("warning") or "price_history_provider_unconfigured"
        return {
            "ok": False,
            "warning": warning,
            "provider_request_summary": {
                **dict(provider_config.get("provider_request_summary") or {}),
                "address_kind": None,
                "requested_pair_address": pair_address,
                "requested_token_address": token_address,
                "interval_sec": interval_sec,
                "interval_label": _interval_label(interval_sec),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "limit": limit,
            },
        }

    if provider_config.get("require_pair_address") and not pair_address:
        return {
            "ok": False,
            "warning": "provider_pair_address_required",
            "provider_request_summary": {
                **dict(provider_config.get("provider_request_summary") or {}),
                "address_kind": "pair",
                "requested_pair_address": pair_address,
                "requested_token_address": token_address,
                "interval_sec": interval_sec,
                "interval_label": _interval_label(interval_sec),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "limit": limit,
            },
        }

    if not pair_address and not provider_config.get("allow_pairless_token_lookup", True):
        return {
            "ok": False,
            "warning": "provider_pair_address_required",
            "provider_request_summary": {
                **dict(provider_config.get("provider_request_summary") or {}),
                "address_kind": "pair",
                "requested_pair_address": pair_address,
                "requested_token_address": token_address,
                "interval_sec": interval_sec,
                "interval_label": _interval_label(interval_sec),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "limit": limit,
            },
        }

    address_kind = "pair" if pair_address else "token"
    endpoint = provider_config.get("pair_endpoint") if pair_address else provider_config.get("token_endpoint")
    address = pair_address or token_address
    interval_label = _interval_label(interval_sec)

    if provider_name == "birdeye_ohlcv_v3":
        params = {
            "address": address,
            "time_from": start_ts or 0,
            "time_to": end_ts or 0,
            "type": interval_label,
        }
    elif provider_name == "birdeye_price_history":
        params = {
            "address": address,
            "address_type": address_kind,
            "time_from": start_ts or 0,
            "time_to": end_ts or 0,
            "type": interval_label,
        }
    else:
        params = {
            "token_address": token_address,
            "pair_address": pair_address or "",
            "start_ts": start_ts or "",
            "end_ts": end_ts or "",
            "interval_sec": interval_sec,
            "limit": limit,
        }

    headers = {
        "Accept": "application/json",
        "User-Agent": "scse/0.1",
    }
    auth_header = str(provider_config.get("auth_header") or "").strip()
    api_key = str(provider_config.get("api_key") or "").strip()
    if provider_name.startswith("birdeye"):
        headers["x-chain"] = str(provider_config.get("chain") or "solana")
    if auth_header and api_key:
        headers[auth_header] = api_key

    request_summary = {
        **dict(provider_config.get("provider_request_summary") or {}),
        "endpoint": endpoint,
        "address": address,
        "address_kind": address_kind,
        "requested_pair_address": pair_address,
        "requested_token_address": token_address,
        "interval_sec": interval_sec,
        "interval_label": interval_label,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    }
    return {
        "ok": provider_status == "configured",
        "endpoint": endpoint,
        "params": params,
        "headers": headers,
        "provider_request_summary": request_summary,
    }



def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    candidates = [payload.get("rows"), payload.get("items"), payload.get("observations")]
    data = payload.get("data")
    if isinstance(data, list):
        candidates.append(data)
    elif isinstance(data, dict):
        candidates.extend([
            data.get("rows"),
            data.get("items"),
            data.get("observations"),
            data.get("list"),
            data.get("candles"),
            data.get("history"),
        ])
    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


class PriceHistoryClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        provider: str = "price_history",
        token_endpoint: str | None = None,
        pair_endpoint: str | None = None,
        chain: str | None = None,
        require_pair_address: bool = False,
        allow_pairless_token_lookup: bool = True,
        provider_status: str = "configured",
        provider_config_source: str | None = None,
        auth_header: str = "X-API-KEY",
        request_kind: str = "ohlcv",
    ) -> None:
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.provider = provider
        self.token_endpoint = str(token_endpoint or "price-history").strip().lstrip("/")
        self.pair_endpoint = str(pair_endpoint or self.token_endpoint).strip().lstrip("/")
        self.chain = str(chain or "solana").strip() or "solana"
        self.require_pair_address = bool(require_pair_address)
        self.allow_pairless_token_lookup = bool(allow_pairless_token_lookup)
        self.provider_status = str(provider_status or "configured")
        self.provider_config_source = provider_config_source
        self.auth_header = str(auth_header or "X-API-KEY")
        self.request_kind = str(request_kind or "ohlcv")

    def provider_bootstrap(self) -> dict[str, Any]:
        return {
            "price_history_provider": self.provider,
            "price_history_provider_status": self.provider_status,
            "provider_bootstrap_ok": self.provider_status == "configured",
            "provider_config_source": self.provider_config_source,
            "base_url": self.base_url,
            "token_endpoint": self.token_endpoint,
            "pair_endpoint": self.pair_endpoint,
            "chain": self.chain,
            "api_key": self.api_key,
            "auth_header": self.auth_header,
            "require_pair_address": self.require_pair_address,
            "allow_pairless_token_lookup": self.allow_pairless_token_lookup,
            "request_kind": self.request_kind,
            "provider_request_summary": {
                "provider": self.provider,
                "provider_status": self.provider_status,
                "provider_config_source": self.provider_config_source,
                "base_url": self.base_url or None,
                "token_endpoint": self.token_endpoint or None,
                "pair_endpoint": self.pair_endpoint or None,
                "chain": self.chain,
                "api_key_present": bool(self.api_key),
                "require_pair_address": self.require_pair_address,
                "allow_pairless_token_lookup": self.allow_pairless_token_lookup,
            },
        }

    def _get(self, endpoint: str, params: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
        if not self.base_url:
            return {"rows": [], "missing": True, "warning": "price_history_provider_unconfigured"}
        query = {key: value for key, value in dict(params).items() if value not in (None, "")}
        req_headers = {"Accept": "application/json", "User-Agent": "scse/0.1"}
        if self.provider.startswith("birdeye"):
            req_headers["x-chain"] = self.chain
        if self.auth_header and self.api_key:
            req_headers[self.auth_header] = self.api_key
        if headers:
            req_headers.update(headers)
        req = Request(
            f"{self.base_url}/{endpoint}?{urlencode(query)}",
            headers=req_headers,
        )
        try:
            with urlopen(req, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return {"rows": [], "missing": True, "warning": "provider_http_error", "http_status": int(exc.code)}
        except (URLError, TimeoutError):
            return {"rows": [], "missing": True, "warning": "provider_http_error"}
        except json.JSONDecodeError:
            return {"rows": [], "missing": True, "warning": "provider_empty_payload"}

    def _normalize_observations(self, rows: list[dict[str, Any]], *, start_ts: int | None) -> list[dict[str, Any]]:
        observations: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _coerce_int(
                row.get("timestamp")
                or row.get("unixTime")
                or row.get("ts")
                or row.get("time")
                or row.get("t")
            )
            price = _coerce_float(
                row.get("price")
                or row.get("close")
                or row.get("close_price")
                or row.get("value")
                or row.get("c")
            )
            if ts is None or price is None:
                continue
            offset = _coerce_int(row.get("offset_sec") or row.get("elapsed_sec"))
            if offset is None and start_ts is not None:
                offset = max(0, ts - start_ts)
            observations.append({
                "timestamp": ts,
                "offset_sec": int(offset or 0),
                "price": price,
            })
        observations.sort(key=lambda item: (item.get("offset_sec", 0), item.get("timestamp", 0)))
        return observations

    def fetch_price_path(
        self,
        *,
        token_address: str,
        pair_address: str | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval_sec: int = 60,
        limit: int = 256,
    ) -> dict[str, Any]:
        provider_config = self.provider_bootstrap()
        request = build_price_history_request(
            provider_config,
            token_address=token_address,
            pair_address=pair_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_sec=interval_sec,
            limit=limit,
        )
        request_summary = dict(request.get("provider_request_summary") or {})
        request_params = {
            "token_address": token_address,
            "pair_address": pair_address or None,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "interval_sec": interval_sec,
            "limit": limit,
        }

        if not request.get("ok"):
            warning = request.get("warning") or provider_config.get("warning") or "price_history_provider_unconfigured"
            return {
                "token_address": token_address,
                "pair_address": pair_address,
                "source_provider": self.provider,
                "price_history_provider": self.provider,
                "price_history_provider_status": self.provider_status,
                "provider_bootstrap_ok": self.provider_status == "configured",
                "provider_config_source": self.provider_config_source,
                "provider_request_summary": request_summary,
                "requested_start_ts": start_ts,
                "requested_end_ts": end_ts,
                "interval_sec": interval_sec,
                "request_params": request_params,
                "provider_row_count": 0,
                "price_path": [],
                "truncated": False,
                "missing": True,
                "price_path_status": "missing",
                "warning": warning,
            }

        payload = self._get(str(request.get("endpoint") or self.token_endpoint), dict(request.get("params") or {}), dict(request.get("headers") or {}))
        rows = _extract_rows(payload)
        truncated = False
        missing = False
        warning = None
        provider_row_count = len(rows)
        if isinstance(payload, dict):
            truncated = bool(payload.get("truncated"))
            missing = bool(payload.get("missing"))
            warning = payload.get("warning")
            data = payload.get("data")
            if warning is None and isinstance(data, dict):
                warning = data.get("warning")
            if warning is None and payload.get("success") is False:
                warning = "provider_empty_payload"

        observations = self._normalize_observations(rows, start_ts=start_ts)
        if provider_row_count > 0 and not observations:
            warning = warning or "price_rows_unparseable"
            missing = True
        elif not observations:
            missing = True
            warning = warning or "no_ohlcv_rows"
        if end_ts is not None and observations and observations[-1]["timestamp"] < int(end_ts):
            truncated = True
            warning = warning or "price_path_incomplete"

        status = "complete"
        if missing:
            status = "missing"
        elif truncated:
            status = "partial"

        return {
            "token_address": token_address,
            "pair_address": pair_address,
            "source_provider": self.provider,
            "price_history_provider": self.provider,
            "price_history_provider_status": self.provider_status,
            "provider_bootstrap_ok": self.provider_status == "configured",
            "provider_config_source": self.provider_config_source,
            "provider_request_summary": request_summary,
            "requested_start_ts": start_ts,
            "requested_end_ts": end_ts,
            "interval_sec": interval_sec,
            "request_params": request_params,
            "provider_row_count": provider_row_count,
            "price_path": observations,
            "truncated": truncated,
            "missing": missing,
            "price_path_status": status,
            "warning": warning,
        }
