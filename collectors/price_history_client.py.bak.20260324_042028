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
    "geckoterminal": "geckoterminal_pool_ohlcv",
    "geckoterminal_pool": "geckoterminal_pool_ohlcv",
    "geckoterminal_pool_ohlcv": "geckoterminal_pool_ohlcv",
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
    "geckoterminal_pool_ohlcv": {
        "base_url": "https://api.geckoterminal.com/api/v2",
        "token_endpoint": "networks/{network}/tokens/{token_address}/pools",
        "pair_endpoint": "networks/{network}/pools/{pool_address}/ohlcv/{timeframe}",
        "chain": "solana",
        "auth_header": None,
        "require_pair_address": False,
        "allow_pairless_token_lookup": True,
        "request_kind": "pool_ohlcv",
        "request_version": "20230302",
        "currency": "usd",
        "token_side": "token",
        "include_empty_intervals": True,
        "pool_resolver": "geckoterminal",
        "resolver_cache_ttl_sec": 86400,
        "max_ohlcv_limit": 1000,
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

_REQUEST_VERSION_KEY_PATHS = (
    ("providers", "price_history", "request_version"),
    ("price_history", "request_version"),
)

_CURRENCY_KEY_PATHS = (
    ("providers", "price_history", "currency"),
    ("price_history", "currency"),
)

_TOKEN_SIDE_KEY_PATHS = (
    ("providers", "price_history", "token_side"),
    ("providers", "price_history", "token"),
    ("price_history", "token_side"),
    ("price_history", "token"),
)

_INCLUDE_EMPTY_INTERVALS_KEY_PATHS = (
    ("providers", "price_history", "include_empty_intervals"),
    ("price_history", "include_empty_intervals"),
)

_POOL_RESOLVER_KEY_PATHS = (
    ("providers", "price_history", "pool_resolver"),
    ("price_history", "pool_resolver"),
)

_RESOLVER_CACHE_TTL_KEY_PATHS = (
    ("providers", "price_history", "resolver_cache_ttl_sec"),
    ("price_history", "resolver_cache_ttl_sec"),
)

_MAX_OHLCV_LIMIT_KEY_PATHS = (
    ("providers", "price_history", "max_ohlcv_limit"),
    ("price_history", "max_ohlcv_limit"),
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
    request_version, _ = _first_config_value(config, _REQUEST_VERSION_KEY_PATHS)
    currency, _ = _first_config_value(config, _CURRENCY_KEY_PATHS)
    token_side, _ = _first_config_value(config, _TOKEN_SIDE_KEY_PATHS)
    include_empty_intervals, _ = _first_config_value(config, _INCLUDE_EMPTY_INTERVALS_KEY_PATHS)
    pool_resolver, _ = _first_config_value(config, _POOL_RESOLVER_KEY_PATHS)
    resolver_cache_ttl_sec, _ = _first_config_value(config, _RESOLVER_CACHE_TTL_KEY_PATHS)
    max_ohlcv_limit, _ = _first_config_value(config, _MAX_OHLCV_LIMIT_KEY_PATHS)

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
        "auth_header": None if defaults.get("auth_header") is None and not api_key else str(defaults.get("auth_header") or "X-API-KEY"),
        "require_pair_address": _coerce_bool(require_pair_address, bool(defaults.get("require_pair_address", False))),
        "allow_pairless_token_lookup": _coerce_bool(allow_pairless_token_lookup, bool(defaults.get("allow_pairless_token_lookup", True))),
        "request_version": str(request_version or defaults.get("request_version") or "").strip() or None,
        "currency": str(currency or defaults.get("currency") or "usd").strip() or "usd",
        "token_side": str(token_side or defaults.get("token_side") or "token").strip() or "token",
        "include_empty_intervals": _coerce_bool(include_empty_intervals, bool(defaults.get("include_empty_intervals", True))),
        "pool_resolver": str(pool_resolver or defaults.get("pool_resolver") or "").strip() or None,
        "resolver_cache_ttl_sec": _coerce_int(resolver_cache_ttl_sec if resolver_cache_ttl_sec is not None else defaults.get("resolver_cache_ttl_sec")) or 0,
        "max_ohlcv_limit": min(max(_coerce_int(max_ohlcv_limit if max_ohlcv_limit is not None else defaults.get("max_ohlcv_limit")) or 1000, 1), 1000),
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
        "request_version": merged.get("request_version"),
        "currency": merged.get("currency"),
        "token_side": merged.get("token_side"),
        "include_empty_intervals": bool(merged.get("include_empty_intervals")),
        "pool_resolver": merged.get("pool_resolver"),
        "resolver_cache_ttl_sec": merged.get("resolver_cache_ttl_sec"),
        "max_ohlcv_limit": merged.get("max_ohlcv_limit"),
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
    elif provider_name == "geckoterminal_pool_ohlcv":
        endpoint = None
        params = {}
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
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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
        "request_kind": provider_config.get("request_kind"),
        "request_version": provider_config.get("request_version"),
        "currency": provider_config.get("currency"),
        "token_side": provider_config.get("token_side"),
        "include_empty_intervals": provider_config.get("include_empty_intervals"),
        "pool_resolver": provider_config.get("pool_resolver"),
        "resolver_cache_ttl_sec": provider_config.get("resolver_cache_ttl_sec"),
        "max_ohlcv_limit": provider_config.get("max_ohlcv_limit"),
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
        request_version: str | None = None,
        currency: str = "usd",
        token_side: str = "token",
        include_empty_intervals: bool = True,
        pool_resolver: str | None = None,
        resolver_cache_ttl_sec: int = 0,
        max_ohlcv_limit: int = 1000,
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
        self.request_version = str(request_version or "").strip() or None
        self.currency = str(currency or "usd").strip() or "usd"
        self.token_side = str(token_side or "token").strip() or "token"
        self.include_empty_intervals = bool(include_empty_intervals)
        self.pool_resolver = str(pool_resolver or "").strip() or None
        self.resolver_cache_ttl_sec = max(int(resolver_cache_ttl_sec or 0), 0)
        self.max_ohlcv_limit = min(max(int(max_ohlcv_limit or 1000), 1), 1000)
        self._resolver_cache: dict[str, tuple[int, dict[str, Any]]] = {}

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
            "request_version": self.request_version,
            "currency": self.currency,
            "token_side": self.token_side,
            "include_empty_intervals": self.include_empty_intervals,
            "pool_resolver": self.pool_resolver,
            "resolver_cache_ttl_sec": self.resolver_cache_ttl_sec,
            "max_ohlcv_limit": self.max_ohlcv_limit,
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
                "request_version": self.request_version,
                "currency": self.currency,
                "token_side": self.token_side,
                "include_empty_intervals": self.include_empty_intervals,
                "pool_resolver": self.pool_resolver,
                "resolver_cache_ttl_sec": self.resolver_cache_ttl_sec,
                "max_ohlcv_limit": self.max_ohlcv_limit,
            },
        }

    def _get(self, endpoint: str, params: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
        if not self.base_url:
            return {"rows": [], "missing": True, "warning": "price_history_provider_unconfigured"}
        query = {key: value for key, value in dict(params).items() if value not in (None, "")}
        req_headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        if self.provider.startswith("birdeye"):
            req_headers["x-chain"] = self.chain
        if self.auth_header and self.api_key:
            req_headers[self.auth_header] = self.api_key
        if headers:
            req_headers.update(headers)
        req = Request(f"{self.base_url}/{endpoint}?{urlencode(query)}", headers=req_headers)
        try:
            with urlopen(req, timeout=20) as response:
                raw_body = response.read().decode("utf-8", errors="replace")
                payload = json.loads(raw_body)
                if isinstance(payload, dict):
                    payload.setdefault("http_status", int(getattr(response, "status", 200) or 200))
                return payload
        except HTTPError as exc:
            body_text = None
            body_payload = None
            provider_error_message = None
            try:
                raw = exc.read()
                body_text = raw.decode("utf-8", errors="replace").strip() if raw else None
                if body_text:
                    try:
                        body_payload = json.loads(body_text)
                    except json.JSONDecodeError:
                        body_payload = None
            except Exception:
                body_text = None
                body_payload = None
            if isinstance(body_payload, dict):
                provider_error_message = body_payload.get("message") or body_payload.get("error") or body_payload.get("warning")
            warning = "provider_rate_limited" if int(exc.code or 0) == 429 else "provider_http_error"
            return {
                "rows": [],
                "missing": True,
                "warning": warning,
                "http_status": int(exc.code),
                "provider_error_message": str(provider_error_message).strip() if provider_error_message else None,
                "provider_error_body": body_text,
                "provider_error_payload": body_payload if isinstance(body_payload, dict) else None,
            }
        except (URLError, TimeoutError) as exc:
            return {
                "rows": [],
                "missing": True,
                "warning": "provider_http_error",
                "http_status": None,
                "provider_error_message": str(exc),
                "provider_error_body": None,
                "provider_error_payload": None,
            }
        except json.JSONDecodeError:
            return {
                "rows": [],
                "missing": True,
                "warning": "provider_empty_payload",
                "http_status": 200,
                "provider_error_message": "json_decode_error",
                "provider_error_body": None,
                "provider_error_payload": None,
            }

    def _normalize_observations(self, rows: list[dict[str, Any]], *, start_ts: int | None) -> list[dict[str, Any]]:
        observations: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _coerce_int(row.get("timestamp") or row.get("unixTime") or row.get("unix_time") or row.get("ts") or row.get("time") or row.get("t"))
            price = _coerce_float(row.get("price") or row.get("close") or row.get("c") or row.get("close_price") or row.get("value"))
            if ts is None or price is None:
                continue
            offset = _coerce_int(row.get("offset_sec") or row.get("elapsed_sec"))
            if offset is None and start_ts is not None:
                offset = max(0, ts - start_ts)
            observations.append({"timestamp": ts, "offset_sec": int(offset or 0), "price": price})
        observations.sort(key=lambda item: (item.get("offset_sec", 0), item.get("timestamp", 0)))
        return observations

    def _geckoterminal_network(self) -> str:
        return str(self.chain or "solana").strip() or "solana"

    def _format_endpoint(self, template: str, **kwargs: Any) -> str:
        return str(template or "").format(**kwargs).lstrip("/")

    def _extract_geckoterminal_pool_candidates(self, payload: Any) -> list[dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            return []
        included_map: dict[str, dict[str, Any]] = {}
        included = payload.get("included") if isinstance(payload, dict) else None
        if isinstance(included, list):
            for item in included:
                if isinstance(item, dict):
                    included_map[f"{item.get('type')}:{item.get('id')}"] = item
        pools: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
            relationships = item.get("relationships") if isinstance(item.get("relationships"), dict) else {}
            dex_name = str(attrs.get("dex_name") or "")
            dex_ref = ((relationships.get("dex") or {}).get("data") if isinstance(relationships.get("dex"), dict) else None)
            if isinstance(dex_ref, dict):
                dex_item = included_map.get(f"{dex_ref.get('type')}:{dex_ref.get('id')}")
                if isinstance(dex_item, dict):
                    dex_name = str((dex_item.get("attributes") or {}).get("name") or dex_name)
            pools.append(
                {
                    "pool_address": item.get("id"),
                    "reserve_in_usd": _coerce_float(attrs.get("reserve_in_usd") or attrs.get("reserve_usd")) or 0.0,
                    "volume_usd_h24": _coerce_float(attrs.get("volume_usd") or attrs.get("volume_usd_h24")) or 0.0,
                    "market_cap_usd": _coerce_float(attrs.get("market_cap_usd")) or 0.0,
                    "dex_name": dex_name,
                    "name": str(attrs.get("name") or ""),
                }
            )
        return pools

    def _select_canonical_pool(self, pools: list[dict[str, Any]]) -> dict[str, Any] | None:
        candidates = [pool for pool in pools if str(pool.get("pool_address") or "").strip()]
        if not candidates:
            return None
        return sorted(
            candidates,
            key=lambda item: (
                -float(item.get("reserve_in_usd") or 0.0),
                -float(item.get("volume_usd_h24") or 0.0),
                -float(item.get("market_cap_usd") or 0.0),
                str(item.get("dex_name") or ""),
                str(item.get("pool_address") or ""),
            ),
        )[0]

    def _resolve_geckoterminal_pool(self, token_address: str, network: str = "solana") -> dict[str, Any]:
        import time

        cache_key = f"{network}:{token_address}"
        now = int(time.time())
        cached = self._resolver_cache.get(cache_key)
        if self.resolver_cache_ttl_sec > 0 and cached and cached[0] > now:
            return dict(cached[1])
        endpoint = self._format_endpoint(self.token_endpoint, network=network, token_address=token_address)
        params = {"page": 1}
        if self.token_side:
            params["token"] = self.token_side
        if self.request_version:
            params["request_version"] = self.request_version
        payload = self._get(endpoint, params)
        pools = self._extract_geckoterminal_pool_candidates(payload)
        selected = self._select_canonical_pool(pools)
        result = {
            "pool_address": selected.get("pool_address") if selected else None,
            "resolver_source": self.pool_resolver or "geckoterminal",
            "resolver_confidence": "high" if selected else "none",
            "pool_candidates_seen": len(pools),
            "pool_resolution_status": "resolved" if selected else "pool_resolution_failed",
            "http_status": payload.get("http_status") if isinstance(payload, dict) else None,
            "provider_error_message": payload.get("provider_error_message") if isinstance(payload, dict) else None,
            "warning": None if selected else ((payload.get("warning") if isinstance(payload, dict) else None) or "pool_resolution_failed"),
        }
        if self.resolver_cache_ttl_sec > 0:
            self._resolver_cache[cache_key] = (now + self.resolver_cache_ttl_sec, dict(result))
        return result

    def _geckoterminal_aggregate(self, interval_sec: int) -> int:
        if interval_sec <= 60:
            return 1
        if interval_sec <= 300:
            return 5
        return 15

    def _fetch_geckoterminal_pool_ohlcv(
        self,
        pool_address: str,
        *,
        start_ts: int | None,
        end_ts: int | None,
        interval_sec: int,
        limit: int,
        network: str,
    ) -> Any:
        endpoint = self._format_endpoint(self.pair_endpoint, network=network, pool_address=pool_address, timeframe="minute")
        params = {
            "aggregate": self._geckoterminal_aggregate(interval_sec),
            "before_timestamp": end_ts,
            "limit": min(max(limit, 1), self.max_ohlcv_limit),
            "currency": self.currency,
            "include_empty_intervals": str(bool(self.include_empty_intervals)).lower(),
        }
        if self.request_version:
            params["request_version"] = self.request_version
        return self._get(endpoint, params)

    def _extract_geckoterminal_ohlcv_rows(self, payload: Any) -> list[list[Any]]:
        if not isinstance(payload, dict):
            return []
        attrs = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        attributes = attrs.get("attributes") if isinstance(attrs.get("attributes"), dict) else {}
        rows = attributes.get("ohlcv_list")
        return [row for row in rows if isinstance(row, list) and len(row) >= 6] if isinstance(rows, list) else []

    def _normalize_geckoterminal_ohlcv_list(self, rows: list[list[Any]], *, start_ts: int | None, end_ts: int | None) -> list[dict[str, Any]]:
        observations: list[dict[str, Any]] = []
        seen: set[int] = set()
        for row in rows:
            ts = _coerce_int(row[0] if len(row) > 0 else None)
            price = _coerce_float(row[4] if len(row) > 4 else None)
            volume = _coerce_float(row[5] if len(row) > 5 else None)
            if ts is None or price is None or ts in seen:
                continue
            if start_ts is not None and ts < start_ts:
                continue
            if end_ts is not None and ts > end_ts:
                continue
            seen.add(ts)
            observations.append({
                "timestamp": ts,
                "offset_sec": max(0, ts - int(start_ts or ts)),
                "price": price,
                "volume": volume,
            })
        observations.sort(key=lambda item: item["timestamp"])
        return observations

    def _fetch_geckoterminal_price_path(
        self,
        *,
        token_address: str,
        pair_address: str | None,
        start_ts: int | None,
        end_ts: int | None,
        interval_sec: int,
        limit: int,
        provider_config: dict[str, Any],
    ) -> dict[str, Any]:
        request_summary = dict(provider_config.get("provider_request_summary") or {})
        request_summary.update({
            "requested_token_address": token_address,
            "requested_pair_address": pair_address,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "interval_sec": interval_sec,
            "interval_label": _interval_label(interval_sec),
            "limit": limit,
            "request_kind": "pool_ohlcv",
        })
        network = self._geckoterminal_network()
        pool_info = {
            "pool_address": pair_address,
            "resolver_source": "seed_pair_address" if pair_address else self.pool_resolver or "geckoterminal",
            "resolver_confidence": "seed" if pair_address else "none",
            "pool_candidates_seen": 1 if pair_address else 0,
            "pool_resolution_status": "seed_pair_address" if pair_address else "pool_resolution_failed",
            "warning": None,
            "http_status": None,
            "provider_error_message": None,
        }
        if not pair_address:
            pool_info = self._resolve_geckoterminal_pool(token_address, network=network)
        selected_pool_address = pool_info.get("pool_address")
        request_summary.update({
            "network": network,
            "selected_pool_address": selected_pool_address,
            "pool_resolver_source": pool_info.get("resolver_source"),
            "pool_resolver_confidence": pool_info.get("resolver_confidence"),
            "pool_candidates_seen": pool_info.get("pool_candidates_seen"),
            "pool_resolution_status": pool_info.get("pool_resolution_status"),
        })
        request_params = {
            "token_address": token_address,
            "pair_address": pair_address or None,
            "pool_address": selected_pool_address,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "interval_sec": interval_sec,
            "limit": limit,
        }
        if not selected_pool_address:
            return {
                "token_address": token_address,
                "pair_address": pair_address,
                "pool_address": None,
                "selected_pool_address": None,
                "pool_resolver_source": pool_info.get("resolver_source"),
                "pool_resolver_confidence": pool_info.get("resolver_confidence"),
                "pool_candidates_seen": pool_info.get("pool_candidates_seen"),
                "pool_resolution_status": "pool_resolution_failed",
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
                "warning": pool_info.get("warning") or "pool_resolution_failed",
                "http_status": pool_info.get("http_status"),
                "provider_error_message": pool_info.get("provider_error_message"),
                "provider_error_body": None,
                "provider_error_payload": None,
            }
        raw_rows: list[list[Any]] = []
        before_ts = end_ts
        warning = None
        http_status = None
        provider_error_message = None
        seen_oldest: set[int] = set()
        while True:
            payload = self._fetch_geckoterminal_pool_ohlcv(
                selected_pool_address,
                start_ts=start_ts,
                end_ts=before_ts,
                interval_sec=interval_sec,
                limit=min(limit, self.max_ohlcv_limit),
                network=network,
            )
            batch = self._extract_geckoterminal_ohlcv_rows(payload)
            raw_rows.extend(batch)
            if isinstance(payload, dict):
                warning = payload.get("warning") or warning
                http_status = payload.get("http_status", http_status)
                provider_error_message = payload.get("provider_error_message", provider_error_message)
            if not batch:
                break
            oldest_ts = min(_coerce_int(row[0]) or 0 for row in batch)
            if oldest_ts <= int(start_ts or oldest_ts) or len(batch) < min(limit, self.max_ohlcv_limit) or oldest_ts in seen_oldest:
                break
            seen_oldest.add(oldest_ts)
            before_ts = oldest_ts - 1
        observations = self._normalize_geckoterminal_ohlcv_list(raw_rows, start_ts=start_ts, end_ts=end_ts)
        missing = not observations
        truncated = bool(end_ts is not None and observations and observations[-1]["timestamp"] < int(end_ts))
        if missing:
            warning = warning or "no_pool_ohlcv_rows"
        elif truncated:
            warning = warning or "price_path_incomplete"
        return {
            "token_address": token_address,
            "pair_address": pair_address,
            "pool_address": selected_pool_address,
            "selected_pool_address": selected_pool_address,
            "pool_resolver_source": pool_info.get("resolver_source"),
            "pool_resolver_confidence": pool_info.get("resolver_confidence"),
            "pool_candidates_seen": pool_info.get("pool_candidates_seen"),
            "pool_resolution_status": pool_info.get("pool_resolution_status"),
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
            "provider_row_count": len(raw_rows),
            "price_path": observations,
            "truncated": truncated,
            "missing": missing,
            "price_path_status": "missing" if missing else ("partial" if truncated else "complete"),
            "warning": warning,
            "http_status": http_status,
            "provider_error_message": provider_error_message,
            "provider_error_body": None,
            "provider_error_payload": None,
        }

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
        if self.provider == "geckoterminal_pool_ohlcv":
            return self._fetch_geckoterminal_price_path(
                token_address=token_address,
                pair_address=pair_address,
                start_ts=start_ts,
                end_ts=end_ts,
                interval_sec=interval_sec,
                limit=limit,
                provider_config=provider_config,
            )

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
        http_status = None
        provider_error_message = None
        provider_error_body = None
        provider_error_payload = None
        provider_row_count = len(rows)
        if isinstance(payload, dict):
            truncated = bool(payload.get("truncated"))
            missing = bool(payload.get("missing"))
            warning = payload.get("warning")
            http_status = payload.get("http_status")
            provider_error_message = payload.get("provider_error_message")
            provider_error_body = payload.get("provider_error_body")
            provider_error_payload = payload.get("provider_error_payload")
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
            "http_status": http_status,
            "provider_error_message": provider_error_message,
            "provider_error_body": provider_error_body,
            "provider_error_payload": provider_error_payload,
        }
