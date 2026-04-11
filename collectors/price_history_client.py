"""Canonical historical price-path collection helpers for replay backfill."""

from __future__ import annotations

import json
import math
import time
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
    _gecko_global_next_request_at: float = 0.0
    _gecko_global_cooldown_until: float = 0.0

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
        gecko_min_request_interval_sec: float = 0.0,
        gecko_rate_limit_cooldown_sec: float = 15.0,
        gecko_ohlcv_404_negative_ttl_sec: float = 1800.0,
        gecko_max_pages_per_token: int = 12,
        request_timeout_sec: float = 20.0,
        transport_memory: dict[str, Any] | None = None,
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
        self.gecko_min_request_interval_sec = max(float(gecko_min_request_interval_sec or 0.0), 0.0)
        self.gecko_rate_limit_cooldown_sec = max(float(gecko_rate_limit_cooldown_sec or 0.0), 0.0)
        self.gecko_ohlcv_404_negative_ttl_sec = max(float(gecko_ohlcv_404_negative_ttl_sec or 0.0), 0.0)
        self.gecko_max_pages_per_token = max(int(gecko_max_pages_per_token or 1), 1)
        self.request_timeout_sec = max(float(request_timeout_sec or 20.0), 1.0)
        memory = transport_memory if isinstance(transport_memory, dict) else {}
        self.transport_memory = memory
        self._transport_provider_cooldowns = memory.setdefault("provider_cooldowns", {})
        self._transport_non_retryable = memory.setdefault("non_retryable", {})
        self._resolver_cache: dict[str, tuple[int, dict[str, Any]]] = {}
        self._gecko_cooldown_until_monotonic: float = 0.0
        self._gecko_last_rate_limit_class: str | None = None
        self._gecko_negative_cache: dict[str, float] = {}

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
                "gecko_min_request_interval_sec": self.gecko_min_request_interval_sec,
                "gecko_rate_limit_cooldown_sec": self.gecko_rate_limit_cooldown_sec,
                "gecko_ohlcv_404_negative_ttl_sec": self.gecko_ohlcv_404_negative_ttl_sec,
                "gecko_max_pages_per_token": self.gecko_max_pages_per_token,
                "request_timeout_sec": self.request_timeout_sec,
            },
        }

    def _acquire_gecko_request_slot(self) -> None:
        now = time.monotonic()
        wait_until = max(
            float(self.__class__._gecko_global_next_request_at),
            float(self.__class__._gecko_global_cooldown_until),
        )
        if wait_until > now:
            time.sleep(wait_until - now)
        self.__class__._gecko_global_next_request_at = time.monotonic() + self.gecko_min_request_interval_sec

    def _gecko_get(self, endpoint: str, params: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
        self._acquire_gecko_request_slot()
        payload = self._get(endpoint, params, headers=headers)
        if isinstance(payload, dict) and (
            payload.get("warning") == "provider_rate_limited" or int(payload.get("http_status") or 0) == 429
        ):
            self._set_gecko_cooldown()
            self.__class__._gecko_global_cooldown_until = max(
                float(self.__class__._gecko_global_cooldown_until),
                time.monotonic() + self.gecko_rate_limit_cooldown_sec,
            )
        return payload

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
            with urlopen(req, timeout=self.request_timeout_sec) as response:
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
                "warning": "provider_timeout" if isinstance(exc, TimeoutError) else "provider_http_error",
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

    def _set_gecko_cooldown(self, *, failure_class: str | None = None) -> None:
        if self.gecko_rate_limit_cooldown_sec <= 0:
            return
        self._gecko_cooldown_until_monotonic = max(
            float(self._gecko_cooldown_until_monotonic),
            time.monotonic() + self.gecko_rate_limit_cooldown_sec,
        )
        if failure_class:
            self._gecko_last_rate_limit_class = failure_class
            self._transport_provider_cooldowns["gecko"] = {
                "until_monotonic": float(self._gecko_cooldown_until_monotonic),
                "failure_class": failure_class,
            }

    def _classify_provider_failure(self, *, stage: str, http_status: int | None, warning: str | None) -> tuple[str | None, bool]:
        status = int(http_status or 0)
        normalized_warning = str(warning or "").strip().lower()
        if normalized_warning == "provider_timeout":
            return "provider_timeout", True
        if status == 429 or normalized_warning == "provider_rate_limited":
            if stage == "resolver":
                return "rate_limited_resolver", True
            if stage == "ohlcv":
                return "rate_limited_ohlcv", True
            return "provider_http_error", True
        if status == 404 and stage == "ohlcv":
            return "ohlcv_not_available", False
        if status == 404:
            return "provider_http_error", False
        if status >= 500:
            return "provider_http_error", True
        if status >= 400:
            return "provider_http_error", True
        if normalized_warning in {"pool_resolution_failed", "no_pool_ohlcv_rows"}:
            return None, True
        return None, True

    def _apply_transport_decision(self, row: dict[str, Any]) -> dict[str, Any]:
        row.setdefault("price_history_route_selected", None)
        row.setdefault("price_history_route_attempts", [])
        row.setdefault("price_history_router_status", None)
        row.setdefault("price_history_router_warning", None)
        row.setdefault("price_history_fallback_used", False)
        row.setdefault("selected_route_provider", None)
        row.setdefault("selected_route_kind", None)
        row.setdefault("selected_route_seed_source", None)
        provider_failure_class = str(row.get("provider_failure_class") or "")
        if bool(row.get("cooldown_applied")):
            transport_action = "skip_due_to_cooldown"
            transport_reason = provider_failure_class or "provider_rate_limited_recently"
        elif row.get("price_path_status") == "complete" or bool(row.get("partial_but_usable_row")):
            transport_action = "accept"
            transport_reason = "usable_price_path"
        elif provider_failure_class in {"rate_limited_resolver", "rate_limited_ohlcv", "provider_timeout"}:
            transport_action = "retry_later"
            transport_reason = provider_failure_class
        elif provider_failure_class == "ohlcv_not_available":
            transport_action = "skip_non_retryable"
            transport_reason = "ohlcv_not_available"
        else:
            transport_action = "retry_later" if bool(row.get("provider_failure_retryable", True)) else "skip_non_retryable"
            transport_reason = provider_failure_class or "provider_http_error"
        row["transport_action"] = transport_action
        row["transport_reason"] = transport_reason
        row["usable_for_replay"] = bool(row.get("replay_usable_price_path")) or bool(row.get("partial_but_usable_row")) or row.get("price_path_status") == "complete"
        row["usable_for_sampling"] = row["usable_for_replay"]
        return row

    def _gecko_negative_cache_key(self, *, network: str, pool_address: str, interval_sec: int) -> str:
        return f"{network}:{pool_address}:{int(interval_sec)}"

    def _gecko_negative_cache_hit(self, *, network: str, pool_address: str, interval_sec: int) -> bool:
        key = self._gecko_negative_cache_key(network=network, pool_address=pool_address, interval_sec=interval_sec)
        expires_at = max(
            float(self._gecko_negative_cache.get(key) or 0.0),
            float(self._transport_non_retryable.get(key) or 0.0),
        )
        now = time.monotonic()
        if expires_at <= now:
            if key in self._gecko_negative_cache:
                self._gecko_negative_cache.pop(key, None)
            if key in self._transport_non_retryable:
                self._transport_non_retryable.pop(key, None)
            return False
        return True

    def _remember_gecko_ohlcv_not_available(self, *, network: str, pool_address: str, interval_sec: int) -> None:
        if self.gecko_ohlcv_404_negative_ttl_sec <= 0:
            return
        key = self._gecko_negative_cache_key(network=network, pool_address=pool_address, interval_sec=interval_sec)
        self._gecko_negative_cache[key] = time.monotonic() + self.gecko_ohlcv_404_negative_ttl_sec
        self._transport_non_retryable[key] = time.monotonic() + self.gecko_ohlcv_404_negative_ttl_sec

    def _with_provider_rate_limit_retry(
        self,
        func: Any,
        *,
        max_attempts: int = 4,
        backoff_sec: tuple[float, ...] = (1.0, 2.0, 4.0),
    ) -> Any:
        attempts = max(1, int(max_attempts))
        response = None
        for attempt_index in range(attempts):
            response = func()
            if not isinstance(response, dict) or response.get("warning") != "provider_rate_limited":
                return response
            if attempt_index >= attempts - 1:
                return response
            delay_idx = min(attempt_index, max(0, len(backoff_sec) - 1))
            time.sleep(float(backoff_sec[delay_idx]))
        return response

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
        payload = self._gecko_get(endpoint, params)
        pools = self._extract_geckoterminal_pool_candidates(payload)
        selected = self._select_canonical_pool(pools)
        result = {
            "pool_address": selected.get("pool_address") if selected else None,
            "resolver_source": self.pool_resolver or "geckoterminal",
            "resolver_confidence": "high" if selected else "none",
            "pool_candidates_seen": len(pools),
            "pool_resolution_status": "resolved" if selected else "pool_resolution_failed",
            "endpoint": endpoint,
            "http_status": payload.get("http_status") if isinstance(payload, dict) else None,
            "provider_error_message": payload.get("provider_error_message") if isinstance(payload, dict) else None,
            "provider_error_body": payload.get("provider_error_body") if isinstance(payload, dict) else None,
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
        return self._gecko_get(endpoint, params)

    def _extract_geckoterminal_ohlcv_rows(self, payload: Any) -> list[list[Any]]:
        if not isinstance(payload, dict):
            return []
        attrs = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        attributes = attrs.get("attributes") if isinstance(attrs.get("attributes"), dict) else {}
        rows = attributes.get("ohlcv_list")
        return [row for row in rows if isinstance(row, list) and len(row) >= 6] if isinstance(rows, list) else []

    def _normalize_geckoterminal_ohlcv_list(
        self,
        rows: list[list[Any]],
        *,
        start_ts: int | None,
        end_ts: int | None,
        interval_sec: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        observed_rows: list[dict[str, Any]] = []
        seen: set[int] = set()
        for row in rows:
            ts = _coerce_int(row[0] if len(row) > 0 else None)
            close = _coerce_float(row[4] if len(row) > 4 else None)
            volume = _coerce_float(row[5] if len(row) > 5 else None)
            if ts is None or close is None or ts in seen:
                continue
            if start_ts is not None and ts < start_ts:
                continue
            if end_ts is not None and ts > end_ts:
                continue
            seen.add(ts)
            observed_rows.append({
                "timestamp": ts,
                "offset_sec": max(0, ts - int(start_ts or ts)),
                "price": close,
                "volume": volume,
            })
        observed_rows.sort(key=lambda item: item["timestamp"])

        densified: list[dict[str, Any]] = []
        gap_fill_count = 0
        if observed_rows:
            densified.append(dict(observed_rows[0]))
        for idx in range(1, len(observed_rows)):
            prev = observed_rows[idx - 1]
            current = observed_rows[idx]
            prev_ts = int(prev["timestamp"])
            current_ts = int(current["timestamp"])
            prev_close = _coerce_float(prev.get("price"))
            if interval_sec > 0 and prev_close is not None and current_ts > prev_ts:
                step = prev_ts + interval_sec
                while step < current_ts:
                    densified.append(
                        {
                            "timestamp": int(step),
                            "offset_sec": max(0, int(step) - int(start_ts or step)),
                            "price": prev_close,
                            "volume": 0.0,
                            "gap_filled": True,
                        }
                    )
                    gap_fill_count += 1
                    step += interval_sec
            densified.append(dict(current))

        metadata = {
            "gap_fill_applied": gap_fill_count > 0,
            "gap_fill_count": gap_fill_count,
            "observed_row_count": len(observed_rows),
            "densified_row_count": len(densified),
            "price_path_origin": "provider_observed_plus_gap_fill" if gap_fill_count > 0 else "provider_observed",
        }
        return densified, metadata

    def _build_gecko_path_debug_fields(
        self,
        *,
        status: str,
        observations: list[dict[str, Any]],
        truncated: bool,
        terminated_on_rate_limit: bool,
        rate_limit_stage: str,
        resolver_endpoint: str | None,
        ohlcv_endpoint: str | None,
        pool_resolution_http_status: int | None,
        ohlcv_http_status: int | None,
    ) -> dict[str, Any]:
        point_count = len(observations)
        replay_usable_price_path = point_count > 0
        partial_but_usable_row = status == "partial" and replay_usable_price_path

        if status == "complete" and replay_usable_price_path:
            replay_data_hint = "historical"
        elif replay_usable_price_path:
            replay_data_hint = "historical_partial"
        else:
            replay_data_hint = "missing_price_path"

        if rate_limit_stage == "resolver":
            rate_limit_endpoint = resolver_endpoint
            rate_limit_http_status = pool_resolution_http_status
            collection_termination_reason = (
                "rate_limited_resolver" if terminated_on_rate_limit else "resolver_unavailable"
            )
        elif rate_limit_stage == "ohlcv":
            rate_limit_endpoint = ohlcv_endpoint
            rate_limit_http_status = ohlcv_http_status
            collection_termination_reason = "rate_limited_ohlcv"
        elif status == "complete":
            rate_limit_endpoint = None
            rate_limit_http_status = None
            collection_termination_reason = "complete_window"
        elif truncated:
            rate_limit_endpoint = None
            rate_limit_http_status = None
            collection_termination_reason = "truncated_window"
        elif status == "partial":
            rate_limit_endpoint = None
            rate_limit_http_status = None
            collection_termination_reason = "partial_window"
        else:
            rate_limit_endpoint = None
            rate_limit_http_status = None
            collection_termination_reason = "missing_window"

        return {
            "partial_but_usable_row": partial_but_usable_row,
            "replay_usable_price_path": replay_usable_price_path,
            "replay_data_hint": replay_data_hint,
            "resolver_endpoint": resolver_endpoint,
            "ohlcv_endpoint": ohlcv_endpoint,
            "rate_limit_endpoint": rate_limit_endpoint,
            "rate_limit_http_status": rate_limit_http_status,
            "collection_termination_reason": collection_termination_reason,
        }

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
            "gecko_min_request_interval_sec": self.gecko_min_request_interval_sec,
            "gecko_rate_limit_cooldown_sec": self.gecko_rate_limit_cooldown_sec,
            "gecko_ohlcv_404_negative_ttl_sec": self.gecko_ohlcv_404_negative_ttl_sec,
            "gecko_max_pages_per_token": self.gecko_max_pages_per_token,
        })
        network = self._geckoterminal_network()
        resolver_endpoint = self._format_endpoint(self.token_endpoint, network=network, token_address=token_address)
        pool_info = {
            "pool_address": pair_address,
            "resolver_source": "seed_pair_address" if pair_address else self.pool_resolver or "geckoterminal",
            "resolver_confidence": "seed" if pair_address else "none",
            "pool_candidates_seen": 1 if pair_address else 0,
            "pool_resolution_status": "seed_pair_address" if pair_address else "pool_resolution_failed",
            "warning": None,
            "endpoint": resolver_endpoint if pair_address else None,
            "http_status": None,
            "pool_resolution_http_status": None,
            "ohlcv_http_status": None,
            "provider_error_message": None,
            "provider_error_body": None,
            "terminated_on_rate_limit": False,
            "rate_limit_stage": "unknown",
            "ohlcv_pages_attempted": 0,
            "ohlcv_pages_succeeded": 0,
        }
        provider_failure_class: str | None = None
        provider_failure_retryable = True
        provider_failure_stage: str | None = None
        cooldown_applied = False
        cooldown_reason: str | None = None
        negative_cache_hit = False
        shared_cooldown = self._transport_provider_cooldowns.get("gecko")
        if isinstance(shared_cooldown, dict):
            self._gecko_cooldown_until_monotonic = max(
                float(self._gecko_cooldown_until_monotonic),
                float(shared_cooldown.get("until_monotonic") or 0.0),
            )
            if not self._gecko_last_rate_limit_class:
                self._gecko_last_rate_limit_class = str(shared_cooldown.get("failure_class") or "") or None
        if self._gecko_cooldown_until_monotonic > time.monotonic():
            cooldown_applied = True
            cooldown_reason = "provider_rate_limited_recently"
            provider_failure_stage = "cooldown"
            provider_failure_class = self._gecko_last_rate_limit_class or "provider_rate_limited_recently"
            provider_failure_retryable = True
            debug_fields = self._build_gecko_path_debug_fields(
                status="missing",
                observations=[],
                truncated=False,
                terminated_on_rate_limit=True,
                rate_limit_stage="resolver",
                resolver_endpoint=resolver_endpoint,
                ohlcv_endpoint=None,
                pool_resolution_http_status=429,
                ohlcv_http_status=None,
            )
            return {
                "token_address": token_address,
                "pair_address": pair_address,
                "pool_address": None,
                "selected_pool_address": None,
                "pool_resolver_source": pool_info.get("resolver_source"),
                "pool_resolver_confidence": pool_info.get("resolver_confidence"),
                "pool_candidates_seen": pool_info.get("pool_candidates_seen"),
                "pool_resolution_status": "cooldown_skipped",
                "source_provider": self.provider,
                "price_history_provider": self.provider,
                "price_history_provider_status": self.provider_status,
                "provider_bootstrap_ok": self.provider_status == "configured",
                "provider_config_source": self.provider_config_source,
                "provider_request_summary": request_summary,
                "requested_start_ts": start_ts,
                "requested_end_ts": end_ts,
                "interval_sec": interval_sec,
                "request_params": {
                    "token_address": token_address,
                    "pair_address": pair_address or None,
                    "pool_address": None,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "interval_sec": interval_sec,
                    "limit": limit,
                },
                "provider_row_count": 0,
                "obs_len": 0,
                "gap_fill_applied": False,
                "gap_fill_count": 0,
                "observed_row_count": 0,
                "densified_row_count": 0,
                "price_path_origin": "provider_observed",
                "price_path": [],
                "truncated": False,
                "missing": True,
                "price_path_status": "missing",
                "warning": "provider_rate_limited_recently",
                "endpoint": None,
                "http_status": 429,
                "pool_resolution_http_status": None,
                "ohlcv_http_status": None,
                "provider_error_message": None,
                "provider_error_body": None,
                "provider_error_payload": None,
                "terminated_on_rate_limit": True,
                "rate_limit_stage": "resolver",
                "ohlcv_pages_attempted": 0,
                "ohlcv_pages_succeeded": 0,
                "provider_failure_class": provider_failure_class,
                "provider_failure_retryable": provider_failure_retryable,
                "provider_failure_stage": provider_failure_stage,
                "cooldown_applied": cooldown_applied,
                "cooldown_reason": cooldown_reason,
                "negative_cache_hit": negative_cache_hit,
                **debug_fields,
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
            "endpoint": pool_info.get("endpoint"),
            "pool_resolution_http_status": pool_info.get("http_status"),
            "provider_error_message": pool_info.get("provider_error_message"),
            "provider_error_body": pool_info.get("provider_error_body"),
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
        resolver_rate_limited = (
            pool_info.get("warning") == "provider_rate_limited" or int(pool_info.get("http_status") or 0) == 429
        )
        if not selected_pool_address:
            provider_failure_class, provider_failure_retryable = self._classify_provider_failure(
                stage="resolver",
                http_status=pool_info.get("http_status"),
                warning=pool_info.get("warning"),
            )
            provider_failure_stage = "resolver" if provider_failure_class else None
            if provider_failure_class == "rate_limited_resolver":
                self._set_gecko_cooldown(failure_class=provider_failure_class)
            debug_fields = self._build_gecko_path_debug_fields(
                status="missing",
                observations=[],
                truncated=False,
                terminated_on_rate_limit=resolver_rate_limited,
                rate_limit_stage="resolver" if resolver_rate_limited else "unknown",
                resolver_endpoint=pool_info.get("endpoint"),
                ohlcv_endpoint=None,
                pool_resolution_http_status=pool_info.get("http_status"),
                ohlcv_http_status=None,
            )
            request_summary.update(
                {
                    "resolver_endpoint": pool_info.get("endpoint"),
                    "ohlcv_endpoint": None,
                    "rate_limit_stage": "resolver" if resolver_rate_limited else "unknown",
                    "rate_limit_endpoint": debug_fields.get("rate_limit_endpoint"),
                    "rate_limit_http_status": debug_fields.get("rate_limit_http_status"),
                    "partial_but_usable_row": debug_fields.get("partial_but_usable_row"),
                    "replay_usable_price_path": debug_fields.get("replay_usable_price_path"),
                    "replay_data_hint": debug_fields.get("replay_data_hint"),
                    "collection_termination_reason": debug_fields.get("collection_termination_reason"),
                }
            )
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
                "obs_len": 0,
                "gap_fill_applied": False,
                "gap_fill_count": 0,
                "observed_row_count": 0,
                "densified_row_count": 0,
                "price_path_origin": "provider_observed",
                "price_path": [],
                "truncated": False,
                "missing": True,
                "price_path_status": "missing",
                "warning": pool_info.get("warning") or "pool_resolution_failed",
                "endpoint": pool_info.get("endpoint"),
                "http_status": pool_info.get("http_status"),
                "pool_resolution_http_status": pool_info.get("http_status"),
                "ohlcv_http_status": None,
                "provider_error_message": pool_info.get("provider_error_message"),
                "provider_error_body": pool_info.get("provider_error_body"),
                "provider_error_payload": None,
                "terminated_on_rate_limit": resolver_rate_limited,
                "rate_limit_stage": "resolver" if resolver_rate_limited else "unknown",
                "ohlcv_pages_attempted": 0,
                "ohlcv_pages_succeeded": 0,
                "provider_failure_class": provider_failure_class,
                "provider_failure_retryable": provider_failure_retryable,
                "provider_failure_stage": provider_failure_stage,
                "cooldown_applied": cooldown_applied,
                "cooldown_reason": cooldown_reason,
                "negative_cache_hit": negative_cache_hit,
                **debug_fields,
            }
        ohlcv_endpoint = self._format_endpoint(self.pair_endpoint, network=network, pool_address=selected_pool_address, timeframe="minute")
        request_summary["endpoint"] = ohlcv_endpoint
        request_summary["resolver_endpoint"] = pool_info.get("endpoint")
        request_summary["ohlcv_endpoint"] = ohlcv_endpoint
        raw_rows: list[list[Any]] = []
        before_ts = end_ts
        warning = None
        endpoint = ohlcv_endpoint
        http_status = None
        pool_resolution_http_status = pool_info.get("http_status")
        ohlcv_http_status = None
        provider_error_message = None
        provider_error_body = None
        terminated_on_rate_limit = False
        rate_limit_stage = "unknown"
        ohlcv_pages_attempted = 0
        ohlcv_pages_succeeded = 0
        seen_oldest: set[int] = set()
        max_page_limit = min(max(limit, 1), self.max_ohlcv_limit)
        if self._gecko_negative_cache_hit(network=network, pool_address=selected_pool_address, interval_sec=interval_sec):
            negative_cache_hit = True
            provider_failure_class = "ohlcv_not_available"
            provider_failure_retryable = False
            provider_failure_stage = "ohlcv"
            warning = "no_pool_ohlcv_rows"
            debug_fields = self._build_gecko_path_debug_fields(
                status="missing",
                observations=[],
                truncated=False,
                terminated_on_rate_limit=False,
                rate_limit_stage="unknown",
                resolver_endpoint=pool_info.get("endpoint"),
                ohlcv_endpoint=ohlcv_endpoint,
                pool_resolution_http_status=pool_resolution_http_status,
                ohlcv_http_status=404,
            )
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
                "provider_row_count": 0,
                "obs_len": 0,
                "gap_fill_applied": False,
                "gap_fill_count": 0,
                "observed_row_count": 0,
                "densified_row_count": 0,
                "price_path_origin": "provider_observed",
                "price_path": [],
                "truncated": False,
                "missing": True,
                "price_path_status": "missing",
                "warning": warning,
                "endpoint": ohlcv_endpoint,
                "http_status": 404,
                "pool_resolution_http_status": pool_resolution_http_status,
                "ohlcv_http_status": 404,
                "provider_error_message": None,
                "provider_error_body": None,
                "provider_error_payload": None,
                "terminated_on_rate_limit": False,
                "rate_limit_stage": "unknown",
                "ohlcv_pages_attempted": 0,
                "ohlcv_pages_succeeded": 0,
                "provider_failure_class": provider_failure_class,
                "provider_failure_retryable": provider_failure_retryable,
                "provider_failure_stage": provider_failure_stage,
                "cooldown_applied": cooldown_applied,
                "cooldown_reason": cooldown_reason,
                "negative_cache_hit": negative_cache_hit,
                **debug_fields,
            }
        while ohlcv_pages_attempted < self.gecko_max_pages_per_token:
            ohlcv_pages_attempted += 1
            payload = self._fetch_geckoterminal_pool_ohlcv(
                selected_pool_address,
                start_ts=start_ts,
                end_ts=before_ts,
                interval_sec=interval_sec,
                limit=max_page_limit,
                network=network,
            )
            batch = self._extract_geckoterminal_ohlcv_rows(payload)
            raw_rows.extend(batch)
            if isinstance(payload, dict):
                warning = payload.get("warning") or warning
                ohlcv_http_status = payload.get("http_status", ohlcv_http_status)
                http_status = ohlcv_http_status
                provider_error_message = payload.get("provider_error_message", provider_error_message)
                provider_error_body = payload.get("provider_error_body", provider_error_body)
                endpoint = payload.get("endpoint") or endpoint
                if warning == "provider_rate_limited" or int(ohlcv_http_status or 0) == 429:
                    terminated_on_rate_limit = True
                    rate_limit_stage = "ohlcv"
                    provider_failure_class = "rate_limited_ohlcv"
                    provider_failure_retryable = True
                    provider_failure_stage = "ohlcv"
                    self._set_gecko_cooldown(failure_class=provider_failure_class)
                    break
                if int(ohlcv_http_status or 0) == 404:
                    provider_failure_class = "ohlcv_not_available"
                    provider_failure_retryable = False
                    provider_failure_stage = "ohlcv"
                    self._remember_gecko_ohlcv_not_available(
                        network=network,
                        pool_address=selected_pool_address,
                        interval_sec=interval_sec,
                    )
                    break
            if batch:
                ohlcv_pages_succeeded += 1
            if not batch:
                break
            oldest_ts = min(_coerce_int(row[0]) or 0 for row in batch)
            if oldest_ts <= int(start_ts or oldest_ts) or len(batch) < max_page_limit or oldest_ts in seen_oldest:
                break
            seen_oldest.add(oldest_ts)
            before_ts = oldest_ts - 1
        observations, densify_metadata = self._normalize_geckoterminal_ohlcv_list(
            raw_rows,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_sec=interval_sec,
        )
        missing = not observations
        truncated = bool(end_ts is not None and observations and observations[-1]["timestamp"] < int(end_ts))
        if missing:
            warning = warning or "no_pool_ohlcv_rows"
        elif terminated_on_rate_limit or truncated:
            warning = warning or "price_path_incomplete"
        status = "missing" if missing else ("partial" if (terminated_on_rate_limit or truncated) else "complete")
        debug_fields = self._build_gecko_path_debug_fields(
            status=status,
            observations=observations,
            truncated=truncated,
            terminated_on_rate_limit=terminated_on_rate_limit,
            rate_limit_stage=rate_limit_stage,
            resolver_endpoint=pool_info.get("endpoint"),
            ohlcv_endpoint=ohlcv_endpoint,
            pool_resolution_http_status=pool_resolution_http_status,
            ohlcv_http_status=ohlcv_http_status,
        )
        request_summary.update(
            {
                "rate_limit_stage": rate_limit_stage,
                "rate_limit_endpoint": debug_fields.get("rate_limit_endpoint"),
                "rate_limit_http_status": debug_fields.get("rate_limit_http_status"),
                "partial_but_usable_row": debug_fields.get("partial_but_usable_row"),
                "replay_usable_price_path": debug_fields.get("replay_usable_price_path"),
                "replay_data_hint": debug_fields.get("replay_data_hint"),
                "collection_termination_reason": debug_fields.get("collection_termination_reason"),
            }
        )
        if provider_failure_class is None:
            provider_failure_class, provider_failure_retryable = self._classify_provider_failure(
                stage="ohlcv",
                http_status=ohlcv_http_status,
                warning=warning,
            )
            provider_failure_stage = "ohlcv" if provider_failure_class else None
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
            "obs_len": len(observations),
            "gap_fill_applied": bool(densify_metadata.get("gap_fill_applied")),
            "gap_fill_count": int(densify_metadata.get("gap_fill_count") or 0),
            "observed_row_count": int(densify_metadata.get("observed_row_count") or 0),
            "densified_row_count": int(densify_metadata.get("densified_row_count") or 0),
            "price_path_origin": densify_metadata.get("price_path_origin") or "provider_observed",
            "price_path": observations,
            "truncated": truncated,
            "missing": missing,
            "price_path_status": status,
            "warning": warning,
            "endpoint": endpoint,
            "http_status": http_status,
            "pool_resolution_http_status": pool_resolution_http_status,
            "ohlcv_http_status": ohlcv_http_status,
            "provider_error_message": provider_error_message,
            "provider_error_body": provider_error_body,
            "provider_error_payload": None,
            "terminated_on_rate_limit": terminated_on_rate_limit,
            "rate_limit_stage": rate_limit_stage,
            "ohlcv_pages_attempted": ohlcv_pages_attempted,
            "ohlcv_pages_succeeded": ohlcv_pages_succeeded,
            "provider_failure_class": provider_failure_class,
            "provider_failure_retryable": provider_failure_retryable,
            "provider_failure_stage": provider_failure_stage,
            "cooldown_applied": cooldown_applied,
            "cooldown_reason": cooldown_reason,
            "negative_cache_hit": negative_cache_hit,
            **debug_fields,
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
            result = self._fetch_geckoterminal_price_path(
                token_address=token_address,
                pair_address=pair_address,
                start_ts=start_ts,
                end_ts=end_ts,
                interval_sec=interval_sec,
                limit=limit,
                provider_config=provider_config,
            )
            return self._apply_transport_decision(result)

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
            return self._apply_transport_decision({
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
            })

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
        return self._apply_transport_decision({
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
        })
