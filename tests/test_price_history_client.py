from __future__ import annotations

import time

from collectors.price_history_client import PriceHistoryClient, resolve_price_history_provider, validate_price_history_provider_config


class PayloadClient(PriceHistoryClient):
    def __init__(self, payload):
        super().__init__(base_url="https://example.test", provider="fake")
        self.payload = payload

    def _get(self, endpoint, params, headers=None):
        return self.payload


class GeckoTerminalClient(PriceHistoryClient):
    def __init__(self, *, resolver_result=None, ohlcv_payloads=None, get_payloads=None, **kwargs):
        super().__init__(
            base_url="https://api.geckoterminal.com/api/v2",
            provider="geckoterminal_pool_ohlcv",
            token_endpoint="networks/{network}/tokens/{token_address}/pools",
            pair_endpoint="networks/{network}/pools/{pool_address}/ohlcv/{timeframe}",
            chain="solana",
            include_empty_intervals=True,
            request_version="20230302",
            currency="usd",
            pool_resolver="geckoterminal",
            max_ohlcv_limit=kwargs.pop("max_ohlcv_limit", 1000),
            **kwargs,
        )
        self.resolver_result = resolver_result
        self.ohlcv_payloads = list(ohlcv_payloads or [])
        self.get_payloads = list(get_payloads or [])
        self.fetch_calls = []
        self.resolve_calls = []
        self.get_calls = []

    def _get(self, endpoint, params, headers=None):
        self.get_calls.append({"endpoint": endpoint, "params": dict(params), "headers": dict(headers or {})})
        if self.get_payloads:
            return self.get_payloads.pop(0)
        return {"data": []}

    def _resolve_geckoterminal_pool(self, token_address: str, network: str = "solana"):
        self.resolve_calls.append((token_address, network))
        if self.resolver_result is not None:
            return dict(self.resolver_result)
        return super()._resolve_geckoterminal_pool(token_address, network)

    def _fetch_geckoterminal_pool_ohlcv(self, pool_address: str, **kwargs):
        call = {"pool_address": pool_address, **kwargs}
        self.fetch_calls.append(call)
        if self.ohlcv_payloads:
            return self.ohlcv_payloads.pop(0)
        return {"data": {"attributes": {"ohlcv_list": []}}}


def test_fetch_price_path_marks_unparseable_rows_distinct_from_no_rows():
    client = PayloadClient({"rows": [{"timestamp": None, "price": "nanx"}], "warning": None})
    result = client.fetch_price_path(token_address="tok", pair_address="pair", start_ts=1000, end_ts=1120)

    assert result["warning"] == "price_rows_unparseable"
    assert result["provider_row_count"] == 1
    assert result["missing"] is True


def test_fetch_price_path_keeps_provider_warning_on_empty_payload():
    client = PayloadClient({"rows": [], "warning": "provider_empty_payload"})
    result = client.fetch_price_path(token_address="tok", pair_address="pair", start_ts=1000, end_ts=1120)

    assert result["warning"] == "provider_empty_payload"
    assert result["price_path_status"] == "missing"


def test_fetch_price_path_sets_partial_when_last_timestamp_before_end_ts():
    client = PayloadClient({"rows": [{"timestamp": 1000, "price": 1.0}, {"timestamp": 1060, "price": 1.1}]})
    result = client.fetch_price_path(token_address="tok", pair_address="pair", start_ts=1000, end_ts=1200)

    assert result["truncated"] is True
    assert result["price_path_status"] == "partial"


def test_price_history_client_uses_configured_default_provider():
    config = {
        "backfill": {"price_history_provider": "birdeye_ohlcv_v3"},
        "providers": {"price_history": {"base_url": "https://public-api.birdeye.so"}},
    }
    bootstrap = validate_price_history_provider_config(config)

    assert bootstrap["provider_bootstrap_ok"] is True
    assert bootstrap["warning"] != "price_history_provider_unconfigured"
    assert bootstrap["price_history_provider"] == "birdeye_ohlcv_v3"


def test_price_history_client_marks_unconfigured_when_no_provider_key_present():
    bootstrap = validate_price_history_provider_config({"backfill": {}})

    assert bootstrap["warning"] == "price_history_provider_unconfigured"
    assert bootstrap["provider_bootstrap_ok"] is False


def test_price_history_client_marks_invalid_provider_name():
    bootstrap = validate_price_history_provider_config({"backfill": {"price_history_provider": "mystery_feed"}})

    assert bootstrap["warning"] == "price_history_provider_invalid"
    assert bootstrap["price_history_provider_status"] == "invalid"


def test_price_history_client_marks_disabled_provider_distinctly():
    bootstrap = validate_price_history_provider_config({"backfill": {"price_history_provider": "disabled"}})

    assert bootstrap["warning"] == "price_history_provider_disabled"
    assert bootstrap["price_history_provider_status"] == "disabled"


def test_price_history_client_normalizes_provider_aliases():
    resolved = resolve_price_history_provider({"backfill": {"price_history_provider": "birdeye_v3"}})

    assert resolved["price_history_provider"] == "birdeye_ohlcv_v3"
    assert resolved["provider_config_source"] == "backfill.price_history_provider"


def test_price_history_client_preserves_real_empty_payload_warning_when_provider_is_configured():
    client = PayloadClient({"rows": [], "warning": "provider_empty_payload"})
    client.provider_status = "configured"
    client.provider_config_source = "backfill.price_history_provider"
    result = client.fetch_price_path(token_address="tok", pair_address="pair", start_ts=1000, end_ts=1120)

    assert result["warning"] == "provider_empty_payload"
    assert result["price_history_provider_status"] == "configured"


def test_price_history_client_normalizes_geckoterminal_provider_aliases():
    resolved = resolve_price_history_provider({"backfill": {"price_history_provider": "geckoterminal_pool"}})

    assert resolved["price_history_provider"] == "geckoterminal_pool_ohlcv"


def test_validate_price_history_provider_config_accepts_geckoterminal_defaults():
    config = {
        "backfill": {"price_history_provider": "geckoterminal_pool_ohlcv"},
        "providers": {
            "price_history": {
                "provider": "geckoterminal_pool_ohlcv",
                "base_url": "https://api.geckoterminal.com/api/v2",
                "chain": "solana",
                "currency": "usd",
                "include_empty_intervals": True,
                "pool_resolver": "geckoterminal",
                "resolver_cache_ttl_sec": 86400,
                "max_ohlcv_limit": 1000,
                "request_version": "20230302",
                "allow_pairless_token_lookup": True,
                "require_pair_address": False,
            }
        },
    }

    bootstrap = validate_price_history_provider_config(config)

    assert bootstrap["provider_bootstrap_ok"] is True
    assert bootstrap["price_history_provider"] == "geckoterminal_pool_ohlcv"
    assert bootstrap["request_version"] == "20230302"
    assert bootstrap["provider_request_summary"]["include_empty_intervals"] is True
    assert bootstrap["provider_request_summary"]["max_ohlcv_limit"] == 1000


def test_fetch_price_path_resolves_pool_before_fetching_geckoterminal_ohlcv():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-1",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 3,
            "pool_resolution_status": "resolved",
        },
        ohlcv_payloads=[{"data": {"attributes": {"ohlcv_list": [[1000, 1.0, 1.0, 1.0, 1.0, 10.0]]}}}],
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1000, interval_sec=60, limit=1000)

    assert client.resolve_calls == [("tok", "solana")]
    assert client.fetch_calls[0]["pool_address"] == "pool-1"
    assert result["selected_pool_address"] == "pool-1"
    assert result["pool_resolution_status"] == "resolved"


def test_fetch_price_path_normalizes_geckoterminal_ohlcv_list():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-1",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 1,
            "pool_resolution_status": "resolved",
        },
        ohlcv_payloads=[
            {"data": {"attributes": {"ohlcv_list": [[1060, 1, 2, 0.5, 1.5, 11], [1000, 1, 2, 0.5, 1.2, 9]]}}}
        ],
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1060, interval_sec=60, limit=1000)

    assert result["price_path"] == [
        {"timestamp": 1000, "offset_sec": 0, "price": 1.2, "volume": 9.0},
        {"timestamp": 1060, "offset_sec": 60, "price": 1.5, "volume": 11.0},
    ]
    assert result["provider_row_count"] == 2


def test_fetch_price_path_marks_missing_when_pool_resolution_returns_no_candidates():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": None,
            "resolver_source": "geckoterminal",
            "resolver_confidence": "none",
            "pool_candidates_seen": 0,
            "pool_resolution_status": "pool_resolution_failed",
            "warning": "pool_resolution_failed",
        }
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1060)

    assert result["missing"] is True
    assert result["warning"] == "pool_resolution_failed"
    assert result["provider_row_count"] == 0


def test_fetch_price_path_keeps_pool_resolution_provenance_fields():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-1",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 7,
            "pool_resolution_status": "resolved",
        },
        ohlcv_payloads=[{"data": {"attributes": {"ohlcv_list": [[1000, 1, 1, 1, 1.0, 10]]}}}],
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1000)

    assert result["pool_resolver_source"] == "geckoterminal"
    assert result["pool_resolver_confidence"] == "high"
    assert result["pool_candidates_seen"] == 7
    assert result["provider_request_summary"]["selected_pool_address"] == "pool-1"


def test_fetch_price_path_uses_include_empty_intervals_for_minute_series():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-1",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 1,
            "pool_resolution_status": "resolved",
        },
        ohlcv_payloads=[{"data": {"attributes": {"ohlcv_list": [[1000, 1, 1, 1, 1.0, 10]]}}}],
    )

    client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1000, interval_sec=60, limit=1000)

    assert client.fetch_calls[0]["interval_sec"] == 60
    assert client.fetch_calls[0]["limit"] == 1000


def test_fetch_price_path_paginates_backwards_with_before_timestamp_when_range_exceeds_limit():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-1",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 1,
            "pool_resolution_status": "resolved",
        },
        ohlcv_payloads=[
            {"data": {"attributes": {"ohlcv_list": [[1120, 1, 1, 1, 1.3, 10], [1060, 1, 1, 1, 1.2, 10]]}}},
            {"data": {"attributes": {"ohlcv_list": [[1000, 1, 1, 1, 1.1, 10]]}}},
        ],
        max_ohlcv_limit=2,
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1120, interval_sec=60, limit=2)

    assert len(client.fetch_calls) == 2
    assert client.fetch_calls[1]["end_ts"] == 1059
    assert [point["timestamp"] for point in result["price_path"]] == [1000, 1060, 1120]


def test_gecko_stop_paging_after_429():
    client = GeckoTerminalClient(
        resolver_result={"pool_address": "pool-1", "resolver_source": "geckoterminal", "resolver_confidence": "high", "pool_candidates_seen": 1, "pool_resolution_status": "resolved"},
        ohlcv_payloads=[
            {"data": {"attributes": {"ohlcv_list": [[1120, 1, 1, 1, 1.3, 10], [1060, 1, 1, 1, 1.2, 10]]}}, "http_status": 200},
            {"warning": "provider_rate_limited", "http_status": 429, "provider_error_message": "rate limit"},
            {"data": {"attributes": {"ohlcv_list": [[1000, 1, 1, 1, 1.1, 10]]}}, "http_status": 200},
        ],
        max_ohlcv_limit=2,
        gecko_max_pages_per_token=10,
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1120, interval_sec=60, limit=2)

    assert len(client.fetch_calls) == 2
    assert result["terminated_on_rate_limit"] is True
    assert result["rate_limit_stage"] == "ohlcv"
    assert result["ohlcv_pages_attempted"] == 2
    assert result["ohlcv_pages_succeeded"] == 1


def test_normalize_gecko_sparse_internal_gap_applies_gap_fill():
    client = GeckoTerminalClient()
    rows = [[100, 1, 1, 1, 1.0, 5], [280, 1, 1, 1, 1.2, 7]]

    normalized, meta = client._normalize_geckoterminal_ohlcv_list(rows, start_ts=100, end_ts=280, interval_sec=60)

    assert [row["timestamp"] for row in normalized] == [100, 160, 220, 280]
    assert normalized[1]["price"] == 1.0
    assert normalized[1]["volume"] == 0.0
    assert meta["gap_fill_applied"] is True
    assert meta["gap_fill_count"] == 2


def test_normalize_gecko_does_not_fill_before_first_observed_row():
    client = GeckoTerminalClient()
    rows = [[280, 1, 1, 1, 1.2, 7], [340, 1, 1, 1, 1.3, 8]]

    normalized, _ = client._normalize_geckoterminal_ohlcv_list(rows, start_ts=100, end_ts=340, interval_sec=60)

    assert [row["timestamp"] for row in normalized] == [280, 340]


def test_normalize_gecko_does_not_fill_after_last_observed_row():
    client = GeckoTerminalClient()
    rows = [[100, 1, 1, 1, 1.0, 7]]

    normalized, meta = client._normalize_geckoterminal_ohlcv_list(rows, start_ts=100, end_ts=340, interval_sec=60)

    assert [row["timestamp"] for row in normalized] == [100]
    assert meta["gap_fill_applied"] is False
    assert meta["gap_fill_count"] == 0


def test_fetch_gecko_price_path_reports_gap_fill_metadata():
    client = GeckoTerminalClient(
        resolver_result={"pool_address": "pool-1", "resolver_source": "geckoterminal", "resolver_confidence": "high", "pool_candidates_seen": 1, "pool_resolution_status": "resolved"},
        ohlcv_payloads=[{"data": {"attributes": {"ohlcv_list": [[100, 1, 1, 1, 1.0, 1], [280, 1, 1, 1, 1.2, 2]]}}}],
    )
    result = client.fetch_price_path(token_address="tok", start_ts=100, end_ts=280, interval_sec=60)

    assert result["gap_fill_applied"] is True
    assert result["gap_fill_count"] == 2
    assert result["observed_row_count"] == 2
    assert result["densified_row_count"] == 4
    assert result["price_path_origin"] == "provider_observed_plus_gap_fill"


def test_fetch_gecko_price_path_keeps_partial_status_when_nonempty():
    client = GeckoTerminalClient(
        resolver_result={"pool_address": "pool-1", "resolver_source": "geckoterminal", "resolver_confidence": "high", "pool_candidates_seen": 1, "pool_resolution_status": "resolved"},
        ohlcv_payloads=[{"data": {"attributes": {"ohlcv_list": [[100, 1, 1, 1, 1.0, 1]]}}}],
    )
    result = client.fetch_price_path(token_address="tok", start_ts=100, end_ts=280, interval_sec=60)

    assert result["price_path_status"] == "partial"
    assert result["missing"] is False
    assert result["obs_len"] == 1


def test_resolve_geckoterminal_pool_stops_after_provider_rate_limit(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr(time, "sleep", lambda seconds: sleep_calls.append(seconds))
    client = GeckoTerminalClient(
        get_payloads=[
            {"warning": "provider_rate_limited", "http_status": 429, "provider_error_message": "rate limit"},
            {"data": [{"id": "pool-1", "attributes": {"reserve_in_usd": 10}}], "http_status": 200},
        ]
    )

    resolved = client._resolve_geckoterminal_pool("tok", network="solana")

    assert resolved["pool_address"] is None
    assert resolved["endpoint"] == "networks/solana/tokens/tok/pools"
    assert len(client.get_calls) == 1
    assert sleep_calls == []


def test_gecko_partial_marks_terminated_on_rate_limit(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)
    client = GeckoTerminalClient(
        resolver_result={"pool_address": "pool-1", "resolver_source": "geckoterminal", "resolver_confidence": "high", "pool_candidates_seen": 1, "pool_resolution_status": "resolved"},
        ohlcv_payloads=[
            {"data": {"attributes": {"ohlcv_list": [[1000, 1, 1, 1, 1.0, 10]]}}, "http_status": 200},
            {"warning": "provider_rate_limited", "http_status": 429, "provider_error_message": "rate limit"},
        ],
        max_ohlcv_limit=1,
    )

    result = client.fetch_price_path(token_address="tok", start_ts=940, end_ts=1120, interval_sec=60, limit=1)

    assert result["missing"] is False
    assert result["price_path_status"] == "partial"
    assert len(client.fetch_calls) == 2
    assert result["terminated_on_rate_limit"] is True
    assert result["rate_limit_stage"] == "ohlcv"
    assert result["ohlcv_http_status"] == 429


def test_gecko_missing_when_first_ohlcv_request_is_429(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)
    client = GeckoTerminalClient(
        resolver_result={"pool_address": "pool-1", "resolver_source": "geckoterminal", "resolver_confidence": "high", "pool_candidates_seen": 1, "pool_resolution_status": "resolved"},
        ohlcv_payloads=[
            {"warning": "provider_rate_limited", "http_status": 429, "provider_error_message": "rate limit"},
        ],
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1120, interval_sec=60, limit=2)

    assert result["price_path_status"] == "missing"
    assert result["missing"] is True
    assert result["terminated_on_rate_limit"] is True
    assert result["rate_limit_stage"] == "ohlcv"
    assert result["ohlcv_pages_attempted"] == 1
    assert result["ohlcv_pages_succeeded"] == 0


def test_gecko_pool_resolution_http_status_and_ohlcv_http_status_are_separate():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-1",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 1,
            "pool_resolution_status": "resolved",
            "http_status": 200,
        },
        ohlcv_payloads=[
            {"warning": "provider_rate_limited", "http_status": 429, "provider_error_message": "rate limit"},
        ],
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1120, interval_sec=60, limit=2)

    assert result["pool_resolution_http_status"] == 200
    assert result["ohlcv_http_status"] == 429


def test_fetch_gecko_price_path_preserves_resolver_observability_when_pool_missing():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": None,
            "resolver_source": "geckoterminal",
            "resolver_confidence": "none",
            "pool_candidates_seen": 0,
            "pool_resolution_status": "pool_resolution_failed",
            "warning": "provider_rate_limited",
            "endpoint": "networks/solana/tokens/tok/pools",
            "http_status": 429,
            "provider_error_message": "rate limit",
            "provider_error_body": "{\"error\":\"limit\"}",
        }
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1060)

    assert result["endpoint"] == "networks/solana/tokens/tok/pools"
    assert result["http_status"] == 429
    assert result["pool_resolution_http_status"] == 429
    assert result["ohlcv_http_status"] is None
    assert result["provider_error_message"] == "rate limit"
    assert result["provider_error_body"] == "{\"error\":\"limit\"}"
    assert result["provider_request_summary"]["endpoint"] == "networks/solana/tokens/tok/pools"


def test_gecko_ohlcv_404_is_classified_non_retryable_and_not_cached_on_first_hit():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": "pool-404",
            "resolver_source": "geckoterminal",
            "resolver_confidence": "high",
            "pool_candidates_seen": 1,
            "pool_resolution_status": "resolved",
        },
        ohlcv_payloads=[{"warning": "provider_http_error", "http_status": 404}],
    )

    result = client.fetch_price_path(token_address="tok", start_ts=1000, end_ts=1060, interval_sec=60, limit=2)

    assert result["provider_failure_class"] == "ohlcv_not_available"
    assert result["provider_failure_retryable"] is False
    assert result["negative_cache_hit"] is False
    assert result["cooldown_applied"] is False


def test_gecko_subsequent_404_uses_negative_cache_without_http_request():
    client = GeckoTerminalClient(
        ohlcv_payloads=[{"warning": "provider_http_error", "http_status": 404}],
        gecko_ohlcv_404_negative_ttl_sec=1800,
    )

    first = client.fetch_price_path(
        token_address="tok",
        pair_address="pool-404",
        start_ts=1000,
        end_ts=1060,
        interval_sec=60,
        limit=2,
    )
    second = client.fetch_price_path(
        token_address="tok",
        pair_address="pool-404",
        start_ts=1000,
        end_ts=1060,
        interval_sec=60,
        limit=2,
    )

    assert first["provider_failure_class"] == "ohlcv_not_available"
    assert first["negative_cache_hit"] is False
    assert len(client.fetch_calls) == 1
    assert second["negative_cache_hit"] is True
    assert second["provider_failure_class"] == "ohlcv_not_available"
    assert len(client.fetch_calls) == 1


def test_gecko_resolver_429_sets_cooldown_and_next_call_skips_http():
    client = GeckoTerminalClient(
        resolver_result={
            "pool_address": None,
            "resolver_source": "geckoterminal",
            "resolver_confidence": "none",
            "pool_candidates_seen": 0,
            "pool_resolution_status": "pool_resolution_failed",
            "warning": "provider_rate_limited",
            "http_status": 429,
        },
        gecko_rate_limit_cooldown_sec=120,
    )

    first = client.fetch_price_path(token_address="tok-a", start_ts=1000, end_ts=1060, interval_sec=60, limit=2)
    second = client.fetch_price_path(token_address="tok-b", start_ts=1000, end_ts=1060, interval_sec=60, limit=2)

    assert first["provider_failure_class"] == "rate_limited_resolver"
    assert first["provider_failure_retryable"] is True
    assert second["cooldown_applied"] is True
    assert second["provider_failure_class"] in {"rate_limited_resolver", "provider_rate_limited_recently"}
    assert client.resolve_calls == [("tok-a", "solana")]
