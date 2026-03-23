from __future__ import annotations

from collectors.price_history_client import PriceHistoryClient, resolve_price_history_provider, validate_price_history_provider_config


class PayloadClient(PriceHistoryClient):
    def __init__(self, payload):
        super().__init__(base_url="https://example.test", provider="fake")
        self.payload = payload

    def _get(self, endpoint, params, headers=None):
        return self.payload


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
