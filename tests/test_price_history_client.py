from __future__ import annotations

from collectors.price_history_client import PriceHistoryClient


class PayloadClient(PriceHistoryClient):
    def __init__(self, payload):
        super().__init__(base_url="https://example.test", provider="fake")
        self.payload = payload

    def _get(self, endpoint, params):
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
