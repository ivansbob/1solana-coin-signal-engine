from __future__ import annotations

from src.replay import chain_backfill


class FakeRpcClient:
    def __init__(self, *args, **kwargs):
        pass

    def get_signatures_for_address(self, address, limit=40):
        return [{"signature": "sig-1"}]

    def _rpc(self, method, params):
        if method == "getTransaction":
            return {"slot": 101}
        if method == "getBlockTime":
            return 1000
        return None


class FakePriceHistoryClient:
    def __init__(self, *args, **kwargs):
        pass

    def fetch_price_path(self, **kwargs):
        return {
            "token_address": kwargs["token_address"],
            "pair_address": kwargs.get("pair_address"),
            "source_provider": "fake",
            "requested_start_ts": kwargs.get("start_ts"),
            "requested_end_ts": kwargs.get("end_ts"),
            "interval_sec": kwargs.get("interval_sec"),
            "price_path": [
                {"timestamp": 1000, "offset_sec": 0, "price": 1.0},
                {"timestamp": 1060, "offset_sec": 60, "price": 1.1},
            ],
            "truncated": False,
            "missing": False,
            "price_path_status": "complete",
            "warning": None,
        }


class FakePartialPriceHistoryClient:
    def __init__(self, *args, **kwargs):
        pass

    def fetch_price_path(self, **kwargs):
        return {
            "token_address": kwargs["token_address"],
            "pair_address": kwargs.get("pair_address"),
            "source_provider": "fake",
            "requested_start_ts": kwargs.get("start_ts"),
            "requested_end_ts": kwargs.get("end_ts"),
            "interval_sec": kwargs.get("interval_sec"),
            "price_path": [{"timestamp": 1000, "offset_sec": 0, "price": 1.0}],
            "truncated": True,
            "missing": False,
            "price_path_status": "partial",
            "warning": "price_path_incomplete",
        }


class StagedPriceHistoryClient:
    def __init__(self, *args, **kwargs):
        self.calls = []

    def fetch_price_path(self, **kwargs):
        self.calls.append(kwargs)
        pair_address = kwargs.get("pair_address")
        interval_sec = kwargs.get("interval_sec")
        start_ts = kwargs.get("start_ts")
        end_ts = kwargs.get("end_ts")
        window_sec = (end_ts or 0) - (start_ts or 0)
        points = []
        status = "missing"
        missing = True
        truncated = False
        warning = "no_ohlcv_rows"

        if window_sec >= 480 and interval_sec == 15 and pair_address:
            points = [
                {"timestamp": start_ts, "offset_sec": 0, "price": 1.0},
                {"timestamp": start_ts + 60, "offset_sec": 60, "price": 1.1},
            ]
            status = "complete"
            missing = False
            warning = None
        elif interval_sec == 60 and pair_address:
            points = [
                {"timestamp": start_ts, "offset_sec": 0, "price": 1.0},
                {"timestamp": start_ts + 60, "offset_sec": 60, "price": 1.1},
            ]
            status = "partial"
            missing = False
            truncated = True
            warning = "price_path_incomplete"
        elif pair_address is None:
            points = [
                {"timestamp": start_ts, "offset_sec": 0, "price": 2.0},
                {"timestamp": start_ts + 60, "offset_sec": 60, "price": 2.2},
            ]
            status = "complete"
            missing = False
            warning = None
        elif start_ts == 700:
            points = [
                {"timestamp": start_ts, "offset_sec": 0, "price": 0.9},
                {"timestamp": start_ts + 60, "offset_sec": 60, "price": 1.0},
            ]
            status = "partial"
            missing = False
            truncated = True
            warning = "price_path_incomplete"

        return {
            "token_address": kwargs["token_address"],
            "pair_address": pair_address,
            "source_provider": "fake",
            "requested_start_ts": start_ts,
            "requested_end_ts": end_ts,
            "interval_sec": interval_sec,
            "price_path": points,
            "truncated": truncated,
            "missing": missing,
            "price_path_status": status,
            "warning": warning,
        }


def _base_config(**backfill_overrides):
    backfill = {
        "cache_enabled": False,
        "max_signatures_per_address": 5,
        "price_path_window_sec": 120,
        "price_path_window_max_sec": 600,
        "price_interval_sec": 15,
        "price_interval_fallbacks": [60, 300],
        "price_path_window_fallback_multipliers": [4],
        "price_path_prelaunch_buffer_sec": 300,
        "price_path_try_pairless": True,
        "price_path_min_points": 2,
        "price_path_retry_attempts": 12,
    }
    backfill.update(backfill_overrides)
    return {"backfill": backfill}


def test_build_chain_context_embeds_replay_usable_price_paths(monkeypatch):
    monkeypatch.setattr(chain_backfill, "SolanaRpcClient", FakeRpcClient)
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    rows = chain_backfill.build_chain_context(
        [{"token_address": "tok", "pair_address": "pair", "pair_created_at_ts": 1000}],
        _base_config(price_path_window_sec=120, price_interval_sec=60, price_interval_fallbacks=[]),
        dry_run=False,
    )

    assert len(rows) == 1
    price_path = rows[0]["price_paths"][0]
    assert price_path["price_path"][1]["offset_sec"] == 60
    assert price_path["truncated"] is False


def test_build_chain_context_preserves_partial_price_history_status(monkeypatch):
    monkeypatch.setattr(chain_backfill, "SolanaRpcClient", FakeRpcClient)
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePartialPriceHistoryClient)
    rows = chain_backfill.build_chain_context(
        [{"token_address": "tok_partial", "pair_address": "pair_partial", "pair_created_at_ts": 1000}],
        _base_config(price_path_window_sec=120, price_interval_sec=60, price_interval_fallbacks=[]),
        dry_run=False,
    )

    price_path = rows[0]["price_paths"][0]
    assert price_path["truncated"] is True
    assert price_path["price_path_status"] == "partial"


def test_collect_price_paths_falls_back_to_wider_window_when_primary_empty(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", StagedPriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "pair_created_at_ts": 1000},
        {},
        _base_config(),
    )[0]

    assert result["missing"] is False
    assert result["resolved_via_fallback"] is True
    assert result["attempt_count"] >= 2
    assert result["fallback_mode"] == "wider_window_x4"


def test_collect_price_paths_falls_back_to_coarser_interval_when_fine_interval_empty(monkeypatch):
    class CoarseOnlyClient(StagedPriceHistoryClient):
        def fetch_price_path(self, **kwargs):
            result = super().fetch_price_path(**kwargs)
            if kwargs.get("interval_sec") == 15:
                result.update({"price_path": [], "missing": True, "price_path_status": "missing", "warning": "no_ohlcv_rows", "truncated": False})
            return result

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", CoarseOnlyClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "pair_created_at_ts": 1000},
        {},
        _base_config(price_path_window_fallback_multipliers=[]),
    )[0]

    assert result["interval_sec"] == 60
    assert result["price_path_status"] in {"complete", "partial"}


def test_collect_price_paths_can_retry_without_pair_address(monkeypatch):
    class PairlessWinsClient(StagedPriceHistoryClient):
        def fetch_price_path(self, **kwargs):
            result = super().fetch_price_path(**kwargs)
            if kwargs.get("pair_address"):
                result.update({"price_path": [], "missing": True, "price_path_status": "missing", "warning": "no_ohlcv_rows", "truncated": False})
            return result

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", PairlessWinsClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "pair_created_at_ts": 1000},
        {},
        _base_config(price_path_window_fallback_multipliers=[]),
    )[0]

    assert result["pair_address"] is None
    assert result["resolved_via_fallback"] is True
    assert len(result["price_path"]) >= 2


def test_collect_price_paths_preserves_best_partial_result(monkeypatch):
    class PartialChooserClient(StagedPriceHistoryClient):
        def fetch_price_path(self, **kwargs):
            start_ts = kwargs.get("start_ts")
            interval_sec = kwargs.get("interval_sec")
            points = [{"timestamp": start_ts, "offset_sec": 0, "price": 1.0}]
            if interval_sec == 60:
                points = [
                    {"timestamp": start_ts + i * 60, "offset_sec": i * 60, "price": 1.0 + i * 0.1}
                    for i in range(5)
                ]
            return {
                "token_address": kwargs["token_address"],
                "pair_address": kwargs.get("pair_address"),
                "source_provider": "fake",
                "requested_start_ts": start_ts,
                "requested_end_ts": kwargs.get("end_ts"),
                "interval_sec": interval_sec,
                "price_path": points,
                "truncated": True,
                "missing": False,
                "price_path_status": "partial",
                "warning": "price_path_incomplete",
            }

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", PartialChooserClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "pair_created_at_ts": 1000},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[60]),
    )[0]

    assert len(result["price_path"]) == 5
    assert result["interval_sec"] == 60


def test_collect_price_paths_emits_diagnostic_missing_row_after_all_attempts_fail(monkeypatch):
    class EmptyClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_price_path(self, **kwargs):
            return {
                "token_address": kwargs["token_address"],
                "pair_address": kwargs.get("pair_address"),
                "source_provider": "fake",
                "requested_start_ts": kwargs.get("start_ts"),
                "requested_end_ts": kwargs.get("end_ts"),
                "interval_sec": kwargs.get("interval_sec"),
                "price_path": [],
                "truncated": False,
                "missing": True,
                "price_path_status": "missing",
                "warning": "no_ohlcv_rows",
            }

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", EmptyClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "pair_created_at_ts": 1000},
        {},
        _base_config(price_path_retry_attempts=4),
    )[0]

    assert result["price_path"] == []
    assert result["price_path_status"] == "missing"
    assert result["attempt_count"] == 4
    assert len(result["attempts"]) == 4
    assert result["warning"] == "no_ohlcv_rows"
