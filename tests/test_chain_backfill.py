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


class SignatureTimeOnlyRpcClient:
    def __init__(self, *args, **kwargs):
        pass

    def get_signatures_for_address(self, address, limit=40):
        return [{"signature": "sig-1", "blockTime": 1710000000}]

    def _rpc(self, method, params):
        if method == "getTransaction":
            return None
        if method == "getBlockTime":
            return None
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
        "price_history_provider": "birdeye_ohlcv_v3",
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
    return {
        "backfill": backfill,
        "providers": {
            "price_history": {
                "provider": backfill.get("price_history_provider"),
                "base_url": "https://public-api.birdeye.so",
                "token_endpoint": "defi/v3/ohlcv",
                "pair_endpoint": "defi/v3/ohlcv/pair",
                "allow_pairless_token_lookup": True,
                "require_pair_address": False,
            }
        },
    }


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


def test_collect_price_paths_derives_start_ts_from_entry_time(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1_773_619_200
    assert result["price_path_time_source"] == "candidate_field"
    assert result["price_path_time_derived"] is True
    assert result["price_path_anchor_field"] == "entry_time"


def test_collect_price_paths_derives_start_ts_from_first_seen_at(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "first_seen_at": "2026-03-15T23:55:00Z"},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1_773_618_900
    assert result["price_path_anchor_field"] == "first_seen_at"


def test_build_chain_context_derives_start_ts_from_signature_block_time(monkeypatch):
    monkeypatch.setattr(chain_backfill, "SolanaRpcClient", SignatureTimeOnlyRpcClient)
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    rows = chain_backfill.build_chain_context(
        [{"token_address": "tok_sig_only"}],
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
        dry_run=False,
    )

    price_path = rows[0]["price_paths"][0]
    assert price_path["requested_start_ts"] == 1_710_000_000
    assert price_path["price_path_time_source"] == "signature_block_time"
    assert price_path["price_path_time_derived"] is True
    assert price_path["price_path_anchor_field"] == "signatures[].blockTime"
    assert price_path["attempt_count"] >= 1


def test_collect_price_paths_emits_explained_missing_when_no_timestamp_sources_exist(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair"},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["price_path"] == []
    assert result["price_path_status"] == "missing"
    assert result["warning"] == "price_path_start_ts_missing"
    assert result["attempt_count"] == 0
    assert "entry_time" in result["missing_required_fields"]
    assert "first_seen_at" in result["missing_required_fields"]


def test_collect_price_paths_runs_attempts_after_derived_start_ts(monkeypatch):
    class TrackingClient(StagedPriceHistoryClient):
        seen_start_ts = []

        def fetch_price_path(self, **kwargs):
            self.__class__.seen_start_ts.append(kwargs.get("start_ts"))
            return super().fetch_price_path(**kwargs)

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", TrackingClient)
    TrackingClient.seen_start_ts = []
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(),
    )[0]

    assert result["attempt_count"] >= 1
    assert TrackingClient.seen_start_ts
    assert TrackingClient.seen_start_ts[0] == 1_773_619_200


class HydrationTrackingRpcClient:
    def __init__(self, *args, **kwargs):
        self.calls = []

    def get_signatures_for_address(self, address, limit=40):
        self.calls.append(("get_signatures_for_address", address, limit))
        return []

    def _rpc(self, method, params):
        self.calls.append((method, params))
        if method == "getTransaction":
            return {"slot": 101}
        if method == "getBlockTime":
            return 1710000000
        return None


def test_collect_price_paths_uses_replay_entry_time_from_upstream_context(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "replay_entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1_773_619_200
    assert result["price_path_anchor_field"] == "replay_entry_time"
    assert result["price_path_time_source"] == "candidate_field"

def test_chain_backfill_prefers_replay_entry_time_over_signature_block_time(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "replay_entry_time": "2026-03-16T00:00:00Z",
            "signatures": [{"signature": "sig-1", "blockTime": 1710000000}],
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1_773_619_200
    assert result["price_path_anchor_field"] == "replay_entry_time"
    assert result["time_anchor_preference_applied"] is True
    assert result["time_anchor_discarded_candidates"][0]["field"] == "signatures[].blockTime"



def test_chain_backfill_prefers_entry_time_over_signature_block_time(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "entry_time": "2026-03-16T00:00:00Z",
            "signatures": [{"signature": "sig-1", "blockTime": 1710000000}],
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1_773_619_200
    assert result["price_path_anchor_field"] == "entry_time"
    assert any(item["field"] == "signatures[].blockTime" for item in result["time_anchor_discarded_candidates"])



def test_chain_backfill_records_discarded_signature_anchor_when_replay_entry_time_wins(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "replay_entry_time": "2026-03-16T00:00:00Z",
            "signatures": [{"signature": "sig-1", "blockTime": 1710000000}],
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    discarded = result["time_anchor_discarded_candidates"]
    assert discarded
    assert discarded[0]["field"] == "signatures[].blockTime"
    assert discarded[0]["reason"] == "lower_preference_than_replay_entry_time"



def test_chain_backfill_prefers_explicit_price_path_start_ts_over_all_other_sources(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "price_path_start_ts": 1700000000,
            "replay_entry_time": "2026-03-16T00:00:00Z",
            "signatures": [{"signature": "sig-1", "blockTime": 1710000000}],
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1700000000
    assert result["price_path_anchor_field"] == "price_path_start_ts"
    assert result["time_anchor_preference_applied"] is True



def test_collect_price_paths_uses_block_times_map_when_signatures_are_string_only(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "signatures": ["sig-1", "sig-2"],
            "block_times": {"sig-1": 1710000000},
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["requested_start_ts"] == 1710000000
    assert result["price_path_anchor_field"] == "block_times"
    assert result["price_path_time_source"] == "block_times"
    assert result["price_path_time_derived"] is True


def test_collect_price_paths_hydrates_signature_block_time_when_map_missing(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    rpc_client = HydrationTrackingRpcClient()
    limiter = chain_backfill.RateLimiter(1000)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "signatures": ["sig-1", "sig-2"],
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
        rpc_client=rpc_client,
        limiter=limiter,
    )[0]

    assert result["requested_start_ts"] == 1710000000
    assert result["price_path_time_source"] == "signature_block_time"
    assert result["signature_hydration_attempted"] is True
    assert result["signature_hydration_count"] == 2


def test_collect_price_paths_prefers_direct_candidate_time_over_signature_hydration(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    rpc_client = HydrationTrackingRpcClient()
    limiter = chain_backfill.RateLimiter(1000)
    result = chain_backfill._collect_price_paths(
        {
            "token_address": "tok",
            "pair_address": "pair",
            "entry_time": "2026-03-16T00:00:00Z",
            "signatures": ["sig-1", "sig-2"],
        },
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
        rpc_client=rpc_client,
        limiter=limiter,
    )[0]

    assert result["price_path_anchor_field"] == "entry_time"
    assert not any(call[0] == "getTransaction" for call in rpc_client.calls)
    assert not any(call[0] == "getBlockTime" for call in rpc_client.calls)


def test_collect_price_paths_records_explained_missing_when_no_time_anchor_exists(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    rpc_client = HydrationTrackingRpcClient()
    limiter = chain_backfill.RateLimiter(1000)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "signatures": ["sig-1"]},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[], time_anchor_use_signature_hydration=False),
        rpc_client=rpc_client,
        limiter=limiter,
    )[0]

    assert result["warning"] == "price_path_start_ts_missing"
    assert result["time_anchor_resolution_status"] == "missing"
    assert result["attempt_count"] == 0
    assert result["time_anchor_attempts"]
    assert "signatures[].blockTime" in result["missing_required_fields"]


def test_collect_price_paths_runs_price_path_attempts_after_signature_anchor_resolution(monkeypatch):
    class TrackingClient(StagedPriceHistoryClient):
        seen_start_ts = []

        def fetch_price_path(self, **kwargs):
            self.__class__.seen_start_ts.append(kwargs.get("start_ts"))
            return super().fetch_price_path(**kwargs)

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", TrackingClient)
    rpc_client = HydrationTrackingRpcClient()
    limiter = chain_backfill.RateLimiter(1000)
    TrackingClient.seen_start_ts = []
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "signatures": ["sig-1"]},
        {},
        _base_config(),
        rpc_client=rpc_client,
        limiter=limiter,
    )[0]

    assert result["attempt_count"] >= 1
    assert TrackingClient.seen_start_ts[0] == 1710000000
    assert result["price_path_time_source"] == "signature_block_time"


def test_chain_backfill_fails_fast_on_provider_bootstrap_error(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", StagedPriceHistoryClient)
    config = _base_config()
    config["backfill"].pop("price_history_provider", None)
    config["providers"] = {}

    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        config,
    )[0]

    assert result["attempt_count"] == 1
    assert result["attempt_strategy"] == "provider_bootstrap_failed"
    assert result["warning"] == "price_history_provider_unconfigured"
    assert result["provider_bootstrap_ok"] is False


def test_chain_backfill_records_provider_status_in_price_path_row(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", FakePriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert result["price_history_provider"] == "birdeye_ohlcv_v3"
    assert result["price_history_provider_status"] == "configured"
    assert result["provider_bootstrap_ok"] is True


def test_chain_backfill_runs_real_attempts_when_provider_is_configured(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", StagedPriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(),
    )[0]

    assert result["attempt_count"] >= 1
    assert result["warning"] != "price_history_provider_unconfigured"
    assert result["price_history_provider_status"] == "configured"


def test_chain_backfill_pairless_attempt_respects_provider_capabilities(monkeypatch):
    class PairlessTracker(StagedPriceHistoryClient):
        seen_pairless = False

        def fetch_price_path(self, **kwargs):
            if kwargs.get("pair_address") is None:
                self.__class__.seen_pairless = True
            result = super().fetch_price_path(**kwargs)
            if kwargs.get("pair_address"):
                result.update({"price_path": [], "missing": True, "price_path_status": "missing", "warning": "provider_pair_not_found", "truncated": False})
            return result

    PairlessTracker.seen_pairless = False
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", PairlessTracker)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_path_window_fallback_multipliers=[], price_interval_fallbacks=[]),
    )[0]

    assert PairlessTracker.seen_pairless is True
    assert result["warning"] != "price_history_provider_unconfigured"
    assert result["pair_address"] is None


def test_seed_backfill_no_longer_stops_at_provider_unconfigured(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", StagedPriceHistoryClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "pair", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(),
    )[0]

    assert result["requested_start_ts"] == 1_773_619_200
    assert result["warning"] != "price_history_provider_unconfigured"
    assert result["price_path_status"] in {"complete", "partial", "missing"}


class GeckoBackfillClient:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def fetch_price_path(self, **kwargs):
        pair_address = kwargs.get("pair_address")
        if pair_address == "seed-pool":
            return {
                "token_address": kwargs["token_address"],
                "pair_address": pair_address,
                "selected_pool_address": "provider-pool",
                "pool_address": "provider-pool",
                "pool_resolver_source": "seed_pair_address",
                "pool_resolver_confidence": "seed",
                "pool_candidates_seen": 1,
                "pool_resolution_status": "seed_pair_address",
                "source_provider": "geckoterminal_pool_ohlcv",
                "requested_start_ts": kwargs.get("start_ts"),
                "requested_end_ts": kwargs.get("end_ts"),
                "interval_sec": kwargs.get("interval_sec"),
                "price_path": [
                    {"timestamp": kwargs.get("start_ts"), "offset_sec": 0, "price": 1.0},
                    {"timestamp": kwargs.get("start_ts") + 60, "offset_sec": 60, "price": 1.1},
                ],
                "truncated": False,
                "missing": False,
                "price_path_status": "complete",
                "warning": None,
            }
        return {
            "token_address": kwargs["token_address"],
            "pair_address": pair_address,
            "selected_pool_address": "resolved-pool",
            "pool_address": "resolved-pool",
            "pool_resolver_source": "geckoterminal",
            "pool_resolver_confidence": "high",
            "pool_candidates_seen": 4,
            "pool_resolution_status": "resolved",
            "source_provider": "geckoterminal_pool_ohlcv",
            "requested_start_ts": kwargs.get("start_ts"),
            "requested_end_ts": kwargs.get("end_ts"),
            "interval_sec": kwargs.get("interval_sec"),
            "price_path": [
                {"timestamp": kwargs.get("start_ts"), "offset_sec": 0, "price": 2.0},
                {"timestamp": kwargs.get("start_ts") + 60, "offset_sec": 60, "price": 2.2},
            ],
            "truncated": False,
            "missing": False,
            "price_path_status": "complete",
            "warning": None,
        }


class GeckoMissingRowsClient:
    def __init__(self, *args, **kwargs):
        pass

    def fetch_price_path(self, **kwargs):
        return {
            "token_address": kwargs["token_address"],
            "pair_address": kwargs.get("pair_address"),
            "selected_pool_address": "resolved-pool",
            "pool_address": "resolved-pool",
            "pool_resolver_source": "geckoterminal",
            "pool_resolver_confidence": "high",
            "pool_candidates_seen": 2,
            "pool_resolution_status": "resolved",
            "source_provider": "geckoterminal_pool_ohlcv",
            "requested_start_ts": kwargs.get("start_ts"),
            "requested_end_ts": kwargs.get("end_ts"),
            "interval_sec": kwargs.get("interval_sec"),
            "price_path": [],
            "truncated": False,
            "missing": True,
            "price_path_status": "missing",
            "warning": "no_pool_ohlcv_rows",
        }


def test_collect_price_paths_embeds_selected_pool_address_from_provider_result(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", GeckoBackfillClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_history_provider="geckoterminal_pool_ohlcv", price_interval_sec=60, price_interval_fallbacks=[]),
    )[0]

    assert result["selected_pool_address"] == "resolved-pool"
    assert result["pool_resolver_source"] == "geckoterminal"


def test_collect_price_paths_preserves_pool_resolution_diagnostics_on_missing_rows(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", GeckoMissingRowsClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_history_provider="geckoterminal_pool_ohlcv", price_interval_sec=60, price_interval_fallbacks=[], price_path_retry_attempts=1),
    )[0]

    assert result["missing"] is True
    assert result["selected_pool_address"] == "resolved-pool"
    assert result["warning"] == "no_pool_ohlcv_rows"


def test_collect_price_paths_prefers_seed_pair_address_but_allows_provider_selected_pool_override(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", GeckoBackfillClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "pair_address": "seed-pool", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_history_provider="geckoterminal_pool_ohlcv", price_interval_sec=60, price_interval_fallbacks=[]),
    )[0]

    assert result["pair_address"] == "seed-pool"
    assert result["selected_pool_address"] == "provider-pool"
    assert result["pool_resolution_status"] == "seed_pair_address"


def test_collect_price_paths_works_with_geckoterminal_pool_provider_and_replay_usable_minute_rows(monkeypatch):
    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", GeckoBackfillClient)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_history_provider="geckoterminal_pool_ohlcv", price_interval_sec=60, price_interval_fallbacks=[]),
    )[0]

    assert result["price_path_status"] == "complete"
    assert len(result["price_path"]) >= 2
    assert result["interval_sec"] == 60


def test_collect_price_paths_preserves_best_partial_result_for_geckoterminal_provider(monkeypatch):
    class GeckoPartialChooser(GeckoBackfillClient):
        def fetch_price_path(self, **kwargs):
            if kwargs.get("interval_sec") == 300:
                return {
                    "token_address": kwargs["token_address"],
                    "pair_address": kwargs.get("pair_address"),
                    "selected_pool_address": "resolved-pool",
                    "pool_address": "resolved-pool",
                    "pool_resolver_source": "geckoterminal",
                    "pool_resolver_confidence": "high",
                    "pool_candidates_seen": 3,
                    "pool_resolution_status": "resolved",
                    "source_provider": "geckoterminal_pool_ohlcv",
                    "requested_start_ts": kwargs.get("start_ts"),
                    "requested_end_ts": kwargs.get("end_ts"),
                    "interval_sec": kwargs.get("interval_sec"),
                    "price_path": [
                        {"timestamp": kwargs.get("start_ts"), "offset_sec": 0, "price": 1.0},
                        {"timestamp": kwargs.get("start_ts") + 300, "offset_sec": 300, "price": 1.2},
                        {"timestamp": kwargs.get("start_ts") + 600, "offset_sec": 600, "price": 1.4},
                    ],
                    "truncated": True,
                    "missing": False,
                    "price_path_status": "partial",
                    "warning": "price_path_incomplete",
                }
            return {
                "token_address": kwargs["token_address"],
                "pair_address": kwargs.get("pair_address"),
                "selected_pool_address": "resolved-pool",
                "pool_address": "resolved-pool",
                "pool_resolver_source": "geckoterminal",
                "pool_resolver_confidence": "high",
                "pool_candidates_seen": 3,
                "pool_resolution_status": "resolved",
                "source_provider": "geckoterminal_pool_ohlcv",
                "requested_start_ts": kwargs.get("start_ts"),
                "requested_end_ts": kwargs.get("end_ts"),
                "interval_sec": kwargs.get("interval_sec"),
                "price_path": [{"timestamp": kwargs.get("start_ts"), "offset_sec": 0, "price": 1.0}],
                "truncated": True,
                "missing": False,
                "price_path_status": "partial",
                "warning": "price_path_incomplete",
            }

    monkeypatch.setattr(chain_backfill, "PriceHistoryClient", GeckoPartialChooser)
    result = chain_backfill._collect_price_paths(
        {"token_address": "tok", "entry_time": "2026-03-16T00:00:00Z"},
        {},
        _base_config(price_history_provider="geckoterminal_pool_ohlcv", price_interval_sec=60, price_interval_fallbacks=[300], price_path_window_fallback_multipliers=[]),
    )[0]

    assert result["interval_sec"] == 300
    assert len(result["price_path"]) == 3
