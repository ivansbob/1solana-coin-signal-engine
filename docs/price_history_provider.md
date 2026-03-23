# Price history provider bootstrap contract

`collectors/price_history_client.py` treats provider bootstrap as a first-class stage before any historical fetch attempts run.

## Supported provider selection paths

The resolver accepts these keys, in priority order:

1. `backfill.price_history_provider`
2. `providers.price_history.provider`
3. `price_history.provider`
4. legacy `backfill.price_provider`

Accepted aliases:

- `birdeye`
- `birdeye_v3`
- `birdeye_ohlcv`
- `birdeye_ohlcv_v3`
- `geckoterminal`
- `geckoterminal_pool`
- `geckoterminal_pool_ohlcv`

The Birdeye aliases normalize to `birdeye_ohlcv_v3`. The GeckoTerminal aliases normalize to `geckoterminal_pool_ohlcv`.

## Bootstrap states

The client keeps bootstrap/config errors distinct from real provider/data outcomes.

Bootstrap states:

- `price_history_provider_unconfigured`
- `price_history_provider_invalid`
- `price_history_provider_disabled`

Real provider/data outcomes include warnings such as:

- `pool_resolution_failed`
- `no_pool_ohlcv_rows`
- `provider_rate_limited`
- `provider_http_error`
- `provider_empty_payload`
- `price_rows_unparseable`
- `no_ohlcv_rows`
- `price_path_incomplete`
- `provider_pair_address_required`

## Provenance emitted into price-path rows

Each collected row should expose:

- `price_history_provider`
- `price_history_provider_status`
- `provider_bootstrap_ok`
- `provider_config_source`
- `provider_request_summary`
- `selected_pool_address`
- `pool_resolver_source`
- `pool_resolver_confidence`
- `pool_candidates_seen`
- `pool_resolution_status`

That provenance is emitted both for successful/partial paths and for diagnostic missing rows.

## GeckoTerminal pool OHLCV behavior

`geckoterminal_pool_ohlcv` is the preferred free fallback for replay backfill.

- The provider resolves `token_address -> canonical pool` before requesting OHLCV.
- Replay uses pool OHLCV, not token-routed OHLCV, so the selected price path stays deterministic instead of following a moving "most liquid" token route.
- `pair_address` can still be used as a seed pool hint, but the provider records the actual `selected_pool_address` separately.
- Minute OHLCV requests default to `include_empty_intervals = true` so replay minute series do not silently drop no-swap buckets.
- Historical pagination walks backward with `before_timestamp` until the requested range is covered or the provider stops returning older candles.
- Config now carries GeckoTerminal-specific request metadata through `provider_request_summary`, including `request_version`, `currency`, `token_side`, `include_empty_intervals`, `pool_resolver`, `resolver_cache_ttl_sec`, and `max_ohlcv_limit`.
- Public API usage should remain conservative because the fallback is intended for the documented ~30 calls/minute rate limit.

## Pairless behavior

Provider capability is explicit:

- `require_pair_address`
- `allow_pairless_token_lookup`

If the provider allows token-only lookup, staged fallback may retry with `pair_address = null`.
If the provider does not allow pairless lookup, the backfill layer skips those pairless retries instead of misclassifying the outcome as an unconfigured provider.
