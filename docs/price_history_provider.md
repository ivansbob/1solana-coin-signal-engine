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

All of those normalize to `birdeye_ohlcv_v3`.

## Bootstrap states

The client keeps bootstrap/config errors distinct from real provider/data outcomes.

Bootstrap states:

- `price_history_provider_unconfigured`
- `price_history_provider_invalid`
- `price_history_provider_disabled`

Real provider/data outcomes include warnings such as:

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

That provenance is emitted both for successful/partial paths and for diagnostic missing rows.

## Pairless behavior

Provider capability is explicit:

- `require_pair_address`
- `allow_pairless_token_lookup`

If the provider allows token-only lookup, staged fallback may retry with `pair_address = null`.
If the provider does not allow pairless lookup, the backfill layer skips those pairless retries instead of misclassifying the outcome as an unconfigured provider.
