# Historical replay blocker — 2026-03-23

Confirmed blocker:
Birdeye OHLCV V3 provider access/quota.

Confirmed evidence:
- direct curl to https://public-api.birdeye.so/defi/v3/ohlcv returns:
  HTTP 400
  {"success":false,"message":"Compute units usage limit exceeded"}

- direct Python probe returns:
  warning=provider_http_error
  status=missing
  provider_row_count=0
  obs_len=0
  http_status=400
  provider_error_message="Compute units usage limit exceeded"

- materialized chain_backfill rows show:
  price_path_status=missing
  warning=provider_http_error
  http_status=400
  provider_error_message="Compute units usage limit exceeded"

Impact:
- backfill_summary.json => missing: 20
- replay_summary.json => historical_rows_used: 0, unresolved_rows: 20
- replay cannot progress until provider access is restored.

Do not repeat:
- replay_7d.py
- chain backfill rebuild
until exact provider probe returns HTTP 200 with non-empty rows.
