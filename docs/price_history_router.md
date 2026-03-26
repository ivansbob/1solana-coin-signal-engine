# Price history router contract

The price-history router is a transport helper that picks route/provider attempts in a deterministic order and returns the first real usable row.

Order:
1. seeded pair/pool route on primary provider,
2. token resolver route on primary provider,
3. seeded pair/pool route on alternate providers,
4. token resolver route on alternate providers.

Terminal success is either:
- `price_path_status=complete`, or
- `partial_but_usable_row=true` (when `backfill.price_history_router.accept_partial_usable=true`).

The router **does not** synthesize data. Missing rows stay missing, and `historical_rows_used` should only increase when a provider returns usable real price-path data.

Router metadata fields on output rows:
- `price_history_route_selected`
- `price_history_route_attempts`
- `price_history_router_status`
- `price_history_router_warning`
- `price_history_fallback_used`
- `selected_route_provider`
- `selected_route_kind`
- `selected_route_seed_source`
