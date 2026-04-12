# Historical replay harness

The historical replay harness is the only supported replay path in this repository. It replaces the old replay shim with an evidence-first path that reconstructs token lifecycles from recorded local artifacts.

## Removal of the stale synthetic simulator

The old synthetic replay helper at `src/replay/simulator.py` has been removed. Future contributors must not reintroduce a separate synthetic replay module as a default path, convenience fallback, or hidden import surface.

Any smoke or degraded replay behavior must continue to flow through `src/replay/historical_replay_harness.py`, with explicit provenance/status labeling in the emitted artifacts. Synthetic PnL, synthetic hold-time generation, and synthetic exit reasons must not return as an alternative replay engine beside the historical harness.

## What makes replay historical here

A run is treated as historical when the harness can load persisted local artifacts such as:

- scored token rows
- entry candidates or signal events
- historical trades or positions
- recorded price paths / lifecycle observations
- persisted bundle, linkage, continuation, wallet, or X-derived fields when present

The harness does **not** fabricate complete trade outcomes when the artifacts are incomplete.

## Input files

The loader looks for these files inside an artifact directory:

- `scored_tokens.jsonl`
- `entry_candidates.json` or `entry_candidates.jsonl`
- `signals.jsonl` or `entry_events.jsonl`
- `trades.jsonl`
- `positions.json`
- `price_paths.json` or `price_paths.jsonl`
- `universe.json` / `universe.jsonl`

Only the files that exist are used. Missing files become explicit partial/unresolved replay status rather than implicit synthetic completion.

## Lifecycle reconstruction

For each token, the harness:

1. loads recorded candidate/signal/trade/position context,
2. re-runs regime selection with the existing deterministic regime rules,
3. reconstructs entry only from recorded evidence,
4. walks the recorded historical price path through the existing exit rules,
5. emits a resolved, partial, ignored, or unresolved lifecycle.

Lifecycle state transitions are managed by `src/replay/replay_state_machine.py`.

## Partial replay policy

If the harness is missing any of the following, it degrades honestly:

- entry price
- price path
- exit observation coverage
- exit snapshots
- bundle / continuation / linkage context

Output rows are labeled with additive provenance fields:

- `replay_input_origin`
- `replay_data_status`
- `replay_resolution_status`
- `synthetic_assist_flag`

Typical statuses:

- `historical`
- `historical_partial`
- `synthetic_smoke`
- `mixed`

## Artifacts written

Each run writes:

- `signals.jsonl`
- `trades.jsonl`
- `positions.json`
- `trade_feature_matrix.jsonl`
- `replay_summary.json`
- `replay_summary.md`
- `manifest.json`

The summary explicitly includes:

- historical row count
- partial row count
- unresolved row count
- whether synthetic fallback was used
- wallet weighting mode
- config hash

## Comparative replay modes

The driver supports wallet weighting comparisons with:

- `off`
- `on`
- `shadow` (true replay rescore mode through the same canonical unified scorer contract; no CLI remap to `on`; wallet deltas are logged but not applied)

## Smoke mode

`scripts/historical_replay_smoke.py` runs the harness against deterministic local fixtures and writes outputs under `data/smoke/`.

## Out of scope

This harness intentionally does **not**:

- place live orders
- integrate with exchanges
- introduce an alternative unified score truth path
- redesign regime logic
- redesign exit logic
- fabricate complete PnL when historical evidence is incomplete

## Wallet-weighting replay parity

Replay resolves scored artifacts in a mode-aware order (`scored_tokens.<mode>.json[l]` before generic `scored_tokens.json[l]`). If only a generic scored artifact is present, the harness re-scores it under the requested wallet mode before lifecycle reconstruction. Fresh replay summaries, manifests, signal/trade artifacts, and `trade_feature_matrix.jsonl` rows include `wallet_weighting_requested_mode`, `wallet_weighting_effective_mode`, `replay_score_source`, `wallet_mode_parity_status`, `historical_input_hash`, and score-layer wallet component fields so `off` / `shadow` / `on` runs can be compared over the same historical truth layer.

Evidence-quality score penalties (`partial_evidence_penalty`, `low_confidence_evidence_penalty`) are propagated into the replay trade feature matrix alongside existing evidence-quality summary fields.

## Candidate config propagation

Calibration candidates are now applied as real replay setting overrides. The harness merges baseline settings, root-level replay settings, and per-candidate overrides before a replay run starts, so calibration no longer silently replays on defaults while pretending to evaluate candidate parameter combinations.

## Lifecycle artifact contract

`trades.jsonl` must be analyzer-usable. Preferred output is a canonical buy/sell ledger. When replay writes a flattened historical lifecycle row instead, the analyzer must still treat that row as a first-class closed trade lifecycle rather than falling back to `positions.json` as the hidden primary source of truth. `positions.json` remains a support / fallback artifact, not the only way to recover closed trades.

## Seed price-path backfill fallbacks

Replay seed backfill now uses staged price-path recovery instead of a single OHLCV fetch. The collector first tries the requested launch window and interval, then can widen the window, retry on coarser intervals, retry without a pair binding, and shift the start timestamp backward by a prelaunch buffer. The selected result keeps compact provenance in `attempt_count`, `attempts`, `resolved_via_fallback`, and `fallback_mode` so missing paths remain diagnosable instead of collapsing into a generic provider miss.

Backfill now also applies a dedicated price-history router before concluding a token row is missing. The router tries seeded pair/pool and token routes across configured providers in deterministic order and propagates route-attempt provenance (`price_history_route_attempts`, `price_history_route_selected`, `selected_route_provider`, `selected_route_kind`). This improves failover but does **not** mask missing data; rows remain explicitly missing unless a provider returns `complete` data or a real `partial_but_usable_row=true`.

When the configured provider is `geckoterminal_pool_ohlcv`, price-path materialization now includes a pool-candidate family (seed hint + canonical resolved pool + alternate resolved pools) as part of the staged ladder. Replay candidates may still carry a seed `pair_address`, but that address is treated as a hint while the emitted row separately records `selected_pool_address`, `pool_resolver_source`, `pool_resolver_confidence`, `pool_candidates_seen`, `pool_resolution_status`, `attempted_pool_candidates`, `selected_pool_candidate_rank`, and `selected_pool_candidate_source`. That means a replay row can preserve both the original seed hint and the actual provider pool used to fetch OHLCV.

For Gecko, `ohlcv_not_available` is now interpreted as a dead-end for a specific pool candidate, not automatically for the whole token/provider route. Provider-family exhaustion is only marked once all configured pool candidates are exhausted, or when cooldown/rate-limit constraints force termination. Replay summary output now distinguishes single-pool misses from provider-family exhaustion and includes candidate-pool attempt/success counters.

Important contract guarantees for diagnostics:

- A seeded pair/pool is always a **routing hint**, never terminal truth for the token.
- `ohlcv_not_available` for one pool candidate does **not** imply token dead-end while other pool candidates remain.
- Every replay output row (including missing rows) must carry route/failure metadata: `selected_route_provider`, `selected_route_kind`, `selected_pool_address`, `selected_pool_candidate_rank`, `selected_pool_candidate_source`, `attempted_pool_candidates`, `attempted_pool_candidate_count`, `provider_failure_class`, `provider_failure_retryable`, `provider_family_exhausted`, and `pool_resolution_status`.

Before those OHLCV attempts begin, the backfill layer now resolves a seed time anchor in stages. It prefers lifecycle-aware candidate fields in a deterministic order: `price_path_start_ts`, `replay_entry_time`, `entry_time`, `opened_at`, and then broader discovery-style timestamps. Only after those higher-preference anchors are absent does it fall back to cached `block_times`, embedded `signatures[].blockTime`, and finally controlled signature hydration that resolves real `blockTime` values from string-only signatures without inventing timestamps. Successful and missing rows now expose both the winning anchor and the discarded lower-priority candidates through `price_path_time_source`, `price_path_time_derived`, `price_path_anchor_field`, `time_anchor_resolution_status`, `time_anchor_attempts`, `time_anchor_candidates`, `time_anchor_discarded_candidates`, `time_anchor_preference_applied`, `signature_hydration_attempted`, `signature_hydration_count`, and `missing_required_fields`.

Replay input assembly can also emit `replay_entry_time` into backfill-ready candidates when the historical harness already reconstructed an entry timestamp from signals, trades, or positions. That keeps the same real time anchor available to both lifecycle replay and upstream price-path population instead of letting sparse seed fixtures stop at `attempt_count = 0` before the first provider fetch.

Price-history bootstrap is now diagnosed separately from real provider/data misses. Backfill rows expose `price_history_provider`, `price_history_provider_status`, `provider_bootstrap_ok`, `provider_config_source`, and `provider_request_summary`, so a row can fail fast on `price_history_provider_unconfigured`, `price_history_provider_invalid`, or `price_history_provider_disabled` without burning the whole staged fallback ladder. Once bootstrap is configured, remaining warnings should describe actual provider/data outcomes such as pool-resolution failures, empty pool OHLCV ranges, rate limits, HTTP failures, parse failures, incomplete windows, or pair/token capability mismatches.

## Gecko sparse OHLCV densification and partial replay usage

When provider is `geckoterminal_pool_ohlcv`, OHLCV can be sparse even with HTTP 200 and a valid pool. The collector now densifies only **internal** gaps between observed provider candles:

- missing internal timestamps are filled at the configured interval;
- synthetic bar OHLC = previous close, volume = `0`;
- no bars are created before the first observed candle;
- no bars are created after the last observed candle.

Materialized rows expose diagnostics/provenance:

- `price_path_origin` (`provider_observed` or `provider_observed_plus_gap_fill`)
- `gap_fill_applied`
- `gap_fill_count`
- `observed_row_count`
- `densified_row_count`

Replay no longer treats every partial path as hard-missing. A partial historical row is replay-usable when post-entry points exist; it becomes unresolved only when the path is empty or all points are pre-entry only. Summary output now reports:

- `partial_historical_rows_used`
- `gap_filled_rows_used`
- `missing_price_path_rows`
- `partial_but_usable_rows`
