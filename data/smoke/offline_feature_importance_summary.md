# Offline feature importance summary

This artifact is **analysis-only** and **not for online decisioning**.
Importance scores reflect association strength, not causal proof.

## Input summary
- source: /Users/ivansbobrovs/Downloads/1solana-coin-signal-engine-main/tests/fixtures/offline_feature_importance/healthy_mixed_replay_matrix.jsonl
- usable rows: 8
- excluded rows: 0
- malformed rows: 0
- excluded outcome fields: exit_reason_final, hold_sec, mae_pct_240s, mfe_pct_240s, net_pnl_pct, time_to_first_profit_sec, trend_survival_15m

## Global warnings
- class imbalance for fast_failure_flag: positives=3 negatives=5
- class imbalance for profitable_trade_flag: positives=5 negatives=3
- fast_failure_flag:bundle_composition_dominant:limited observed rows (8)
- fast_failure_flag:bundle_count_first_60s:limited observed rows (8)
- fast_failure_flag:buy_pressure_entry:limited observed rows (8)
- fast_failure_flag:cluster_concentration_ratio:limited observed rows (8)
- fast_failure_flag:creator_in_cluster_flag:limited observed rows (8)
- fast_failure_flag:liquidity_refill_ratio_120s:limited observed rows (8)
- fast_failure_flag:liquidity_usd:limited observed rows (8)
- fast_failure_flag:net_unique_buyers_60s:limited observed rows (8)
- fast_failure_flag:regime_confidence:limited observed rows (8)
- fast_failure_flag:seller_reentry_ratio:limited observed rows (8)
- fast_failure_flag:smart_wallet_dispersion_score:limited observed rows (8)
- fast_failure_flag:x_author_velocity_5m:limited observed rows (8)
- low sample size for fast_failure_flag: 8 rows
- low sample size for profitable_trade_flag: 8 rows
- low sample size for trend_success_flag: 8 rows
- profitable_trade_flag:bundle_composition_dominant:limited observed rows (8)
- profitable_trade_flag:bundle_count_first_60s:limited observed rows (8)
- profitable_trade_flag:buy_pressure_entry:limited observed rows (8)
- profitable_trade_flag:cluster_concentration_ratio:limited observed rows (8)
- profitable_trade_flag:creator_in_cluster_flag:limited observed rows (8)
- profitable_trade_flag:liquidity_refill_ratio_120s:limited observed rows (8)
- profitable_trade_flag:liquidity_usd:limited observed rows (8)
- profitable_trade_flag:net_unique_buyers_60s:limited observed rows (8)
- profitable_trade_flag:regime_confidence:limited observed rows (8)
- profitable_trade_flag:seller_reentry_ratio:limited observed rows (8)
- profitable_trade_flag:smart_wallet_dispersion_score:limited observed rows (8)
- profitable_trade_flag:x_author_velocity_5m:limited observed rows (8)
- trend_success_flag:bundle_composition_dominant:limited observed rows (8)
- trend_success_flag:bundle_count_first_60s:limited observed rows (8)
- trend_success_flag:buy_pressure_entry:limited observed rows (8)
- trend_success_flag:cluster_concentration_ratio:limited observed rows (8)
- trend_success_flag:creator_in_cluster_flag:limited observed rows (8)
- trend_success_flag:liquidity_refill_ratio_120s:limited observed rows (8)
- trend_success_flag:liquidity_usd:limited observed rows (8)
- trend_success_flag:net_unique_buyers_60s:limited observed rows (8)
- trend_success_flag:regime_confidence:limited observed rows (8)
- trend_success_flag:seller_reentry_ratio:limited observed rows (8)
- trend_success_flag:smart_wallet_dispersion_score:limited observed rows (8)
- trend_success_flag:x_author_velocity_5m:limited observed rows (8)

## Target: profitable_trade_flag
- description: Offline binary target for positive net PnL trades.
- sample_size: 8
- positives: 5
- negatives: 3
- warnings: low sample size for profitable_trade_flag: 8 rows, class imbalance for profitable_trade_flag: positives=5 negatives=3
- top feature groups:
  - continuation_features: total=7.6595 avg=1.9149 coverage=100.00% top=seller_reentry_ratio, smart_wallet_dispersion_score, liquidity_refill_ratio_120s
  - cluster_features: total=4.0772 avg=2.0386 coverage=100.00% top=creator_in_cluster_flag, cluster_concentration_ratio
  - friction_features: total=3.7251 avg=1.8625 coverage=100.00% top=buy_pressure_entry, liquidity_usd
  - bundle_features: total=2.3163 avg=1.1581 coverage=100.00% top=bundle_count_first_60s, bundle_composition_dominant
  - regime_features: total=1.8708 avg=1.8708 coverage=100.00% top=regime_confidence
- top features:
  - #1 creator_in_cluster_flag [cluster_features] score=2.0656 coverage=100.00% status=ok direction=negative_association
  - #2 cluster_concentration_ratio [cluster_features] score=2.0116 coverage=100.00% status=ok direction=negative_association
  - #3 seller_reentry_ratio [continuation_features] score=2.0084 coverage=100.00% status=ok direction=negative_association
  - #4 buy_pressure_entry [friction_features] score=1.9422 coverage=100.00% status=ok direction=positive_association
  - #5 smart_wallet_dispersion_score [continuation_features] score=1.9210 coverage=100.00% status=ok direction=positive_association
  - #6 liquidity_refill_ratio_120s [continuation_features] score=1.9167 coverage=100.00% status=ok direction=positive_association
  - #7 regime_confidence [regime_features] score=1.8708 coverage=100.00% status=ok direction=positive_association
  - #8 bundle_count_first_60s [bundle_features] score=1.8475 coverage=100.00% status=ok direction=negative_association

## Target: trend_success_flag
- description: Offline binary target for stronger trend-like success behavior using survival, MFE, PnL, and early-profit evidence.
- sample_size: 8
- positives: 4
- negatives: 4
- warnings: low sample size for trend_success_flag: 8 rows
- top feature groups:
  - continuation_features: total=6.7796 avg=1.6949 coverage=100.00% top=smart_wallet_dispersion_score, liquidity_refill_ratio_120s, net_unique_buyers_60s
  - friction_features: total=3.4432 avg=1.7216 coverage=100.00% top=liquidity_usd, buy_pressure_entry
  - cluster_features: total=3.1751 avg=1.5875 coverage=100.00% top=cluster_concentration_ratio, creator_in_cluster_flag
  - bundle_features: total=2.1071 avg=1.0535 coverage=100.00% top=bundle_count_first_60s, bundle_composition_dominant
  - x_features: total=1.7534 avg=1.7534 coverage=100.00% top=x_author_velocity_5m
- top features:
  - #1 x_author_velocity_5m [x_features] score=1.7534 coverage=100.00% status=ok direction=positive_association
  - #2 bundle_count_first_60s [bundle_features] score=1.7321 coverage=100.00% status=ok direction=negative_association
  - #3 regime_confidence [regime_features] score=1.7304 coverage=100.00% status=ok direction=positive_association
  - #4 liquidity_usd [friction_features] score=1.7302 coverage=100.00% status=ok direction=positive_association
  - #5 smart_wallet_dispersion_score [continuation_features] score=1.7285 coverage=100.00% status=ok direction=positive_association
  - #6 buy_pressure_entry [friction_features] score=1.7130 coverage=100.00% status=ok direction=positive_association
  - #7 liquidity_refill_ratio_120s [continuation_features] score=1.7124 coverage=100.00% status=ok direction=positive_association
  - #8 net_unique_buyers_60s [continuation_features] score=1.7000 coverage=100.00% status=ok direction=positive_association

## Target: fast_failure_flag
- description: Offline binary target for rapid bad outcomes or early failure behavior.
- sample_size: 8
- positives: 3
- negatives: 5
- warnings: low sample size for fast_failure_flag: 8 rows, class imbalance for fast_failure_flag: positives=3 negatives=5
- top feature groups:
  - continuation_features: total=7.6595 avg=1.9149 coverage=100.00% top=seller_reentry_ratio, smart_wallet_dispersion_score, liquidity_refill_ratio_120s
  - cluster_features: total=4.0772 avg=2.0386 coverage=100.00% top=creator_in_cluster_flag, cluster_concentration_ratio
  - friction_features: total=3.7251 avg=1.8625 coverage=100.00% top=buy_pressure_entry, liquidity_usd
  - bundle_features: total=2.3163 avg=1.1581 coverage=100.00% top=bundle_count_first_60s, bundle_composition_dominant
  - regime_features: total=1.8708 avg=1.8708 coverage=100.00% top=regime_confidence
- top features:
  - #1 creator_in_cluster_flag [cluster_features] score=2.0656 coverage=100.00% status=ok direction=positive_association
  - #2 cluster_concentration_ratio [cluster_features] score=2.0116 coverage=100.00% status=ok direction=positive_association
  - #3 seller_reentry_ratio [continuation_features] score=2.0084 coverage=100.00% status=ok direction=positive_association
  - #4 buy_pressure_entry [friction_features] score=1.9422 coverage=100.00% status=ok direction=negative_association
  - #5 smart_wallet_dispersion_score [continuation_features] score=1.9210 coverage=100.00% status=ok direction=negative_association
  - #6 liquidity_refill_ratio_120s [continuation_features] score=1.9167 coverage=100.00% status=ok direction=negative_association
  - #7 regime_confidence [regime_features] score=1.8708 coverage=100.00% status=ok direction=negative_association
  - #8 bundle_count_first_60s [bundle_features] score=1.8475 coverage=100.00% status=ok direction=positive_association
