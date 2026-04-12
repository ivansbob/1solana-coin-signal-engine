# Strategy Metric Coverage Matrix (2026-04-01)

This matrix explicitly tracks the status of each metric introduced in previous PRs: Accepted and mapped (included in TotalScore v7 contract), Folded into existing PR (incorporated into another metric), Optional / paper-only (available but not in production scoring), Deferred / research-only (under development).

| Metric Family                    | Status                  | Bucket (if accepted) | Default Weight | Stage     | Notes                              | PR Introduced |
|----------------------------------|-------------------------|----------------------|----------------|-----------|------------------------------------|---------------|
| VolAccelZ                        | Accepted and mapped     | score_contribution   | 0.38           | active    | Main momentum driver               | PR-016        |
| SmartMoneyCombinedScore          | Accepted and mapped     | score_contribution   | 0.13           | active    | Distance + Bundle Shield           | PR-035        |
| LiquidityQualityScore            | Accepted and mapped     | score_contribution   | 0.11           | active    | Execution realism                  | PR-022        |
| OrderflowPurityScore             | Accepted and mapped     | hard_gate            | -              | active    | Dirty flow → blocker               | PR-022        |
| DefiHealthScore                  | Accepted and mapped     | regime_modifier      | 0.09           | optional  | Only for protocol tokens           | PR-017        |
| SocialVelocityScore              | Accepted and mapped     | score_contribution   | 0.08           | research  | Subordinate to on-chain            | PR-036        |
| DriftPerpContext                 | Accepted and mapped     | regime_modifier      | 0.05           | optional  | Contextual broader limits          | PR-010        |
| RiskAdjustedWalletScore          | Accepted and mapped     | sizing_modifier      | 0.20           | active    | Re-sizing risk mathematically      | PR-072        |
| WalletCohortScore                | Folded into existing PR | -                    | -              | -         | Incorporated into SmartMoneyCombinedScore | PR-014    |
| EvidenceQualityScore             | Optional / paper-only   | paper_only_realism   | -              | optional  | For paper trading realism only     | PR-007        |
| XValidationScore                 | Optional / paper-only   | paper_only_realism   | -              | optional  | Validation metrics for replay      | PR-003        |
| BundleClusterScore               | Deferred / research-only| -                    | -              | research  | Under development                  | PR-035        |
| RegistryScore                    | Folded into existing PR | -                    | -              | -         | Incorporated into wallet scoring   | PR-004        |
| LinkageScore                     | Deferred / research-only| -                    | -              | research  | Creator linkage metrics            | PR-015        |
| FastPrescore                     | Accepted and mapped     | replay_only          | -              | active    | Pre-scoring for discovery          | PR-002        |
