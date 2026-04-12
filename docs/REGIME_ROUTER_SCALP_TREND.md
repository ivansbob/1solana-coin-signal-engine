# Regime Router (Scalp / Trend) Definition

The Engine acts as a `Deterministic Router` prior to fully computing the entrance size and exiting properties. The Regime Router categorizes candidates into one of natively supported regimes or drops the candidate (ignores it), allowing execution logic downstream to assume very specific bounds.

### Available Regimes

- **TREND**: The token is showing signs of fundamental backing, substantial initial distribution among highly reputed clusters (Smart Wallets), low LP manipulation, and accumulation. Expect to hold this longer to ride out minor volatility dips.
- **SCALP**: The token is showing massive velocity via immediate, intense buying (very early in its first few minutes), and volume surges with high single/two-cluster saturation. We hold briefly and exit aggressively when momentum shifts.
- **IGNORE**: Anything with significant blockers, catastrophic failures in clustering structure, missing data, or highly manipulated behavior.

## Decoupled Implementation

Previously, regime classification was tightly knit to the unified score computation. Now we explicitly expose `decide_regime()` which accepts a `TokenContext` dictionary, returning structured types:

```python
class RegimeDecision(TypedDict):
    regime: Literal["SCALP", "TREND", "IGNORE", "UNKNOWN"]
    confidence: float
    expected_hold_class: Literal["short", "medium", "long", "none"]
    reason: str
    reason_flags: List[str]
    warnings: List[str]
    blockers: List[str]
    ...
```

### Determinism

By depending purely on stateful dictionary contexts populated by API pulls rather than dynamically shifting state, `decide_regime` enforces absolute determinism useful for replay testing and robust decision tracing.

### Core Eligibility

If a token has ANY critical failures such as bad orderflow, failed LP burns under heavy sell pressure, massive clustering near dev wallets, it yields `IGNORE`.
Otherwise:
- **TREND** eligibility evaluates holder growth, multi-cluster dispersion, confirmation of success ratios without severe retry looping.
- **SCALP** eligibility assesses speed, timing bounds, early buy ratios and volume velocity to confirm hyper-momentum.

The `confidence` scale operates internally adjusting points based on specific feature flags. If below structural thresholds (e.g., `ENTRY_REGIME_CONFIDENCE_FLOOR_SCALP`), the process defaults safely to `IGNORE`.
