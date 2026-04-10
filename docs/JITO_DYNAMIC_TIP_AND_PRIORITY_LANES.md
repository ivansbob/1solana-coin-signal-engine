# JITO Dynamic Tip and Priority Lanes

## Overview

This document describes the implementation of dynamic Jito tip calculation and priority lane classification for realistic transaction execution modeling in Solana coin signal engine.

## Goal

Add modeling of dynamic Jito tip and priority lanes to account for realistic transaction landing costs and probabilities, maintaining honest approach under ≤$10 budget constraints with focus on replay/paper trading first.

## Key Concepts

### Priority Lane Classification

Network congestion is classified into three priority lanes:

- **baseline**: Normal network conditions (< 30% congestion)
- **elevated**: Increased competition (30-70% congestion)
- **congested**: High congestion (> 70% congestion)

### Dynamic Tip Target

The tip amount adjusts based on network conditions:

```
DynamicTipTarget = base_tip + congestion_multiplier × recent_failed_tx_rate
```

### Tip Efficiency Score

Measures cost-effectiveness of tips:

```
TipEfficiencyScore = min(1.0, estimated_landing_improvement / (tip_cost_in_sol × 10000))
```

### Landing Pressure Score

Reflects transaction landing difficulty:

```
LandingPressureScore = 1.0 - min(1.0, congestion_level × 0.8)
```

## Integration into Total Score

Jito metrics contribute a small weight to the total score:

```
TotalScore_vX = ... + 0.04 × TipEfficiencyScore × LandingPressureScore + ...
```

## Execution Gates

- **Soft blocker**: `tip_too_expensive_for_edge` if tip_budget_violation_flag is True
- **Warning**: `high_landing_pressure` if LandingPressureScore < 0.4

## Implementation Details

### Files

- `src/ingest/jito_priority_context.py`: Core adapter and simulator
- `src/paper/landing_pressure_sim.py`: Landing pressure simulation
- `src/strategy/types.py`: Extended with ExecutionContext and LandingEvidence
- `src/strategy/scoring_vX.py`: Integrated into score calculation
- `src/strategy/execution_gates.py`: Added Jito-specific gates
- `configs/jito_priority_lanes.yaml`: Configuration parameters
- `tests/test_jito_priority_context.py`: Comprehensive test suite

### Replay/Paper Mode

When live Jito data is unavailable, the system uses heuristic simulation based on:
- Smart money inflows
- Social velocity
- Network activity patterns

### Missing Data Semantics

- Conservative estimation when congestion data is stale
- Fallback to simulation for replay mode
- Optional live streaming integration

## Configuration

See `configs/jito_priority_lanes.yaml` for parameter definitions.

## Testing

Tests cover:
- Baseline lane behavior
- Congested lane tip increases
- Budget violation detection
- Simulation fallback
- Monotonic efficiency relationships

## Non-Goals

- Production-grade MEV routing
- Automatic tip setting
- Dependency on paid streaming services