# Execution Realism

## Overview

This document outlines the execution realism features implemented in the Solana coin signal engine, focusing on realistic transaction costs, landing probabilities, and slippage modeling.

## Key Components

### Jito Dynamic Tips and Priority Lanes

See [JITO_DYNAMIC_TIP_AND_PRIORITY_LANES.md](JITO_DYNAMIC_TIP_AND_PRIORITY_LANES.md) for detailed implementation of dynamic tip calculation and priority lane classification.

### Liquidity Quality and Slippage Realism

Implemented in PR-017, provides realistic slippage estimates based on Jupiter impact calculations.

### Network Realism

PR-024 models network congestion and failed transaction rates.

### Micro-capital Jito Realism

PR-021 ensures tips remain proportional to small capital bases.

## Philosophy

- **Honest under ≤$10 constraints**: All modeling respects small trading budgets
- **Replay/paper first**: Core functionality works without expensive live infrastructure
- **Transparent trade-offs**: Clear visibility into cost vs. probability decisions

## Integration Points

### Total Score Impact

Execution realism metrics contribute small weights to prevent gaming:

- Jito efficiency: 0.04 × tip_efficiency × landing_pressure
- Liquidity quality: 11.0 × liquidity_score
- Friction penalties: Variable deductions for excessive slippage

### Gate System

Execution blockers prevent unrealistic trades:

- Tip budget violations
- Excessive slippage
- High landing pressure
- Poor liquidity quality

## Testing Strategy

- Unit tests for all formulas and edge cases
- Integration tests for score calculation
- Simulation validation against historical data
- Budget constraint verification

## Future Extensions

- Live Jito streaming integration (optional)
- Advanced MEV modeling
- Cross-chain execution realism