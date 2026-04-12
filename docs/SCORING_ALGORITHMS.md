# Scoring Algorithms

The engine natively prevents blind intuition scoring via rigorous historical backtesting constraints. Whenever scoring systems scale dynamically relying upon variables, we explicitly compute `ImprovementScore` through offline mapping configurations.

## Ablation Scoring Protocol

Whenever modifying algorithms, dependencies are pushed directly through **Ablation Modules** (`src/research/abalation.py`). The offline runner disables explicit metrics (e.g., forcing `VolAccelZ` rules mathematically False globally) then sweeps parameters recursively across the dataset resolving whether components actively contribute delta efficiencies.

- Overriding outputs mathematically proves components are strictly net-positive boundaries inherently ensuring zero arbitrary variables enter execution boundaries natively.
