"""Landing Pressure Simulator for replay/paper trading."""

from typing import Dict, Any, Optional
from dataclasses import dataclass
import random

@dataclass
class LandingSimulationResult:
    success_rate: float  # 0.0 to 1.0
    average_landing_time_ms: float
    failure_reasons: Dict[str, int]  # e.g., {"congestion": 10, "insufficient_tip": 5}
    simulated_tx_count: int

class LandingPressureSimulator:
    """Simulator for transaction landing pressure in replay/paper mode."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.baseline_success_rate = self.config.get("baseline_success_rate", 0.95)
        self.congestion_penalty_factor = self.config.get("congestion_penalty_factor", 0.3)
        self.tip_efficiency_factor = self.config.get("tip_efficiency_factor", 2.0)

    def simulate_landing_pressure(self, jito_context: Any, num_simulations: int = 100) -> LandingSimulationResult:
        """Simulate landing pressure for given Jito context."""
        success_count = 0
        total_landing_time = 0.0
        failure_reasons = {}

        base_success_rate = self.baseline_success_rate

        # Adjust for congestion
        congestion_penalty = jito_context.congestion_level * self.congestion_penalty_factor
        adjusted_success_rate = max(0.1, base_success_rate - congestion_penalty)

        # Adjust for tip efficiency
        tip_bonus = jito_context.tip_efficiency_score * self.tip_efficiency_factor
        final_success_rate = min(0.99, adjusted_success_rate + tip_bonus)

        for _ in range(num_simulations):
            if random.random() < final_success_rate:
                success_count += 1
                # Simulate landing time: faster with higher efficiency
                landing_time = random.uniform(100, 1000) * (1 - jito_context.tip_efficiency_score * 0.5)
                total_landing_time += landing_time
            else:
                # Determine failure reason
                if jito_context.congestion_level > 0.7:
                    reason = "congestion"
                elif jito_context.tip_efficiency_score < 0.3:
                    reason = "insufficient_tip"
                else:
                    reason = "network_issue"
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

        average_landing_time = total_landing_time / success_count if success_count > 0 else 0.0

        return LandingSimulationResult(
            success_rate=success_count / num_simulations,
            average_landing_time_ms=average_landing_time,
            failure_reasons=failure_reasons,
            simulated_tx_count=num_simulations
        )

    def estimate_landing_improvement(self, jito_context: Any) -> float:
        """Estimate percentage improvement in landing probability."""
        # Simple estimation based on tip efficiency and congestion
        base_improvement = jito_context.tip_efficiency_score * 20.0  # Up to 20% from tip
        congestion_reduction = jito_context.congestion_level * 10.0  # Congestion reduces effectiveness
        return max(0.0, base_improvement - congestion_reduction)