"""Tests for Jito Priority Context functionality."""

import pytest
from src.ingest.jito_priority_context import JitoPriorityContextAdapter, JitoPriorityContext
from src.paper.landing_pressure_sim import LandingPressureSimulator
from src.strategy.execution_gates import evaluate_jito_gates


class TestJitoPriorityContext:
    def test_baseline_lane_low_tip(self):
        """Test baseline lane has low tip and high efficiency."""
        adapter = JitoPriorityContextAdapter({"base_tip_lamports": 1000})
        token_ctx = {"smart_money_inflows_1h_usd": 10000}  # Low activity

        context = adapter.build_jito_context(token_ctx)

        assert context.priority_lane == "baseline"
        assert context.dynamic_tip_target_lamports == 1000  # Base tip
        assert context.tip_efficiency_score > 0.8  # High efficiency
        assert context.landing_pressure_score > 0.8  # Low pressure

    def test_congested_lane_higher_tip_and_lower_efficiency(self):
        """Test congested lane increases tip and reduces efficiency."""
        adapter = JitoPriorityContextAdapter({
            "base_tip_lamports": 1000,
            "congestion_multiplier": 5000
        })
        token_ctx = {"smart_money_inflows_1h_usd": 200000, "social_velocity_10m": 100}  # High activity

        context = adapter.build_jito_context(token_ctx)

        assert context.priority_lane == "congested"
        assert context.dynamic_tip_target_lamports > 1000  # Higher tip
        assert context.tip_efficiency_score < 0.8  # Lower efficiency due to cost
        assert context.landing_pressure_score < 0.5  # High pressure

    def test_tip_budget_violation_triggers_warning(self):
        """Test that excessive tip triggers budget violation."""
        adapter = JitoPriorityContextAdapter({
            "base_tip_lamports": 1000,
            "congestion_multiplier": 100000,  # Very high multiplier
            "max_budget_sol": 0.001  # Very low budget: 0.001 SOL = 1_000_000 lamports
        })
        token_ctx = {"smart_money_inflows_1h_usd": 500000}  # Very high activity

        context = adapter.build_jito_context(token_ctx)

        assert context.tip_budget_violation_flag is True

        # Test gates
        jito_ctx_dict = {
            "tip_budget_violation_flag": context.tip_budget_violation_flag,
            "landing_pressure_score": context.landing_pressure_score
        }
        gates = evaluate_jito_gates(jito_ctx_dict)

        assert "tip_too_expensive_for_edge" in gates["soft_blockers"]

    def test_replay_mode_uses_simulation_fallback(self):
        """Test that without live data, simulation fallback is used."""
        adapter = JitoPriorityContextAdapter()
        token_ctx = {"smart_money_inflows_1h_usd": 50000}

        context = adapter.build_jito_context(token_ctx, live_data=None)  # No live data

        # Should still produce valid context using simulation
        assert isinstance(context, JitoPriorityContext)
        assert context.congestion_level >= 0.0
        assert context.congestion_level <= 1.0
        assert context.priority_lane in ["baseline", "elevated", "congested"]

    def test_tip_efficiency_is_monotonic_with_landing_improvement(self):
        """Test that tip efficiency increases with landing improvement."""
        adapter = JitoPriorityContextAdapter()

        # Test with different improvement levels
        improvements = [5.0, 15.0, 30.0]
        efficiencies = []

        for imp in improvements:
            # Mock context with different improvements
            class MockContext:
                estimated_landing_improvement_pct = imp
                tip_efficiency_score = adapter.calculate_tip_efficiency_score(imp, 0.00001)  # Low cost

            efficiencies.append(MockContext().tip_efficiency_score)

        # Should be monotonic increasing
        assert efficiencies[0] <= efficiencies[1] <= efficiencies[2]


class TestLandingPressureSimulator:
    def test_simulation_produces_valid_results(self):
        """Test that landing simulation produces valid results."""
        sim = LandingPressureSimulator()

        # Create mock jito context
        class MockJitoContext:
            tip_efficiency_score = 0.8
            congestion_level = 0.2
            landing_pressure_score = 0.9

        result = sim.simulate_landing_pressure(MockJitoContext(), num_simulations=10)

        assert 0.0 <= result.success_rate <= 1.0
        assert result.average_landing_time_ms >= 0
        assert result.simulated_tx_count == 10
        assert isinstance(result.failure_reasons, dict)

    def test_estimate_landing_improvement(self):
        """Test landing improvement estimation."""
        sim = LandingPressureSimulator()

        class MockJitoContext:
            tip_efficiency_score = 0.5
            congestion_level = 0.3

        improvement = sim.estimate_landing_improvement(MockJitoContext())

        assert improvement >= 0.0
        assert improvement <= 100.0  # Reasonable percentage