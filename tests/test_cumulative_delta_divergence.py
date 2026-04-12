"""Tests for cumulative delta divergence metrics."""

import pytest
from src.strategy.orderflow_metrics import (
    compute_cumulative_delta_divergence,
    compute_cumulative_delta_divergence_with_data
)


class TestCumulativeDeltaDivergence:
    """Test cases for cumulative delta divergence calculation."""

    def test_strong_hidden_accumulation(self):
        """Test strong hidden accumulation case (div >= 0.18, score = 1.0)."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=200.0,  # 200K USD
            cum_sell_volume=100.0,  # 100K USD
            price_change_bps=150.0  # +1.5%
        )

        # cum_delta = 100, divergence = 100 / (150 + 1) ≈ 0.662
        assert result["cum_delta_divergence"] == pytest.approx(0.662, rel=1e-3)
        assert result["cum_delta_score"] == 1.0
        assert result["cum_delta_provenance"]["data_source"] == "provided_data"

    def test_neutral_accumulation(self):
        """Test neutral accumulation case (0.08 <= div < 0.18, score = 0.65)."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=113.0,  # 113K USD
            cum_sell_volume=100.0,  # 100K USD
            price_change_bps=100.0  # +1.0%
        )

        # cum_delta = 13, divergence = 13 / (100 + 1) ≈ 0.1287
        assert result["cum_delta_divergence"] == pytest.approx(0.1287, rel=1e-3)
        assert result["cum_delta_score"] == 0.65

    def test_weak_accumulation(self):
        """Test weak accumulation case (0.0 <= div < 0.08, score = 0.3)."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=105.0,  # 105K USD
            cum_sell_volume=100.0,  # 100K USD
            price_change_bps=200.0  # +2.0%
        )

        # cum_delta = 5, divergence = 5 / (200 + 1) ≈ 0.024875
        assert result["cum_delta_divergence"] == pytest.approx(0.024875, abs=1e-5)
        assert result["cum_delta_score"] == 0.3

    def test_hidden_distribution(self):
        """Test hidden distribution case (div < 0, score = 0.0)."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=80.0,  # 80K USD
            cum_sell_volume=120.0,  # 120K USD
            price_change_bps=50.0  # +0.5%
        )

        # cum_delta = -40, divergence = -40 / (50 + 1) ≈ -0.784
        assert result["cum_delta_divergence"] == pytest.approx(-0.784, rel=1e-3)
        assert result["cum_delta_score"] == 0.0

    def test_missing_data(self):
        """Test missing data case (returns None for divergence and score)."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=None,
            cum_sell_volume=None,
            price_change_bps=None
        )

        assert result["cum_delta_divergence"] is None
        assert result["cum_delta_score"] is None
        assert result["cum_delta_provenance"]["data_source"] == "missing_data"

    def test_zero_price_change(self):
        """Test zero price change case (uses +1 to avoid division by zero)."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=150.0,  # 150K USD
            cum_sell_volume=100.0,  # 100K USD
            price_change_bps=0.0  # No price change
        )

        # cum_delta = 50, divergence = 50 / (0 + 1) = 50.0
        assert result["cum_delta_divergence"] == 50.0
        assert result["cum_delta_score"] == 1.0  # Since 50.0 >= 0.18

    def test_negative_price_change(self):
        """Test negative price change case."""
        result = compute_cumulative_delta_divergence_with_data(
            token_address="test_token",
            cum_buy_volume=100.0,  # 100K USD
            cum_sell_volume=110.0,  # 110K USD
            price_change_bps=-50.0  # -0.5%
        )

        # cum_delta = -10, divergence = -10 / (-50 + 1) ≈ 0.204
        assert result["cum_delta_divergence"] == pytest.approx(0.204, rel=1e-3)
        assert result["cum_delta_score"] == 1.0  # >= 0.18

    def test_default_computation(self):
        """Test the default computation function (with placeholder data)."""
        result = compute_cumulative_delta_divergence("test_token")

        # Based on placeholder data: buy=100k, sell=80k, price_change=150bps
        # divergence = (100000 - 80000) / (150 + 1) ≈ 132.45
        assert result["cum_delta_divergence"] == pytest.approx(132.45, rel=1e-3)
        assert result["cum_delta_score"] == 1.0  # >= 0.18
        assert result["cum_delta_provenance"]["data_source"] == "dune_analytics"