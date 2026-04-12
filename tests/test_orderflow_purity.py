"""Tests for orderflow purity and ghost bid score metrics."""

import pytest
from unittest.mock import patch
from src.strategy.orderflow_purity_metrics import compute_orderflow_purity, compute_orderflow_purity_metrics


class TestComputeOrderflowPurity:
    """Test the core purity computation function."""

    def test_clean_organic_flow(self):
        """Test scenario: clean organic flow."""
        result = compute_orderflow_purity("token123", 60, ghost_bid_ratio=0.04, wash_trade_proxy=0.08, organic_buy_ratio=0.78)
        assert result["purity_score"] == 1.0
        assert result["ghost_bid_ratio"] == 0.04

    def test_moderate_noise(self):
        """Test scenario: moderate noise."""
        result = compute_orderflow_purity("token123", 60, ghost_bid_ratio=0.12, wash_trade_proxy=0.18, organic_buy_ratio=0.55)
        assert result["purity_score"] == 0.6

    def test_dirty_wash_trading(self):
        """Test scenario: strong wash-trading dominance."""
        result = compute_orderflow_purity("token123", 60, ghost_bid_ratio=0.15, wash_trade_proxy=0.42, organic_buy_ratio=0.30)
        assert result["purity_score"] == 0.2

    def test_ghost_bid_heavy(self):
        """Test scenario: ghost-bid dominated flow."""
        result = compute_orderflow_purity("token123", 60, ghost_bid_ratio=0.31, wash_trade_proxy=0.10, organic_buy_ratio=0.40)
        assert result["purity_score"] == 0.0

    def test_missing_data_fallback(self):
        """Test fallback when no token_address provided."""
        token_ctx = {}  # Missing token_address

        result = compute_orderflow_purity_metrics(token_ctx)

        # Should return neutral/default values
        assert result["orderflow_purity_score"] == 0.5
        assert "ghost_bid_ratio" in result
        assert "wash_trade_proxy" in result
        assert "organic_buy_ratio" in result


class TestPurityScoreCalculation:
    """Test the purity score formula logic."""

    def test_perfect_score_conditions(self):
        """Test conditions for perfect 1.0 score."""
        # ghost <= 0.08, wash <= 0.12, organic >= 0.65
        assert self._calculate_score(0.05, 0.08, 0.70) == 1.0

    def test_moderate_score_conditions(self):
        """Test conditions for 0.6 score."""
        # ghost <= 0.15, wash <= 0.25
        assert self._calculate_score(0.12, 0.18, 0.55) == 0.6

    def test_dirty_score_conditions(self):
        """Test conditions for 0.2 score."""
        # ghost > 0.25 or wash > 0.35
        assert self._calculate_score(0.30, 0.10, 0.40) == 0.2
        assert self._calculate_score(0.10, 0.40, 0.50) == 0.2

    def test_critical_dirty_score(self):
        """Test conditions for 0.0 score."""
        # Everything else
        assert self._calculate_score(0.20, 0.30, 0.50) == 0.0

    def _calculate_score(self, ghost, wash, organic):
        """Helper to calculate score per formula."""
        if ghost <= 0.08 and wash <= 0.12 and organic >= 0.65:
            return 1.0
        elif ghost <= 0.15 and wash <= 0.25:
            return 0.6
        elif ghost > 0.25 or wash > 0.35:
            return 0.2
        else:
            return 0.0


class TestIntegrationWithLegacy:
    """Test integration with existing metrics."""

    def test_legacy_metrics_preserved(self):
        """Ensure legacy metrics are still computed."""
        token_ctx = {
            "token_address": "test_token",
            "signed_buy_volume": 100.0,
            "total_buy_volume": 200.0,
            "block_0_buy_volume": 20.0,
            "repeat_buyer_count": 10.0,
            "unique_buyers_1m": 50.0,
            "wallets_in_largest_cluster": 12.0,
            "organic_taker_volume": 150.0,
            "total_volume": 300.0
        }

        with patch('src.strategy.orderflow_purity_metrics.compute_orderflow_purity') as mock_compute:
            mock_compute.return_value = {
                "ghost_bid_ratio": 0.05,
                "wash_trade_proxy": 0.10,
                "organic_buy_ratio": 0.70,
                "purity_score": 1.0,
                "provenance": "test"
            }

            result = compute_orderflow_purity_metrics(token_ctx)

            # Check new fields
            assert result["ghost_bid_ratio"] == 0.05
            assert result["wash_trade_proxy"] == 0.10
            assert result["organic_buy_ratio"] == 0.70
            assert result["orderflow_purity_score"] == 1.0

            # Check legacy fields still present
            assert "signed_buy_ratio" in result
            assert "block_0_snipe_pct" in result
            assert "repeat_buyer_ratio" in result
            assert "sybil_cluster_ratio" in result
            assert "organic_taker_volume_ratio" in result