import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from dataclasses import asdict

from collectors.jupiter_quote_client import JupiterQuoteClient, ArbQuoteResult
from collectors.jupiter_route_filter import (
    filter_by_profit_threshold,
    filter_route_labels,
    estimate_fee_lamports,
    is_viable
)
from collectors.jupiter_arb_scanner import JupiterArbScanner, ArbOpportunity


class TestJupiterQuoteClient:
    @pytest.fixture
    def client(self):
        return JupiterQuoteClient(timeout_sec=1)

    @pytest.mark.asyncio
    async def test_get_arb_quotes_success(self, client):
        """Test successful arbitrage quote retrieval"""
        mock_quote1 = {
            "outAmount": "1000000000",  # 1 SOL out
            "routePlan": [{"swapInfo": {"label": "Raydium"}}],
            "priceImpactPct": "0.5"
        }
        mock_quote2 = {
            "outAmount": "1020000000",  # 1.02 SOL out (2% profit)
            "routePlan": [{"swapInfo": {"label": "Orca"}}],
            "priceImpactPct": "0.3"
        }

        with patch('httpx.AsyncClient') as mock_client:
            mock_response1 = MagicMock()
            mock_response1.status_code = 200
            mock_response1.json.return_value = mock_quote1

            mock_response2 = MagicMock()
            mock_response2.status_code = 200
            mock_response2.json.return_value = mock_quote2

            mock_client.return_value.__aenter__.return_value.get.side_effect = [mock_response1, mock_response2]

            result = await client.get_arb_quotes(
                base_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                quote_mint="",
                amount_in=1000000000,  # 1 SOL
                slippage_bps=0
            )

            assert result.amount_in == 1000000000
            assert result.amount_out == 1020000000
            assert result.profit_lamports == 20000000  # 0.02 SOL
            assert result.profit_pct == 2.0
            assert result.is_profitable is True
            assert "Raydium -> Orca" in result.route_label

    @pytest.mark.asyncio
    async def test_get_arb_quotes_api_error(self, client):
        """Test API error handling"""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"

            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response

            with pytest.raises(Exception, match="Jupiter API error"):
                await client.get_arb_quotes("test_mint", "", 1000000000)

    @pytest.mark.asyncio
    async def test_get_arb_quotes_json_error(self, client):
        """Test JSON error response handling"""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"error": "Invalid input"}

            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response

            with pytest.raises(Exception, match="Jupiter quote1 error"):
                await client.get_arb_quotes("test_mint", "", 1000000000)


class TestJupiterRouteFilter:
    @pytest.fixture
    def profitable_result(self):
        return ArbQuoteResult(
            quote1={},
            quote2={},
            amount_in=1000000000,
            amount_out=1015000000,
            profit_lamports=15000000,
            profit_pct=1.5,
            route_label="Raydium -> Orca",
            is_profitable=True,
            price_impact_pct=0.8
        )

    @pytest.fixture
    def unprofitable_result(self):
        return ArbQuoteResult(
            quote1={},
            quote2={},
            amount_in=1000000000,
            amount_out=999000000,
            profit_lamports=-1000000,
            profit_pct=-0.1,
            route_label="Raydium -> Orca",
            is_profitable=False,
            price_impact_pct=0.8
        )

    def test_filter_by_profit_threshold_pass(self, profitable_result):
        """Test profit threshold filter - pass"""
        assert filter_by_profit_threshold(profitable_result, min_profit_pct=1.0, max_price_impact_pct=1.0) is True

    def test_filter_by_profit_threshold_fail_low_profit(self, profitable_result):
        """Test profit threshold filter - fail low profit"""
        assert filter_by_profit_threshold(profitable_result, min_profit_pct=2.0) is False

    def test_filter_by_profit_threshold_fail_high_impact(self, profitable_result):
        """Test profit threshold filter - fail high impact"""
        assert filter_by_profit_threshold(profitable_result, max_price_impact_pct=0.5) is False

    def test_filter_route_labels_pass(self, profitable_result):
        """Test route label filter - pass"""
        assert filter_route_labels(profitable_result) is True

    def test_filter_route_labels_blocked_dex(self, profitable_result):
        """Test route label filter - blocked DEX"""
        assert filter_route_labels(profitable_result, blocked_dexes=["Raydium"]) is False

    def test_filter_route_labels_allowed_dex(self, profitable_result):
        """Test route label filter - allowed DEX"""
        assert filter_route_labels(profitable_result, allowed_dexes=["Raydium", "Orca"]) is True
        assert filter_route_labels(profitable_result, allowed_dexes=["Meteora"]) is False

    def test_estimate_fee_lamports(self):
        """Test fee estimation"""
        fee = estimate_fee_lamports(1000000000, priority_fee=5000, jito_tip=10000)
        assert fee == 15000

    def test_is_viable_pass(self, profitable_result):
        """Test overall viability - pass"""
        assert is_viable(profitable_result, min_profit_pct=1.0, fee_lamports=10000000) is True

    def test_is_viable_fail_profit(self, unprofitable_result):
        """Test overall viability - fail profit"""
        assert is_viable(unprofitable_result) is False

    def test_is_viable_fail_fees(self, profitable_result):
        """Test overall viability - fail after fees"""
        assert is_viable(profitable_result, fee_lamports=20000000) is False


class TestJupiterArbScanner:
    @pytest.fixture
    def scanner(self):
        return JupiterArbScanner(
            amount_in_lamports=1000000000,
            min_profit_pct=1.0,
            max_concurrency=2,
            use_light_prescreening=False  # Disable pre-screening for unit tests
        )

    @pytest.fixture
    def mock_tokens(self):
        return [
            {"address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "symbol": "USDC"},
            {"address": "So11111111111111111111111111111111111111112", "symbol": "WSOL"}
        ]

    @pytest.mark.asyncio
    async def test_scan_tokens_success(self, scanner, mock_tokens):
        """Test successful token scanning"""
        mock_result = ArbQuoteResult(
            quote1={"routePlan": [{"swapInfo": {"label": "Raydium"}}], "priceImpactPct": "0.5"},
            quote2={"outAmount": "1020000000", "routePlan": [{"swapInfo": {"label": "Orca"}}], "priceImpactPct": "0.3"},
            amount_in=1000000000,
            amount_out=1020000000,
            profit_lamports=20000000,
            profit_pct=2.0,
            route_label="Raydium -> Orca",
            is_profitable=True,
            price_impact_pct=0.4
        )

        with patch.object(scanner.client, 'get_arb_quotes', return_value=mock_result):
            opportunities = await scanner.scan_tokens(mock_tokens)

            assert len(opportunities) == 2  # Both tokens should be viable
            assert opportunities[0].arb_score == 40.0  # min(100, 2.0 * 20)
            assert opportunities[0].symbol in ["USDC", "WSOL"]
            assert opportunities[0].profit_pct == 2.0

    @pytest.mark.asyncio
    async def test_scan_tokens_with_errors(self, scanner, mock_tokens):
        """Test scanning with some errors"""
        mock_result = ArbQuoteResult(
            quote1={},
            quote2={"outAmount": "1020000000"},
            amount_in=1000000000,
            amount_out=1020000000,
            profit_lamports=20000000,
            profit_pct=2.0,
            route_label="Raydium -> Orca",
            is_profitable=True,
            price_impact_pct=0.4
        )

        async def mock_get_quotes(base_mint, quote_mint, amount_in, **kwargs):
            if base_mint == mock_tokens[0]["address"]:
                return mock_result
            else:
                raise Exception("API Error")

        with patch.object(scanner.client, 'get_arb_quotes', side_effect=mock_get_quotes):
            opportunities = await scanner.scan_tokens(mock_tokens)

            assert len(opportunities) == 1  # Only first token should succeed
            assert opportunities[0].symbol == "USDC"

    def test_to_daily_aggregate_section_no_opportunities(self, scanner):
        """Test aggregate section with no opportunities"""
        section = scanner.to_daily_aggregate_section([])
        assert "No viable arbitrage opportunities found" in section
        assert "## JUPITER ARB SCANNER SUMMARY" in section

    def test_to_daily_aggregate_section_with_opportunities(self, scanner):
        """Test aggregate section with opportunities"""
        opportunities = [
            ArbOpportunity(
                token_address="test_addr",
                symbol="TEST",
                profit_lamports=20000000,
                profit_pct=2.0,
                route_label="Raydium -> Orca",
                price_impact_pct=0.5,
                arb_score=40.0,
                scanned_at="2024-01-01T00:00:00Z",
                raw_quote1={},
                raw_quote2={}
            )
        ]

        section = scanner.to_daily_aggregate_section(opportunities)
        assert "## JUPITER ARB SCANNER SUMMARY" in section
        assert "Total viable opportunities: 1" in section
        assert "High-confidence opportunities" in section
        assert "TEST" in section
        assert "2.00%" in section