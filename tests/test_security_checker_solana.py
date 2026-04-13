import pytest
import unittest.mock as mock
from collectors.security_checker import check_token, rugwatch_risk_score, run_rugwatch_token_checks


@pytest.mark.asyncio
async def test_risk_score_clean_token():
    # Mock clean token data
    mock_data = {
        "has_mint_authority": False,
        "has_freeze_authority": False,
        "decimals": 9
    }
    result = rugwatch_risk_score(mock_data, "raydium")
    assert result["risk_score"] <= 10
    assert result["verdict"] == "PASS"


@pytest.mark.asyncio
async def test_risk_score_high_risk():
    # Mock high risk token
    mock_data = {
        "has_mint_authority": True,
        "has_freeze_authority": True,
        "decimals": 9
    }
    result = rugwatch_risk_score(mock_data, "pumpfun")
    assert result["risk_score"] >= 70
    assert result["verdict"] == "BLOCK"


@pytest.mark.asyncio
async def test_risk_score_warn():
    # Mock warn token
    mock_data = {
        "has_mint_authority": True,
        "has_freeze_authority": False,
        "decimals": 9
    }
    result = rugwatch_risk_score(mock_data, "unknown")
    assert 30 <= result["risk_score"] < 70
    assert result["verdict"] == "WARN"


@pytest.mark.asyncio
async def test_honeypot_no_sells():
    from collectors.honeypot_simulator import simulate_solana_honeypot

    # Mock DexScreener response
    mock_response = {
        "pairs": [{
            "txns": {
                "h1": {
                    "buys": 50,
                    "sells": 0
                }
            }
        }]
    }

    with mock.patch('httpx.AsyncClient') as mock_client:
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = mock_response
        mock_client.return_value.__aenter__.return_value.get.return_value = mock_resp

        result = await simulate_solana_honeypot("test_mint")
        assert result["is_honeypot"] is True
        assert result["confidence"] >= 0.8


@pytest.mark.asyncio
async def test_check_token_whitelist():
    # Test whitelisted token (USDC)
    result = await check_token("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    assert result["safe"] is True
    assert result["status"] == "WHITELISTED"
    # Should not call RPC
    # Assuming no external calls for whitelist</content>
<parameter name="filePath">tests/test_security_checker_solana.py