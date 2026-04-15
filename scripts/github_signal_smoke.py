#!/usr/bin/env python3
"""
Smoke test for github_signal.py
Tests basic functionality without making real API calls.
"""
import asyncio
import sys
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.insert(0, '..')

from collectors.github_signal import calculate_dev_activity_score, build_github_text_section


def test_calculate_dev_activity_score():
    """Test scoring function with dummy repo data."""
    repo = {
        "stargazers_count": 100,
        "forks_count": 20,
        "pushed_at": "2026-04-13T10:00:00Z",
        "description": "AI agent on Solana with zk-proof",
        "topics": ["ai-agent", "solana", "zk-proof"],
    }

    score_data = calculate_dev_activity_score(repo)

    assert isinstance(score_data, dict)
    assert "dev_activity_score" in score_data
    assert "narrative_score" in score_data
    assert "reason_codes" in score_data
    assert "extracted_contracts" in score_data
    assert "scam_risk" in score_data
    assert "provenance" in score_data
    assert score_data["dev_activity_score"] >= 0
    assert score_data["dev_activity_score"] <= 100

    print("✓ calculate_dev_activity_score test passed")


def test_build_github_text_section():
    """Test text formatting with dummy candidates."""
    candidates = [
        {
            "full_name": "test/repo",
            "description": "Test AI agent repo",
            "stargazers_count": 50,
            "forks_count": 10,
            "pushed_at": "2026-04-13T10:00:00Z",
            "dev_activity_score": 75.5,
            "narrative_score": 20.0,
            "reason_codes": ["strong_2026_narrative"],
            "extracted_contracts": ["11111111111111111111111111111112"],
            "scam_risk": "low"
        }
    ]

    text = build_github_text_section(candidates)

    assert isinstance(text, str)
    assert "GitHub Alpha Signals 2026" in text
    assert "test/repo" in text
    assert "75.5/100" in text
    assert "FOUND SOLANA CONTRACTS" in text

    print("✓ build_github_text_section test passed")


async def test_search_new_repos_mock():
    """Test search function with mocked API response."""
    mock_response = {
        "items": [
            {
                "full_name": "mock/repo",
                "description": "Mock AI repo",
                "stargazers_count": 10,
                "forks_count": 2,
                "pushed_at": "2026-04-13T10:00:00Z",
                "topics": []
            }
        ]
    }

    with patch('aiohttp.ClientSession') as mock_session:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json.return_value = mock_response
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_resp

        from collectors.github_signal import search_new_repos
        repos = await search_new_repos()

        assert isinstance(repos, list)
        assert len(repos) == 1
        assert repos[0]["full_name"] == "mock/repo"

        print("✓ search_new_repos mock test passed")


def main():
    """Run all smoke tests."""
    print("Running GitHub signal collector smoke tests...")

    try:
        test_calculate_dev_activity_score()
        test_build_github_text_section()

        # Run async test
        asyncio.run(test_search_new_repos_mock())

        print("\n🎉 All smoke tests passed!")
        return 0

    except Exception as e:
        print(f"\n❌ Smoke test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())