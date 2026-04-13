#!/usr/bin/env python3
"""
Smoke test for enhanced github_signal.py (PR-2)
Tests new 2026 metrics: velocity_score, smart_money_proxy, agentic_potential, etc.
"""
import asyncio
import sys
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.insert(0, '..')

from collectors.github_signal import calculate_enhanced_dev_activity_score, generate_coding_agent_prompt


def test_calculate_enhanced_dev_activity_score():
    """Test enhanced scoring with new 2026 metrics."""
    repo = {
        "stargazers_count": 200,
        "forks_count": 50,
        "pushed_at": "2026-04-13T10:00:00Z",
        "description": "AI agent on Solana with zk-proof and restaking",
        "topics": ["ai-agent", "solana", "zk-proof", "restaking", "eliza"],
    }

    score_data = calculate_enhanced_dev_activity_score(repo, x_mentions=10)

    assert isinstance(score_data, dict)
    assert "velocity_score" in score_data
    assert "smart_money_proxy" in score_data
    assert "agentic_potential" in score_data
    assert "x_mention_boost" in score_data
    assert "risk_adjusted_score" in score_data
    assert "actionable_for_coding_agent" in score_data
    assert score_data["velocity_score"] >= 0
    assert score_data["agentic_potential"] >= 0
    assert score_data["risk_adjusted_score"] >= 0

    print("✓ calculate_enhanced_dev_activity_score test passed")


def test_generate_coding_agent_prompt():
    """Test coding agent prompt generation."""
    candidates = [
        {
            "full_name": "test/ai-repo",
            "description": "Autonomous trading agent",
            "stargazers_count": 100,
            "forks_count": 20,
            "pushed_at": "2026-04-13T10:00:00Z",
            "risk_adjusted_score": 85.0,
            "agentic_potential": 50,
            "velocity_score": 20.0,
            "smart_money_proxy": 2,
            "extracted_contracts": ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"],
        }
    ]

    prompt = generate_coding_agent_prompt(candidates)

    assert isinstance(prompt, str)
    assert "GitHub Alpha → Coding Agent Prompt 2026" in prompt
    assert "test/ai-repo" in prompt
    assert "Risk-Adjusted Score: 85.0" in prompt
    assert "Agentic Potential: 50" in prompt
    assert "Solana Agent Kit" in prompt
    assert "reinforcement learning" in prompt

    print("✓ generate_coding_agent_prompt test passed")


async def test_enrich_with_x_signals_stub():
    """Test X enrichment stub."""
    from collectors.github_signal import enrich_with_x_signals
    mentions = await enrich_with_x_signals("test/repo")
    assert isinstance(mentions, int)
    assert mentions >= 0

    print("✓ enrich_with_x_signals stub test passed")


def test_integration():
    """Test full integration: base score → enhanced → prompt."""
    repo = {
        "stargazers_count": 150,
        "forks_count": 30,
        "pushed_at": "2026-04-13T10:00:00Z",
        "description": "Solana Agent Kit integration for DLMM agents",
        "topics": ["solana-agent-kit", "dlmm-agent", "ai-trading"],
    }

    # Simulate X mentions
    x_mentions = 25

    # Get enhanced score
    enhanced = calculate_enhanced_dev_activity_score(repo, x_mentions)

    # Generate prompt
    prompt = generate_coding_agent_prompt([enhanced])

    assert enhanced["agentic_potential"] > 0
    assert "Solana Agent Kit" in prompt
    assert "DLMM" in prompt

    print("✓ full integration test passed")


def main():
    """Run all enhanced smoke tests."""
    print("Running enhanced GitHub signal collector smoke tests (PR-2)...")

    try:
        test_calculate_enhanced_dev_activity_score()
        test_generate_coding_agent_prompt()

        # Run async test
        asyncio.run(test_enrich_with_x_signals_stub())

        test_integration()

        print("\n🎉 All enhanced smoke tests passed!")
        return 0

    except Exception as e:
        print(f"\n❌ Enhanced smoke test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())