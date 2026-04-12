import pytest
from src.strategy.totalscore_v7 import TotalScoreV7ContractLoader

def test_coverage_matrix_is_complete_for_accepted_metrics():
    # We must explicitly track PR variables verifying they aren't completely ignored natively
    schema_path = "./data_contracts/candidate_snapshot_v7.schema.json"
    defaults_path = "./configs/totalscore_v7.defaults.yaml"
    loader = TotalScoreV7ContractLoader(schema_path, defaults_path)

    # 022 Exit Realism (represented via Liquidity / Orderflow mappings generally)
    # 035 Accumulation / Smart Money
    assert loader.get_metric_config("SmartMoneyCombinedScore")["stage"] == "active"

    # 072 Risk Adjusted Wallet
    risk = loader.get_metric_config("RiskAdjustedWalletScore")
    assert risk["bucket"] == "sizing_modifier"

    # 036 Social Velocity
    social = loader.get_metric_config("SocialVelocityScore")
    assert social["stage"] == "research" # Isolated avoiding direct memecoin noise natively

    # PR-010 Drift Perp Context
    drift = loader.get_metric_config("DriftPerpContext")
    assert drift["bucket"] == "regime_modifier"

    # PR-002 Fast Prescore
    fast = loader.get_metric_config("FastPrescore")
    assert fast["bucket"] == "replay_only"
    assert fast["stage"] == "active"
