"""
Level 1-2 Token Security Aggregator
Teycir Honeypot (EVM) + RugWatch (Solana)
"""

from datetime import datetime
from typing import Dict, Any, List, Union

from .security_checker import (
    honeypot_check_teycir,
    run_rugwatch_token_checks,
    rugwatch_risk_score
)

def aggregate_token_checks(
    token: Union[str, Dict],
    rpc_url: str = "https://api.mainnet-beta.solana.com",
    etherscan_key: str = ""
) -> Dict[str, Any]:
    """Проверка одного токена"""
    if isinstance(token, str):
        address = token
        chain = "ethereum" if token.startswith("0x") else "solana"
    else:
        address = token.get("address", str(token))
        chain = token.get("chain", "solana")

    if chain.lower() == "solana":
        raw = run_rugwatch_token_checks(rpc_url, address)
        score = rugwatch_risk_score(raw)
        checks = {"rugwatch": score}
        final_score = score.get("risk_score", 0)
        status = score.get("status", "UNKNOWN")
    else:
        hp = honeypot_check_teycir(address, chain=chain, etherscan_api_key=etherscan_key)
        checks = {"honeypot": hp}
        final_score = hp.get("risk_score", 0)
        status = hp.get("status", "UNKNOWN")

    return {
        "timestamp": datetime.now().isoformat(),
        "token": address,
        "chain": chain.upper(),
        "final_risk_score": final_score,
        "overall_status": status,
        "checks": checks
    }

def save_daily_aggregate(
    tokens: List[Union[str, Dict]],
    filename: str = "daily_aggregate.txt",
    etherscan_key: str = ""
):
    """Финальная версия daily aggregate"""
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"TOKEN SECURITY AGGREGATE — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write("=" * 90 + "\n\n")

        for item in tokens:
            try:
                data = aggregate_token_checks(item, etherscan_key=etherscan_key)
                
                f.write(f"Token   : {data['token']}\n")
                f.write(f"Chain   : {data['chain']}\n")
                f.write(f"Risk    : {data['final_risk_score']:2d}/100   |   {data['overall_status']}\n")
                
                for name, check in data["checks"].items():
                    reasons = check.get("reasons", [])
                    reason_str = " | ".join(reasons) if reasons else "Clean"
                    f.write(f"   → {name.upper():<10} : {reason_str}\n")
                
                f.write("-" * 85 + "\n\n")
            except Exception as e:
                f.write(f"Token   : {item}   →   ERROR: {e}\n\n")

    print(f"✅ Агрегат сохранён → {filename} ({len(tokens)} токенов)")

if __name__ == "__main__":
    test_tokens = [
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    ]
    
    save_daily_aggregate(test_tokens)