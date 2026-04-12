#!/usr/bin/env python3
"""
Level 1-2 Token Security Runner — финальная версия
"""

from datetime import datetime

import collectors.security_checker as sc
from collectors.new_pools import get_new_tokens, filter_tokens

DEBUG = True

def main():
    print("🚀 Level 1-2 Token Security Aggregator\n")

    try:
        # Token sources (Level 3: add more sources here)
        tokens = []
        raydium_tokens = get_new_tokens()
        raydium_tokens = filter_tokens(raydium_tokens)
        raydium_tokens = [t for t in raydium_tokens if len(t) > 30 and t.strip()]
        tokens.extend(raydium_tokens)
        # Future: tokens.extend(other_source())
    except Exception as e:
        print(f"[ERROR] Failed to get new tokens: {e}")
        return

    if not tokens:
        print("No new tokens found")
        return

    print(f"🚀 New tokens detected: {len(tokens)}")

    # Prepare tokens list for processing (all Solana for now)
    token_list = [(addr, "solana") for addr in tokens]

    with open("daily_aggregate.txt", "w", encoding="utf-8") as f:
        f.write(f"TOKEN SECURITY AGGREGATE — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write("=" * 90 + "\n\n")

        for addr, chain in token_list:
            try:
                print(f"Processing token: {addr}")
                if chain == "ethereum" or addr.startswith("0x"):
                    data = sc.honeypot_check_teycir(addr, chain="ethereum")
                    risk = data.get("risk_score", 0)
                    status = data.get("status", "UNKNOWN")
                    reasons = data.get("reasons", [])
                    check_name = "HONEYPOT"
                else:
                    data = sc.run_rugwatch_token_checks("https://api.mainnet-beta.solana.com", addr)
                    score = sc.rugwatch_risk_score(data)
                    risk = score.get("risk_score", 0)
                    status = score.get("status", "UNKNOWN")
                    reasons = score.get("reasons", [])
                    check_name = "RUGWATCH"

                reason_str = " | ".join(reasons) if reasons else "No major issues"

                f.write(f"Token   : {addr}\n")
                f.write(f"Chain   : {chain.upper()}\n")
                f.write(f"Risk    : {risk}/100 | {status}\n")
                f.write(f"   → {check_name} : {reason_str}\n")
                f.write("-" * 85 + "\n\n")

                print(f"✓ {addr[:20]}... → {risk}/100 | {status}")
                if DEBUG:
                    print(f"Risk: {risk}, Label: {status}")
            except Exception as e:
                f.write(f"Token   : {addr} → ERROR: {e}\n\n")
                print(f"✗ {addr[:20]}... → ERROR")
                if DEBUG:
                    print(f"Error details: {e}")

    print(f"\n✅ Файл сохранён: daily_aggregate.txt")

if __name__ == "__main__":
    main()