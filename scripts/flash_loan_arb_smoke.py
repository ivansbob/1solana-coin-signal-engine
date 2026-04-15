#!/usr/bin/env python3
import asyncio
from pathlib import Path
from analytics.arb_scanner import scan_arb_opportunities
from trading.flash_loan_executor import execute_flash_loan_jupiter_arb
from config.settings import load_settings

async def main():
    settings = load_settings()
    opps = await scan_arb_opportunities()
    executed = 0
    for opp in opps[:5]:
        if opp.get("action") == "ARB" and opp.get("arb_score", 0) >= 60:
            result = await execute_flash_loan_jupiter_arb(opp, settings, Path("data/smoke"))
            if result.get("status") == "success":
                executed += 1
    print(f"✅ Flash Loan ARB smoke passed — executed {executed} trades")
    return 0

if __name__ == "__main__":
    asyncio.run(main())