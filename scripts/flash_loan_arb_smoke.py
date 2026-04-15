#!/usr/bin/env python3
import asyncio
from analytics.arb_scanner import scan_arb_opportunities
from trading.flash_loan_executor import execute_flash_loan_jupiter_arb
from config.settings import load_settings

async def main():
    settings = load_settings()
    opps = await scan_arb_opportunities()
    for opp in opps[:3]:
        if opp.get("arb_score", 0) >= 60:
            await execute_flash_loan_jupiter_arb(opp, settings, Path("data/smoke"))
    print("✅ Flash Loan ARB smoke passed")

if __name__ == "__main__":
    asyncio.run(main())