#!/usr/bin/env python3
"""Aggregate free data pipeline with flash loan execution."""

import asyncio
from pathlib import Path
from typing import Dict, Any

from analytics.arb_scanner import scan_arb_opportunities
from config.settings import load_settings

async def main():
    settings = load_settings()
    output_dir = Path("data/processed")

    # Assume collected_data is built here
    collected_data: Dict[str, Any] = {}
    # JSON structure includes: "arb_score": number, "flash_loan_ready": bool, "action": "ARB" | ...

    # Scan arb opportunities
    arb_opportunities = await scan_arb_opportunities()
    collected_data["arb_opportunities"] = arb_opportunities

    # ====================== FLASH-LOAN EXECUTOR ======================
    from trading.flash_loan_executor import execute_flash_loan_jupiter_arb
    executed = []
    for opp in collected_data["arb_opportunities"]:
        if opp.get("action") == "ARB" and opp.get("arb_score", 0) >= 60:
            result = await execute_flash_loan_jupiter_arb(opp, settings, output_dir)
            executed.append(result)
    print(f"Executed {len(executed)} flash-loan arbs")
    collected_data["flash_loan_executions"] = executed

if __name__ == "__main__":
    asyncio.run(main())