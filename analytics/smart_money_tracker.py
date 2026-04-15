import asyncio
import httpx
import json
import os
from typing import List, Dict, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Load smart wallets
SMART_WALLETS = set()
try:
    with open("data/registry/smart_wallets.json", 'r') as f:
        SMART_WALLETS = set(json.load(f))
except FileNotFoundError:
    logger.warning("smart_wallets.json not found")

try:
    with open("data/registry/hot_wallets.json", 'r') as f:
        hot = json.load(f)
        SMART_WALLETS.update(hot.keys() if isinstance(hot, dict) else hot)
except FileNotFoundError:
    pass

@dataclass
class SmartMoneySignal:
    token_mint: str
    smart_wallet_hits: int
    smart_wallet_addresses: List[str]
    total_smart_volume_sol: float
    smart_inflow_score: float
    insider_flag: bool
    first_smart_buy_age_minutes: int

async def check_smart_money_inflow(token_mint: str, recent_transactions: List[Dict[str, Any]]) -> SmartMoneySignal:
    """
    Check recent transactions for smart wallet inflows.
    """
    smart_buys = []
    total_buys = 0
    first_buy_age = float('inf')

    for tx in recent_transactions[:100]:  # Check last 100 tx
        if tx.get("type") == "SWAP":
            buyer = tx.get("buyer") or tx.get("signer")
            if buyer in SMART_WALLETS:
                amount_sol = tx.get("amount_sol", 0)
                smart_buys.append({"address": buyer, "volume": amount_sol, "age": tx.get("age_minutes", 0)})
                first_buy_age = min(first_buy_age, tx.get("age_minutes", 0))
        if tx.get("type") in ["SWAP", "BUY"]:
            total_buys += 1

    smart_wallet_hits = len(smart_buys)
    total_smart_volume = sum(b["volume"] for b in smart_buys)
    smart_inflow_score = (smart_wallet_hits / max(total_buys, 1)) * 10  # 0-10 scale

    insider_flag = smart_wallet_hits > 3 and first_buy_age < 5

    return SmartMoneySignal(
        token_mint=token_mint,
        smart_wallet_hits=smart_wallet_hits,
        smart_wallet_addresses=[b["address"] for b in smart_buys],
        total_smart_volume_sol=round(total_smart_volume, 2),
        smart_inflow_score=round(smart_inflow_score, 2),
        insider_flag=insider_flag,
        first_smart_buy_age_minutes=int(first_buy_age) if first_buy_age != float('inf') else 0
    )

# Note: In real implementation, fetch transactions from Helius if not provided
# async def fetch_recent_transactions(token_mint: str) -> List[Dict[str, Any]]:
#     # Use Helius API
#     pass