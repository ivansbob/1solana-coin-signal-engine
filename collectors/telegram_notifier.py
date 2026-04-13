import asyncio
import httpx
import json
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Load from env
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ALERTS_ENABLED = os.getenv("TELEGRAM_ALERTS_ENABLED", "false").lower() == "true"
MIN_SPREAD_PCT = float(os.getenv("TELEGRAM_MIN_SPREAD_PCT", "0.3"))

async def send_arb_alert(opportunity: Dict[str, Any]) -> bool:
    """
    Send alert for viable arb opportunities.
    """
    if not ALERTS_ENABLED or not BOT_TOKEN or not CHAT_ID:
        return False

    if (not opportunity.get("viable") or
        opportunity.get("net_spread_pct", 0) < MIN_SPREAD_PCT or
        opportunity.get("mev_risk") == "high"):
        return False

    message = f"""🟢 ARB SIGNAL — {opportunity.get('symbol', 'UNKNOWN')}

💰 Spread: {opportunity.get('net_spread_pct', 0):.2f}% net
📊 Buy: {opportunity.get('buy_dex')} @ ${opportunity.get('buy_price', 0):.6f}
📈 Sell: {opportunity.get('sell_dex')} @ ${opportunity.get('sell_price', 0):.6f}
💧 Liquidity: ${opportunity.get('liquidity_buy_side', 0):,.0f}
⚡ Max position: {opportunity.get('max_position_sol', 0):.2f} SOL
🧠 Smart wallets: {opportunity.get('smart_wallet_hits', 0)}
⏰ Pool age: {opportunity.get('age_minutes', 0)} min
🔗 https://dexscreener.com/solana/{opportunity.get('token_mint')}

Risk: {opportunity.get('mev_risk', 'unknown')} | Verdict: ARB_HIGH
"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        logger.info(f"Sent Telegram alert for {opportunity.get('symbol')}")
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False

async def send_daily_summary(summary: Dict[str, Any]) -> bool:
    """Send daily summary"""
    if not ALERTS_ENABLED:
        return False

    message = f"""=== DAILY SUMMARY ===
Scanned: {summary.get('total', 0)} tokens
Passed security: {summary.get('safe', 0)}
ARB_HIGH: {summary.get('arb_high', 0)}
ARB_MEDIUM: {summary.get('arb_medium', 0)}
Smart money hits: {summary.get('sm_hits', 0)}
Cross-chain opportunities: {summary.get('cc_arb', 0)}
Best opportunity: {summary.get('best_symbol', 'N/A')} @ {summary.get('best_spread', 0):.2f}% spread
"""

    # Same send logic
    return await send_arb_alert({"summary": message})  # Simplified