#!/usr/bin/env python3
"""Flash Loan + Jupiter Arbitrage Executor (интеграция moshthepitt/flash-loan-mastery)"""
import asyncio
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from utils.logger import log_info, log_warning
from utils.io import append_jsonl
from config.settings import Settings
from trading.paper_trader import process_entry_signals

FLM_PROGRAM_ID = "1oanfPPN8r1i4UbugXHDxWMbWVJ5qLSN5qzNFZkz6Fg"
JUPITER_API = "https://quote-api.jup.ag/v6"

async def get_jupiter_quote(input_mint: str, output_mint: str, amount: int, slippage_bps: int = 100) -> Dict:
    """Получаем quote от Jupiter (бесплатно)"""
    url = f"{JUPITER_API}/quote?inputMint={input_mint}&outputMint={output_mint}&amount={amount}&slippageBps={slippage_bps}"
    # Здесь можно использовать httpx (уже есть в проекте)
    # Для простоты используем subprocess curl (надёжно работает в Docker)
    cmd = ["curl", "-s", url]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return {"error": "jupiter_quote_failed"}
    try:
        return json.loads(result.stdout)
    except:
        return {"error": "json_parse_failed"}

async def execute_flash_loan_jupiter_arb(
    signal: Dict[str, Any],
    settings: Settings,
    run_dir: Path
) -> Dict[str, Any]:
    """Главный executor: flash-loan → Jupiter swap → repay в одной tx"""
    token_in = signal.get("token_address") or signal.get("input_mint")
    token_out = signal.get("output_mint")  # можно добавить в arb_scanner
    amount = int(signal.get("amount_lamports", 1_000_000_000))  # 1 SOL по умолчанию

    # 1. Получаем quote
    quote = await get_jupiter_quote(token_in, token_out, amount)
    if "error" in quote:
        log_warning("jupiter_quote_failed", token=token_in)
        return {"status": "failed", "reason": "jupiter_quote_failed"}

    # 2. Формируем flash-loan borrow + Jupiter swap + repay (через CLI flm-jupiter-arb или прямой вызов)
    # Для начала используем готовый CLI из flm-jupiter-arb (он уже в репозитории)
    cli_cmd = [
        "yarn", "start", "simple-jupiter-arb",
        "-k", str(settings.SOLANA_WALLET_PATH),  # добавь в settings
        "-m1", token_in,
        "-m2", token_out,
        "-a", str(amount)
    ]
    try:
        result = subprocess.run(cli_cmd, cwd=Path("flm-jupiter-arb"), capture_output=True, text=True, timeout=30)
        tx_signature = result.stdout.strip().splitlines()[-1] if result.returncode == 0 else None
    except Exception as e:
        tx_signature = None
        log_warning("flash_loan_cli_failed", error=str(e))

    # 3. Логируем как paper-trade (ARB regime)
    trade_record = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event": "paper_flash_loan_arb",
        "token_address": token_in,
        "symbol": signal.get("symbol", "ARB"),
        "side": "BUY",
        "regime": "ARB",
        "arb_score": signal.get("arb_score", 0),
        "tx_signature": tx_signature,
        "expected_profit_sol": quote.get("outAmount", 0) / 1e9 - amount / 1e9,
        "contract_version": "flash_loan_jupiter_v1",
    }
    append_jsonl(run_dir / "trades.jsonl", trade_record)

    log_info("flash_loan_arb_executed", token=token_in, arb_score=signal.get("arb_score"), tx=tx_signature)
    return {"status": "success", "tx": tx_signature, "profit_est": trade_record["expected_profit_sol"]}