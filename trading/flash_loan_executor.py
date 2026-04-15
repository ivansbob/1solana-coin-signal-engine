#!/usr/bin/env python3
"""Flash Loan + Jupiter Arbitrage Executor (интеграция moshthepitt/flash-loan-mastery)"""
import asyncio
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone

from utils.logger import log_info, log_warning
from utils.io import append_jsonl
from config.settings import Settings
from trading.paper_trader import process_entry_signals  # для совместимости


FLM_PROGRAM_ID = "1oanfPPN8r1i4UbugXHDxWMbWVJ5qLSN5qzNFZkz6Fg"
JUPITER_QUOTE_API = "https://quote-api.jup.ag/v6/quote"
FLM_CLI_PATH = Path("flm-jupiter-arb")  # путь к папке с CLI из репозитория moshthepitt


async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    slippage_bps: int = 150,
) -> Dict[str, Any]:
    """Получаем свежий quote от Jupiter (бесплатно)"""
    url = f"{JUPITER_QUOTE_API}?inputMint={input_mint}&outputMint={output_mint}&amount={amount_lamports}&slippageBps={slippage_bps}"
    try:
        # используем curl (уже есть в окружении, надёжно)
        cmd = ["curl", "-s", "-L", url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            return {"error": "jupiter_network_error"}
        data = json.loads(result.stdout)
        if "error" in data:
            return {"error": data.get("error")}
        return data
    except Exception as e:
        log_warning("jupiter_quote_exception", error=str(e))
        return {"error": "jupiter_quote_failed"}


async def execute_flash_loan_jupiter_arb(
    signal: Dict[str, Any],
    settings: Settings,
    run_dir: Path,
) -> Dict[str, Any]:
    """Главный executor: flash-loan → Jupiter swap → repay в одной транзакции"""
    token_in = str(signal.get("token_address") or signal.get("input_mint") or "")
    token_out = str(signal.get("output_mint") or "")
    amount = int(signal.get("amount_lamports") or signal.get("size_sol", 0) * 1_000_000_000 or 1_000_000_000)

    if not token_out:
        # если output_mint не указан — используем USDC как default
        token_out = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    log_info("flash_loan_arb_started", token_in=token_in, token_out=token_out, amount=amount)

    # 1. Получаем quote
    quote = await get_jupiter_quote(token_in, token_out, amount)
    if "error" in quote:
        log_warning("flash_loan_arb_quote_failed", token=token_in)
        return {"status": "failed", "reason": quote["error"]}

    expected_out = int(quote.get("outAmount", 0))
    estimated_profit = (expected_out - amount) / 1_000_000_000

    # 2. Запускаем готовый CLI из flm-jupiter-arb (самый простой и надёжный способ)
    cli_cmd = [
        "yarn", "start", "simple-jupiter-arb",
        "-k", str(getattr(settings, "SOLANA_WALLET_PATH", "~/.config/solana/id.json")),
        "-m1", token_in,
        "-m2", token_out,
        "-a", str(amount),
    ]

    try:
        result = subprocess.run(
            cli_cmd,
            cwd=FLM_CLI_PATH,
            capture_output=True,
            text=True,
            timeout=45,
        )
        tx_signature = None
        for line in result.stdout.splitlines():
            if line.startswith("https://solscan.io/tx/") or "tx/" in line:
                tx_signature = line.strip().split("/")[-1]
                break
        success = result.returncode == 0 and tx_signature
    except Exception as e:
        success = False
        tx_signature = None
        log_warning("flash_loan_cli_exception", error=str(e))

    # 3. Логируем как полноценный ARB-трейд
    trade_record = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event": "paper_flash_loan_arb",
        "token_address": token_in,
        "symbol": signal.get("symbol", "ARB"),
        "side": "BUY",
        "regime": "ARB",
        "arb_score": float(signal.get("arb_score", 0)),
        "tx_signature": tx_signature,
        "expected_profit_sol": round(estimated_profit, 6),
        "status": "success" if success else "failed",
        "contract_version": "flash_loan_jupiter_v1",
        "flash_loan_program": FLM_PROGRAM_ID,
    }

    append_jsonl(run_dir / "trades.jsonl", trade_record)

    if success:
        log_info(
            "flash_loan_arb_executed",
            token=token_in,
            arb_score=signal.get("arb_score"),
            profit_est=estimated_profit,
            tx=tx_signature,
        )
    else:
        log_warning("flash_loan_arb_failed", token=token_in, reason="cli_execution_failed")

    return trade_record