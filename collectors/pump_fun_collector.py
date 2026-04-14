import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
import websockets
import base64

logger = logging.getLogger(__name__)

from utils.io import append_jsonl, write_json, ensure_dir
from utils.rate_limit import async_acquire
from utils.retry import with_retry
from config.settings import load_settings

settings = load_settings()
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfX3lLzQ1N2e4vK5"
HELIUS_WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={settings.HELIUS_API_KEY}"

# Успешные метрики 2026 (на основе реальных данных Pump.fun graduates)
MIN_MARKET_CAP = 35_000          # начинаем смотреть отсюда
GRADUATION_THRESHOLD = 90.0      # % до миграции на Raydium
HIGH_VELOCITY_THRESHOLD = 25.0   # сильный buy pressure

SCAM_FILTERS = ["dev", "team", "tax", "renounced", "locked", "burned"]  # косвенные признаки


async def fetch_pump_fun_coins() -> List[Dict]:
    """Fetch recent pump.fun tokens via Helius WebSocket logs monitoring."""
    await async_acquire("dex")

    collected_tokens = []
    seen_mints = set()

    async def subscribe_and_listen():
        backoff = 1  # initial backoff in seconds
        max_backoff = 60
        while True:
            try:
                async with websockets.connect(HELIUS_WS_URL) as websocket:
                    # Subscribe to logs for Pump.fun program
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [PUMP_FUN_PROGRAM_ID]},
                            {"commitment": "confirmed"}
                        ]
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    logger.info("Subscribed to Pump.fun program logs via Helius WebSocket")

                    # Listen for messages
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            if "params" in data and "result" in data["params"]:
                                logs = data["params"]["result"]["value"]["logs"]
                                signature = data["params"]["result"]["value"]["signature"]

                                # Parse logs for InitializeMint or Create events
                                mint_address = parse_pump_fun_logs(logs)
                                if mint_address and mint_address not in seen_mints:
                                    seen_mints.add(mint_address)
                                    token_data = {
                                        "mint": mint_address,
                                        "symbol": "UNKNOWN",  # Will be filled later if possible
                                        "name": "Pump.fun Token",
                                        "usd_market_cap": 0,  # Initial low
                                        "virtual_sol_reserves": 0,
                                        "reply_count": 0,
                                        "last_trade_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                                        "buy_pressure": 0,
                                        "v24hUSD": 0,
                                        "unique_buyers_1h": 0,
                                        "holder_count": 0,
                                        "dev_sell_pressure_5m": 0,
                                        "top10_holder_pct": 0.65
                                    }
                                    collected_tokens.append(token_data)
                                    logger.info(f"New Pump.fun token detected: {mint_address}")

                                    # Limit to recent tokens, e.g., last 50
                                    if len(collected_tokens) >= 50:
                                        break

                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse WS message: {e}")
                            continue

                    # Reset backoff on successful connection
                    backoff = 1

            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
                logger.warning(f"WebSocket connection lost: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)  # exponential backoff
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    # Run the listener for a short time to collect recent tokens
    try:
        await asyncio.wait_for(subscribe_and_listen(), timeout=30)  # 30 seconds timeout
    except asyncio.TimeoutError:
        logger.info("WS monitoring timeout reached, returning collected tokens")

    return collected_tokens


def parse_pump_fun_logs(logs: List[str]) -> str:
    """Parse Solana logs to extract new token mint addresses from Pump.fun events."""
    for log in logs:
        if "InitializeMint" in log or "Create" in log:
            # Pump.fun typically creates tokens with specific instruction data
            # Look for mint address in the logs or instruction data
            # This is a simplified parser; in reality, you'd decode the instruction data
            # For now, assume the mint is mentioned in logs
            if "mint" in log.lower():
                # Extract mint address - this is placeholder logic
                # Real implementation would decode base64 instruction data
                parts = log.split()
                for part in parts:
                    if len(part) == 44 and part.replace('_', '').isalnum():  # Base58 check approx
                        return part
    return None


def calculate_graduation_score(coin: Dict[str, Any]) -> Dict[str, Any]:
    """Улучшенный scoring на основе реальных предикторов успеха после graduation."""
    market_cap = float(coin.get("usd_market_cap") or 0)
    virtual_sol_reserves = float(coin.get("virtual_sol_reserves") or 0)
    reply_count = int(coin.get("reply_count") or 0)
    last_trade_timestamp = coin.get("last_trade_timestamp")

    # Bonding curve progress
    curve_progress_pct = min((market_cap / 69_000) * 100, 100.0)

    # Buy pressure velocity (самая важная метрика успеха)
    buy_pressure = float(coin.get("buy_pressure") or coin.get("v24hUSD") or 0)
    unique_buyers = int(coin.get("unique_buyers_1h") or coin.get("holder_count", 0) - 1)

    # Acceleration proxy (сколько уникальных покупателей за последний час)
    buyer_velocity = unique_buyers / max(1, (datetime.now(timezone.utc).timestamp() - int(last_trade_timestamp or 0)) / 3600)

    # Dev sell pressure (критично низкий = хороший знак)
    dev_sell_pressure = float(coin.get("dev_sell_pressure_5m") or 0)

    # Holder distribution quality
    holder_concentration = float(coin.get("top10_holder_pct") or 0.65)  # чем ниже — тем лучше

    # Основной score
    base_score = curve_progress_pct * 0.4

    if buyer_velocity > HIGH_VELOCITY_THRESHOLD:
        base_score += 35
    if unique_buyers > 40 and curve_progress_pct > 70:
        base_score += 25
    if dev_sell_pressure < 0.03:
        base_score += 20
    if holder_concentration < 0.45:
        base_score += 15

    # Штрафы
    if dev_sell_pressure > 0.15:
        base_score -= 30
    if holder_concentration > 0.75:
        base_score -= 25

    risk_adjusted_score = round(max(10, min(100, base_score)), 2)

    is_graduating = curve_progress_pct >= GRADUATION_THRESHOLD
    is_high_potential = risk_adjusted_score >= 75 and is_graduating

    return {
        "market_cap": round(market_cap, 0),
        "curve_progress_pct": round(curve_progress_pct, 1),
        "buyer_velocity": round(buyer_velocity, 2),
        "unique_buyers_1h": unique_buyers,
        "dev_sell_pressure_5m": round(dev_sell_pressure, 4),
        "holder_concentration": round(holder_concentration, 3),
        "risk_adjusted_graduation_score": risk_adjusted_score,
        "is_graduating": is_graduating,
        "is_high_potential": is_high_potential,
        "graduation_reason_codes": [
            "strong_buyer_velocity" if buyer_velocity > HIGH_VELOCITY_THRESHOLD else "",
            "low_dev_sell" if dev_sell_pressure < 0.05 else "",
            "healthy_distribution" if holder_concentration < 0.50 else ""
        ],
        "provenance": "pump_fun_graduation_tracker_2026"
    }


async def get_recent_pools(limit: int = 50) -> List[str]:
    """Get list of recent pump.fun token addresses"""
    high_potential = await run_pump_fun_graduation_tracker(limit)
    return [token['token_address'] for token in high_potential]


async def run_pump_fun_graduation_tracker(max_tokens: int = 50) -> List[Dict]:
    """Основная функция трекера."""
    coins = await fetch_pump_fun_coins()
    seen_tokens = set()
    high_potential_tokens = []

    for coin in coins:
        mint = coin.get("mint")
        if not mint or mint in seen_tokens:
            continue
        seen_tokens.add(mint)

        symbol = coin.get("symbol", "UNKNOWN")
        market_cap = float(coin.get("usd_market_cap") or 0)

        if market_cap < MIN_MARKET_CAP:
            continue

        score_data = calculate_graduation_score(coin)

        token_data = {
            "token_address": mint,
            "symbol": symbol,
            "name": coin.get("name", ""),
            **score_data,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": "pump_fun",
            "raydium_migration_expected": score_data["is_graduating"]
        }

        if score_data["is_high_potential"]:
            print(f"🚀 HIGH POTENTIAL GRADUATE: {symbol} | Score: {score_data['risk_adjusted_graduation_score']} | MC: ${market_cap:,.0f}")
            high_potential_tokens.append(token_data)

        # Сохраняем все graduating токены
        if score_data["is_graduating"]:
            append_jsonl(Path("data/processed/pump_fun_graduating.jsonl"), token_data)

    # Сохраняем топ для агрегатора и coding agent
    write_json(Path("data/processed/pump_fun_high_potential.json"), {
        "as_of": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "high_potential_count": len(high_potential_tokens),
        "tokens": sorted(high_potential_tokens, key=lambda x: x["risk_adjusted_graduation_score"], reverse=True)[:20]
    })

    return high_potential_tokens


def generate_pump_fun_coding_agent_prompt(high_potential: List[Dict]) -> str:
    """Максимально полезный промпт для coding agent."""
    lines = [
        "# Pump.Fun Graduation → Autonomous Sniper / DLMM Agent Prompt 2026",
        "Ты — Solana AI Trading Agent. Используй данные ниже для создания/улучшения автономного агента.\n",
        "Фокус: Ловить токены за 5–15 минут до или сразу после миграции на Raydium с высоким buyer velocity.\n"
    ]

    for token in high_potential[:10]:
        score = token["risk_adjusted_graduation_score"]
        lines.append(f"## HIGH POTENTIAL: {token['symbol']} ({token['token_address']})")
        lines.append(f"Graduation Score: {score}/100 | Curve: {token['curve_progress_pct']}% | Buyer Velocity: {token.get('buyer_velocity', 0)}")
        lines.append(f"Dev Sell: {token.get('dev_sell_pressure_5m', 0)} | Holders Concentration: {token.get('holder_concentration', 0)}")
        lines.append("Рекомендации для агента:")
        lines.append("1. Мониторить этот mint через Helius WebSocket")
        lines.append("2. При curve_progress > 92% и buyer_velocity > 25 → готовить Jito bundle на покупку")
        lines.append("3. После миграции на Raydium: проверить liquidity depth и открыть позицию с evidence-weighted sizing")
        lines.append("4. Exit rules: trailing stop + dev-sell monitor + MFE capture\n")

    lines.append("\nЗадача: Напиши код агента, который автоматически:")
    lines.append("- Подключается к Pump.fun + Raydium")
    lines.append("- Использует velocity scoring из PR-2")
    lines.append("- Интегрируется с GitHub Velocity Tracker (PR-3)")
    lines.append("- Применяет continuation_enricher из основной системы")
    lines.append("- Логирует все решения с provenance")

    return "\n".join(lines)