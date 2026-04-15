from typing import Dict, Any, List, Optional, Set
from .jupiter_quote_client import ArbQuoteResult

# Ported from src/constant/index.ts dexLabel keys
KNOWN_DEX_LABELS: Set[str] = {
    "Raydium",
    "Orca",
    "Meteora",
    "Lifinity",
    "Aldrin",
    "Crema",
    "Cropper",
    "Invariant",
    "Mercurial",
    "Openbook",
    "Phoenix",
    "Saber",
    "Saros",
    "Step Finance",
    "Whirlpool",
    "Jupiter",
    "Dexlab",
    "Serum",
    "BonkSwap",
    "Pump.fun",
    "Moonshot",
    "FluxBeam",
    "GuacSwap",
    "Perps",
    "Drift",
    "GooseFX",
    "Sencha",
    "Balansol",
    "Clone",
    "Cykura",
    "Symmetry",
    "Sunny",
    "Unknown DEX"
}


def filter_by_profit_threshold(result: ArbQuoteResult, min_profit_pct: float = 0.15, max_price_impact_pct: float = 1.0) -> bool:
    """Filter by minimum profit percentage and maximum price impact"""
    return result.profit_pct >= min_profit_pct and result.price_impact_pct <= max_price_impact_pct


def filter_route_labels(result: ArbQuoteResult, allowed_dexes: Optional[List[str]] = None, blocked_dexes: Optional[List[str]] = None) -> bool:
    """Filter by DEX allowlist/blocklist"""
    if allowed_dexes:
        allowed_dexes_set = set(allowed_dexes)
        for dex in KNOWN_DEX_LABELS:
            if dex in result.route_label and dex not in allowed_dexes_set:
                return False

    if blocked_dexes:
        blocked_dexes_set = set(blocked_dexes)
        for dex in blocked_dexes_set:
            if dex in result.route_label:
                return False

    return True


def estimate_fee_lamports(amount_in: int, priority_fee: int = 5000, jito_tip: int = 10000) -> int:
    """Estimate total fees in lamports"""
    return priority_fee + jito_tip


def is_viable(
    result: ArbQuoteResult,
    min_profit_pct: float = 0.15,
    max_price_impact_pct: float = 1.0,
    fee_lamports: int = 15000
) -> bool:
    """Check if arbitrage opportunity is viable after fees"""
    # Apply profit threshold filter
    if not filter_by_profit_threshold(result, min_profit_pct, max_price_impact_pct):
        return False

    # Apply route label filter (default no restrictions)
    if not filter_route_labels(result):
        return False

    # Check profit after fees
    net_profit = result.profit_lamports - fee_lamports
    return net_profit > 0