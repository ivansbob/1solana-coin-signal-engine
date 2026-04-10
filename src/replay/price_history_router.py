"""Orchestrates pulling from Jupiter, Pyth, Gecko, DexScreener naturally resolving bounds safely."""

from typing import Dict, Any
from src.collectors.price_history_client import PriceHistoryClient
from src.replay.price_path_resolver import resolve_price_path
from src.strategy.types import PricePathEvidence

class PriceHistoryRouter:
    def __init__(self, client: PriceHistoryClient):
        self.client = client
        
    def fetch_best_price_path(self, token_address: str, timestamp_sec: int) -> PricePathEvidence:
        """
        Attempts hierarchical loading explicitly:
        1. Jupiter
        2. Pyth
        3. GeckoTerminal 
        4. DexScreener
        """
        # Jupiter Executions
        data = self.client.fetch_jupiter_quote(token_address, timestamp_sec)
        if data: return resolve_price_path(data, timestamp_sec, "jupiter")
        
        # Pyth
        data = self.client.fetch_pyth_hermes(token_address, timestamp_sec)
        if data: return resolve_price_path(data, timestamp_sec, "pyth")
            
        # Gecko 
        if "geckoterminal" in self.client.rate_limited_sources:
            # If we know rate_limited, try next
            pass
        else:
            data = self.client.fetch_geckoterminal_ohlcv(token_address, timestamp_sec)
            if data: return resolve_price_path(data, timestamp_sec, "geckoterminal")
            
        # DexScreener Match
        data = self.client.fetch_dexscreener_proxy(token_address, timestamp_sec)
        if data: return resolve_price_path(data, timestamp_sec, "dexscreener")
        
        # We failed entirely to resolve natural layers. Let backfill attempt later.
        if "geckoterminal" in self.client.rate_limited_sources:
            return {
                "price_path_status": "rate_limited",
                "price_path_source": "geckoterminal_blocked",
                "price_path_confidence": 0.0,
                "gap_size_sec": -1,
                "backfill_applied": False,
                "price_path_diagnostic": "rate_limits_exhausted_all_proxy_options"
            }
            
        return resolve_price_path(None, timestamp_sec, "exhausted_all_routers")
