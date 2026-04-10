"""Core simulation parameters abstracting API fetches for historical quotes across different sources."""

import random
from typing import Dict, Any, Optional

class PriceHistoryClient:
    """Mock executing simulated queries tracking rate limits across nodes."""
    
    def __init__(self):
        self.rate_limited_sources = set()

    def fetch_jupiter_quote(self, token_address: str, timestamp_sec: int) -> Optional[Dict[str, Any]]:
        if "jupiter" in self.rate_limited_sources:
            return None
        # Mocking finding quote occasionally missing
        if random.random() < 0.2:
            return None
        return {"price": 1.5, "source": "jupiter", "timestamp": timestamp_sec}

    def fetch_pyth_hermes(self, token_address: str, timestamp_sec: int) -> Optional[Dict[str, Any]]:
        if "pyth" in self.rate_limited_sources:
            return None
        if random.random() < 0.3:
            return None
        return {"price": 1.51, "source": "pyth", "timestamp": timestamp_sec}

    def fetch_geckoterminal_ohlcv(self, token_address: str, timestamp_sec: int) -> Optional[Dict[str, Any]]:
        # Gecko natively hits massive 429
        if "geckoterminal" in self.rate_limited_sources or random.random() < 0.6:
            self.rate_limited_sources.add("geckoterminal")
            return None
        return {"price": 1.49, "source": "geckoterminal", "timestamp": timestamp_sec - 60}

    def fetch_dexscreener_proxy(self, token_address: str, timestamp_sec: int) -> Optional[Dict[str, Any]]:
        if "dexscreener" in self.rate_limited_sources or random.random() < 0.2:
            return None
        return {"price": 1.52, "source": "dexscreener", "timestamp": timestamp_sec - 120}

