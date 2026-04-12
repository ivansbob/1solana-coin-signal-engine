import pytest
from src.replay.price_history_router import PriceHistoryRouter
from src.collectors.price_history_client import PriceHistoryClient

def test_router_prefers_jupiter_then_pyth_then_gecko():
    client = PriceHistoryClient()
    router = PriceHistoryRouter(client)
    
    # We enforce fetching Jupiter aggressively
    # To test perfectly, we will bypass randomness by directly assigning the outputs
    client.fetch_jupiter_quote = lambda token, ts: {"price": 10.0, "source": "jupiter", "timestamp": ts}
    evidence = router.fetch_best_price_path("A", 1000)
    assert evidence["price_path_source"] == "jupiter"
    assert evidence["price_path_status"] == "full"
    
    # Drop Jupiter -> Should hit pyth
    client.fetch_jupiter_quote = lambda token, ts: None
    client.fetch_pyth_hermes = lambda token, ts: {"price": 10.1, "source": "pyth", "timestamp": ts}
    evidence2 = router.fetch_best_price_path("A", 1000)
    assert evidence2["price_path_source"] == "pyth"
    
    # Drop pyth -> Should hit geckoterminal
    client.fetch_pyth_hermes = lambda token, ts: None
    client.fetch_geckoterminal_ohlcv = lambda token, ts: {"price": 10.2, "source": "geckoterminal", "timestamp": ts - 60}
    evidence3 = router.fetch_best_price_path("A", 1000)
    assert evidence3["price_path_source"] == "geckoterminal"
    assert evidence3["price_path_status"] == "partial" # Gap is > 0 but < 300 

def test_rate_limit_fallback_to_backfill():
    client = PriceHistoryClient()
    router = PriceHistoryRouter(client)
    
    # Mark every external API blocked or broken
    client.fetch_jupiter_quote = lambda token, ts: None
    client.fetch_pyth_hermes = lambda token, ts: None
    client.rate_limited_sources.add("geckoterminal")
    client.fetch_dexscreener_proxy = lambda token, ts: None
    
    evidence = router.fetch_best_price_path("A", 1000)
    assert evidence["price_path_status"] == "rate_limited"
    assert evidence["price_path_source"] == "geckoterminal_blocked"

def test_partial_price_path_applies_correct_penalty():
    client = PriceHistoryClient()
    router = PriceHistoryRouter(client)
    
    # If the price path is 400 seconds old -> STALE
    client.fetch_jupiter_quote = lambda token, ts: {"price": 1.0, "source": "jupiter", "timestamp": ts - 400}
    evidence = router.fetch_best_price_path("A", 1000)
    
    assert evidence["price_path_status"] == "stale"
    assert evidence["price_path_confidence"] == 0.0 # Heavy drop
    
