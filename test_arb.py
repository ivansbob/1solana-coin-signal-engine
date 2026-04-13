import asyncio
from analytics.arb_scanner import scan_arb_opportunities

async def test():
    # Test with JUP token
    test_mints = ["JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"]  # JUP mint
    results = await scan_arb_opportunities(test_mints)
    for res in results:
        print(f"Token: {res.symbol} | Net Spread: {res.net_spread_pct}% | Viable: {res.viable}")

asyncio.run(test())