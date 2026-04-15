# Known safe tokens based on best-of-crypto and hardcoded mints

KNOWN_SAFE_MINTS = {
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",  # RAY
    "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",  # ORCA
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",  # WIF
}

KNOWN_SAFE_SYMBOLS = {
    "SOL",
    "USDC",
    "USDT",
    "RAY",
    "ORCA",
    "JUP",
    "BONK",
    "WIF",
}


def is_known_safe(mint: str, symbol: str = "") -> bool:
    return mint in KNOWN_SAFE_MINTS or symbol.upper() in KNOWN_SAFE_SYMBOLS