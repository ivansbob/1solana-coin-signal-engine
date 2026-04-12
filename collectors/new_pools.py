import httpx

RAYDIUM_URL = "https://api.raydium.io/v2/sdk/token/raydium.mainnet.json"

_previous_tokens = set()


def get_current_tokens():
    try:
        response = httpx.get(RAYDIUM_URL, timeout=10)
        data = response.json()
        return set(j["mint"] for j in data.get("unNamed", []))
    except Exception as e:
        print(f"[ERROR] Raydium fetch failed: {e}")
        return set()


def get_new_tokens():
    global _previous_tokens

    current_tokens = get_current_tokens()

    if not _previous_tokens:
        _previous_tokens = current_tokens
        return []

    new_tokens = list(current_tokens - _previous_tokens)

    _previous_tokens = current_tokens

    return new_tokens


def filter_tokens(tokens):
    return [t for t in tokens if len(t) > 30]