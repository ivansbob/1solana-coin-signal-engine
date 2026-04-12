import requests
import re
import json
import os
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

HONEYPOT_PATTERNS = [
    {"name": "balance_tx_origin", "regex": re.compile(r'function\s+balanceOf[^}]{0,500}(?:tx\.origin|origin\(\))', re.DOTALL | re.IGNORECASE)},
    {"name": "allowance_tx_origin", "regex": re.compile(r'function\s+allowance[^}]{0,500}(?:tx\.origin|origin\(\))', re.DOTALL | re.IGNORECASE)},
    {"name": "transfer_tx_origin", "regex": re.compile(r'function\s+transfer[^}]{0,500}(?:tx\.origin|origin\(\))', re.DOTALL | re.IGNORECASE)},
    {"name": "hidden_fee_taxPayer", "regex": re.compile(r'function\s+_taxPayer[^}]{0,300}(?:tx\.origin|origin\(\))', re.DOTALL | re.IGNORECASE)},
    {"name": "isSuper_tx_origin", "regex": re.compile(r'function\s+_isSuper[^}]{0,200}(?:tx\.origin|origin\(\))', re.DOTALL | re.IGNORECASE)},
    {"name": "tx_origin_require", "regex": re.compile(r'require\s*\(\s*[^)]{0,500}?tx\.origin', re.DOTALL | re.IGNORECASE)},
    {"name": "tx_origin_if_auth", "regex": re.compile(r'if\s*\(\s*[^)]{0,200}?tx\.origin\s*[!=]=[^)]{0,200}?\)\s*(?:revert|require)', re.DOTALL | re.IGNORECASE)},
    {"name": "tx_origin_assert", "regex": re.compile(r'assert\s*\(\s*[^)]{0,200}?tx\.origin', re.DOTALL | re.IGNORECASE)},
    {"name": "tx_origin_mapping", "regex": re.compile(r'\[\s*tx\.origin\s*\]\s*=', re.DOTALL | re.IGNORECASE)},
    {"name": "sell_block_pattern", "regex": re.compile(r'if\s*\(\s*_isSuper\s*\(\s*recipient\s*\)\s*\)\s*return\s+false', re.DOTALL | re.IGNORECASE)},
    {"name": "asymmetric_transfer_logic", "regex": re.compile(r'function\s+_canTransfer[^}]{0,500}return\s+false', re.DOTALL | re.IGNORECASE)},
    {"name": "transfer_whitelist_only", "regex": re.compile(r'require\s*\(\s*_whitelist\[[^\]]{0,200}\]\s*\|\|\s*_whitelist\[[^\]]{0,200}\]\s*,', re.DOTALL | re.IGNORECASE)},
    {"name": "hidden_sell_tax", "regex": re.compile(r'if\s*\([^)]{0,200}pair[^)]{0,200}\)[^{]{0,500}\{[^}]{0,500}sellTax\s*=\s*(?:100|99|98|97|96|95)', re.DOTALL | re.IGNORECASE)},
]

MIN_PATTERNS_FOR_DETECTION = 2

def parse_source_code(source_code: str) -> str:
    normalized = source_code.strip()
    if normalized.startswith('{{') and normalized.endswith('}}'):
        normalized = normalized[1:-1]
    if normalized.startswith('{'):
        try:
            data = json.loads(normalized)
            if isinstance(data, dict) and 'sources' in data:
                combined = ''
                for filename, file_obj in data.get('sources', {}).items():
                    content = file_obj.get('content') if isinstance(file_obj, dict) else None
                    if content:
                        combined += f"// File: {filename}\n{content}\n\n"
                return combined if combined else source_code
        except Exception:
            pass
    return source_code

def detect_honeypot(source_code: str) -> Dict[str, Any]:
    if not source_code or len(source_code) < 50:
        return {"is_honeypot": False, "matched_count": 0, "patterns": [], "risk_score": 0}
    source_code = source_code[:500 * 1024]
    matches: List[Dict] = []
    for p in HONEYPOT_PATTERNS:
        try:
            for match in p["regex"].finditer(source_code):
                line = source_code[:match.start()].count('\n') + 1
                matches.append({"name": p["name"], "line": line, "snippet": match.group(0)[:120]})
        except Exception:
            continue
    is_honeypot = len(matches) >= MIN_PATTERNS_FOR_DETECTION
    risk_score = min(10, len(matches) * 3)
    return {
        "is_honeypot": is_honeypot,
        "matched_count": len(matches),
        "patterns": matches[:15],
        "risk_score": risk_score,
        "status": "HONEYPOT" if is_honeypot else "SUSPICIOUS" if matches else "SAFE"
    }

def fetch_contract_source(address: str, chain: str = "ethereum", etherscan_api_key: str = "") -> Optional[str]:
    chain_configs = {
        "ethereum": 1, "eth": 1, "polygon": 137, "matic": 137,
        "arbitrum": 42161, "arb": 42161, "bnb": 56, "bsc": 56,
        "base": 8453, "optimism": 10, "op": 10,
        "avalanche": 43114, "avax": 43114,
        "fantom": 250, "ftm": 250,
    }
    chain_lower = chain.lower().strip()
    chain_id = chain_configs.get(chain_lower)
    if not chain_id:
        return None
    url = f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=contract&action=getsourcecode&address={address}"
    if etherscan_api_key:
        url += f"&apikey={etherscan_api_key}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if str(data.get("status")) != "1" or not data.get("result"):
            return None
        result = data["result"][0]
        source = result.get("SourceCode")
        if not source:
            return None
        return parse_source_code(source)
    except Exception:
        return None

def honeypot_check_teycir(token_address: str, chain: str = "ethereum", etherscan_api_key: str = "") -> Dict[str, Any]:
    load_dotenv()
    if not etherscan_api_key:
        etherscan_api_key = os.getenv("ETHERSCAN_API_KEY", "")
    result: Dict[str, Any] = {
        "source": "teycir_honeypotscan",
        "token_address": token_address.lower(),
        "chain": chain,
        "is_honeypot": False,
        "risk_score": 0,
        "reasons": [],
        "status": "unknown",
        "matched_patterns": []
    }
    try:
        source_code = fetch_contract_source(token_address, chain, etherscan_api_key)
        if not source_code:
            result.update({"status": "source_not_verified", "reasons": ["Source code not verified on block explorer"]})
            return result
        detection = detect_honeypot(source_code)
        result.update({
            "is_honeypot": detection["is_honeypot"],
            "risk_score": detection["risk_score"],
            "matched_patterns": [p["name"] for p in detection["patterns"]],
            "status": detection["status"],
            "reasons": [f"Matched {len(detection['patterns'])} honeypot patterns"] if detection["patterns"] else ["No dangerous patterns detected"]
        })
    except Exception as e:
        result.update({"status": "error", "reasons": [f"Scan error: {str(e)[:120]}"]})
    return result

if __name__ == "__main__":
    import json
    key = os.getenv("ETHERSCAN_API_KEY", "")
    test_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    result = honeypot_check_teycir(test_addr, chain="ethereum", etherscan_api_key=key)
    print(json.dumps(result, indent=2, ensure_ascii=False))
