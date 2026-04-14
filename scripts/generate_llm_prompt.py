import json
import os
from datetime import datetime
from typing import Dict, Any, List

def load_json(filename: str) -> List[Dict[str, Any]]:
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def generate_llm_prompt() -> str:
    """Generate LLM prompt from latest data"""
    # Load data (Обновленные пути)
    candidates = load_json("data/processed/entry_candidates.json")
    arb_opps = load_json("data/processed/arb_opportunities.json") # <- Исправлен путь

    # Sort arb opps by spread (безопасный get)
    arb_opps_sorted = sorted(arb_opps, key=lambda x: float(x.get("net_spread_pct", x.get("spread_pct", 0))), reverse=True)[:10]

    # Sort candidates by smart_inflow_score (защита от отсутствия ключа)
    candidates_sorted = sorted(candidates, key=lambda x: float(x.get("smart_inflow_score", 0) or 0), reverse=True)[:15]

    # Build prompt
    prompt = f"""=== SYSTEM ROLE ===
You are a Web3 arbitrage analyst at a top-tier fund. You specialize in identifying profitable arbitrage opportunities across DEXes and cross-chain bridges. You consider MEV risk, liquidity depth, smart money inflows, and honeypot risks. You provide concise, actionable recommendations.

=== ARB OPPORTUNITIES (Top 10 by spread) ===
"""

    for opp in arb_opps_sorted:
        prompt += f"""
- {opp.get('symbol', 'UNKNOWN')}: {opp.get('net_spread_pct', 0):.2f}% net spread
  Buy: {opp.get('buy_dex')} @ ${opp.get('buy_price', 0):.6f} (Liq: ${opp.get('liquidity_buy_side', 0):,.0f})
  Sell: {opp.get('sell_dex')} @ ${opp.get('sell_price', 0):.6f} (Liq: ${opp.get('liquidity_sell_side', 0):,.0f})
  Max Position: {opp.get('max_position_sol', 0):.2f} SOL | MEV: {opp.get('mev_risk', 'unknown')} | Smart Hits: {opp.get('smart_wallet_hits', 0)}
"""

    prompt += """

=== NEW TOKENS (Top 15 by smart inflow) ===
"""

    for cand in candidates_sorted:
        prompt += f"""
- {cand.get('symbol', 'UNKNOWN')} ({cand.get('token_address', '')[:8]}...):
  Liquidity: ${cand.get('liquidity_usd', 0):,.0f} | Volume H1: ${cand.get('volume_h1', 0):,.0f}
  Price: ${cand.get('price_usd', 0):.6f} | Change H1: {cand.get('price_change_h1', 0):.2f}%
  Age: {cand.get('age_minutes', 0)} min | Smart Score: {cand.get('smart_inflow_score', 0):.1f}
  Verdict: {cand.get('verdict', 'UNKNOWN')} | Risk: {cand.get('risk_score', 0):.1f}
"""

    prompt += """

=== TASK ===
For each token above, return a JSON object with:
{
  "symbol": "string",
  "recommendation": "ARB_NOW" | "ARB_WATCH" | "BUY" | "IGNORE",
  "confidence": 1-10,
  "reasoning": "1-sentence explanation",
  "entry_suggestion": "brief action plan",
  "risk_flags": ["list", "of", "flags"]
}

RULES:
- ARB_NOW only if net_spread > 0.5% AND honeypot=false AND smart_hits > 0
- ARB_WATCH if spread 0.2-0.5% or incomplete data
- BUY only if dev_score > 0.7 AND ethical AND fast_liquidity
- Always flag MEV risk if pool_age < 10 min

Output valid JSON array.
"""

    return prompt

def save_prompt(prompt: str):
    """Save prompt to file and open in editor"""
    date = datetime.now().strftime("%Y-%m-%d")
    filename = f"llm_prompt_{date}.txt"
    with open(filename, 'w') as f:
        f.write(prompt)
    print(f"Prompt saved to {filename}")
    # os.system(f"open {filename}")  # For Mac

if __name__ == "__main__":
    prompt = generate_llm_prompt()
    save_prompt(prompt)