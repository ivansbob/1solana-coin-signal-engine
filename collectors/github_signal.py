import re
import asyncio
import time
from typing import Dict, Any, List
import aiohttp
from dotenv import load_dotenv

from utils.rate_limit import acquire
from utils.retry import with_retry
from config.settings import Settings  # или load_settings()

load_dotenv()

# ================== НАСТРОЙКИ 2026 ==================
GITHUB_SEARCH_URL = "https://api.github.com/search/repositories"
MAX_REPOS = 25  # увеличено для большего покрытия
LOOKBACK_HOURS = 72  # чуть больше для захвата ранних сигналов

# Hot нарративы 2026 (AI x Web3 + modular + Solana-specific)
KEYWORDS = [
    "ai-agent",
    "eliza",
    "elizaos",
    "tee",
    "trusted-execution-environment",
    "zk-coprocessor",
    "zkml",
    "zk-proof",
    "zkevm",
    "zk-rollup",
    "svm-l2",
    "liquid-restaking",
    "eigenlayer",
    "restaking",
    "intent-based",
    "account-abstraction",
    "erc-4337",
    "cross-chain",
    "omnichain",
    "layerzero",
    "wormhole",
    "modular-blockchain",
    "data-availability",
    "celestia",
    "decentralized-sequencer",
    "based-rollup",
    "pump.fun",
    "meme-agent",
    "jito-bundle",
    "solana-agent-kit",
    "ai-rig",
    "zerebro",
    "tars-protocol",  # популярные AI-фреймворки на Solana
]

# Scam/low-signal фильтры (низкий приоритет или исключение)
SCAM_FILTERS = [
    "meme-coin",
    "100x",
    "presale",
    "safemoon",
    "fork-of",
    "rug-proof",
    "honeypot",
    "dev-sell",
    "fair-launch",  # часто маскируют скам
]

# Улучшенный Regex для Solana адресов (base58, 32-44 символа)
# Более строгий: исключает очевидный мусор
SOLANA_ADDRESS_REGEX = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

# ================== НОВЫЕ МЕТРИКИ 2026 ==================
# (не дублируем KEYWORDS и SOLANA_ADDRESS_REGEX из PR-1)

AGENTIC_KEYWORDS = [
    "solana-agent-kit",
    "eliza",
    "openclaw",
    "mcp-server",
    "ai-trading-agent",
    "dlmm-agent",
    "autonomous-agent",
    "reinforcement-learning",
    "goat-sdk",
]

# ================== ОСНОВНЫЕ ФУНКЦИИ ==================


async def search_new_repos() -> List[Dict]:
    """Поиск новых/активных репозиториев по нарративам 2026."""
    async with aiohttp.ClientSession() as session:
        query = (
            " ".join(KEYWORDS)
            + " language:Rust OR language:TypeScript OR language:Python created:>="
            + time.strftime(
                "%Y-%m-%d", time.gmtime(time.time() - LOOKBACK_HOURS * 3600)
            )
        )

        params = {
            "q": query,
            "sort": "updated",
            "order": "desc",
            "per_page": MAX_REPOS,
        }

        acquire("github")  # rate limit

        async with session.get(GITHUB_SEARCH_URL, params=params) as resp:
            if resp.status != 200:
                raise Exception(f"GitHub API error: {resp.status}")
            data = await resp.json()
            return data.get("items", [])


def calculate_dev_activity_score(repo: Dict[str, Any]) -> Dict[str, Any]:
    """Улучшенный scoring с нарративами 2026 + contract extraction."""
    stars = repo.get("stargazers_count", 0)
    forks = repo.get("forks_count", 0)
    pushed_at = repo.get("pushed_at", "")
    description = (repo.get("description") or "").lower()
    readme_snippet = (repo.get("readme") or "").lower()  # если парсишь отдельно
    topics = repo.get("topics", [])

    # Базовый activity
    score = min(100, (stars * 0.4) + (forks * 0.3) + (len(topics) * 5))

    reason_codes: List[str] = []

    # Нарративный буст 2026
    narrative_hits = sum(
        1
        for kw in KEYWORDS
        if kw.lower() in description or kw.lower() in " ".join(topics)
    )
    score += narrative_hits * 8
    if narrative_hits >= 2:
        reason_codes.append("strong_2026_narrative")

    # AI/TEE/zk буст
    if any(
        kw in description for kw in ["ai-agent", "tee", "zk-", "restaking", "eliza"]
    ):
        score += 15
        reason_codes.append("ai_tee_zk_priority")

    # Scam downgrade
    scam_hits = sum(1 for f in SCAM_FILTERS if f.lower() in description)
    if scam_hits > 0:
        score = max(10, score - scam_hits * 12)
        reason_codes.append("scam_filter_hit")

    # Contract extraction (description + readme snippet)
    found_addresses = SOLANA_ADDRESS_REGEX.findall(description + " " + readme_snippet)
    extracted_contracts = list(set(found_addresses))  # дедуп

    # Финальный clamp + provenance
    final_score = round(max(0, min(100, score)), 2)

    return {
        "dev_activity_score": final_score,
        "narrative_score": round(narrative_hits / max(1, len(KEYWORDS)) * 100, 2),
        "reason_codes": reason_codes,
        "extracted_contracts": extracted_contracts,
        "scam_risk": "high" if scam_hits > 1 else "low",
        "provenance": "github_search_2026",
        "last_pushed": pushed_at,
    }


def build_github_text_section(candidates: List[Dict]) -> str:
    """Улучшенный вывод для LLM-анализа + арбитраж."""
    lines = ["# GitHub Alpha Signals 2026 (AI x Solana + Modular Web3)\n"]
    lines.append(
        f"Found {len(candidates)} high-potential repos (last {LOOKBACK_HOURS}h)\n"
    )

    for repo in sorted(
        candidates, key=lambda x: x.get("dev_activity_score", 0), reverse=True
    ):
        full_name = repo.get("full_name", "unknown")
        description = repo.get("description") or "No description"
        stars = repo.get("stargazers_count", 0)
        forks = repo.get("forks_count", 0)
        time_ago = repo.get("pushed_at", "unknown")[:10]

        score_data = calculate_dev_activity_score(repo)  # или уже предвычислено

        lines.append(f"## Repo: {full_name}")
        lines.append(f"Description: {description}")
        lines.append(f"Stars: {stars} | Forks: {forks} | Last push: {time_ago}")
        lines.append(f"Dev Activity Score: {score_data['dev_activity_score']}/100")
        lines.append(f"Narrative Score: {score_data.get('narrative_score', 0)}%")
        lines.append(f"Reason: {', '.join(score_data['reason_codes'])}")

        contracts = score_data.get("extracted_contracts", [])
        if contracts:
            lines.append(f"🔥 FOUND SOLANA CONTRACTS: {', '.join(contracts)}")
            lines.append(
                "→ Рекомендация: Проверить на DexScreener / Pump.fun / Birdeye для раннего арбитража/liquidity"
            )

        if score_data.get("scam_risk") == "high":
            lines.append("⚠️ HIGH SCAM RISK — low priority")

        lines.append("---\n")

    lines.append(
        "\n💡 Prompt для LLM: Проанализируй эти репозитории на предмет ранних мем-агентов, AI-trading инструментов и новых Solana-контрактов. Выдели арбитражные возможности (новые пулы, бандлы, restaking интеграции)."
    )

    return "\n".join(lines)


def calculate_enhanced_dev_activity_score(
    repo: Dict[str, Any], x_mentions: int = 0
) -> Dict[str, Any]:
    """Улучшенный scoring с метриками 2026 (velocity, smart_money_proxy, agentic_potential)."""
    base = calculate_dev_activity_score(repo)  # вызов из PR-1 (не дублируем)

    description = (repo.get("description") or "").lower()
    topics = repo.get("topics", [])
    stars = repo.get("stargazers_count", 0)
    forks = repo.get("forks_count", 0)
    pushed_at = repo.get("pushed_at", "")

    # Velocity proxy (быстрое накопление traction = как bonding curve velocity)
    from datetime import datetime, timezone

    velocity_score = min(
        100,
        (stars + forks * 2)
        / max(
            1,
            (
                datetime.now(timezone.utc)
                - datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
            ).days
            + 1,
        )
        * 10,
    )

    # Smart money / early adoption proxy
    smart_money_proxy = (
        1
        if any(
            kw in description or kw in " ".join(topics)
            for kw in ["smart money", "copy trade", "jito", "early alpha"]
        )
        else 0
    )
    smart_money_proxy += (
        1 if any(ak in " ".join(topics) for ak in AGENTIC_KEYWORDS) else 0
    )

    # Agentic potential (для coding agents — насколько репозиторий подходит под автономного AI trading agent)
    agentic_hits = sum(
        1 for kw in AGENTIC_KEYWORDS if kw in description or kw in " ".join(topics)
    )
    agentic_potential = round(agentic_hits * 18 + (x_mentions * 5), 2)

    # Risk-adjusted final score
    risk_penalty = 20 if base.get("scam_risk") == "high" else 0
    final_score = round(
        max(
            10,
            base["dev_activity_score"]
            + velocity_score * 0.6
            + smart_money_proxy * 12
            + agentic_potential * 0.4
            - risk_penalty,
        ),
        2,
    )

    return {
        **base,
        "velocity_score": round(velocity_score, 2),
        "smart_money_proxy": smart_money_proxy,
        "agentic_potential": agentic_potential,
        "x_mention_boost": x_mentions,
        "risk_adjusted_score": final_score,
        "actionable_for_coding_agent": "high" if agentic_potential > 40 else "medium",
        "provenance": "github_enhanced_2026",
    }


async def enrich_with_x_signals(repo_full_name: str) -> int:
    """Агрегация X-данных для репозитория (mentions, hype)."""
    # Здесь можно вызвать x_semantic_search или x_keyword_search из твоих utils
    # Пример stub (реализуй через существующий x_keyword_search)
    query = f"{repo_full_name} (ai agent OR solana OR trading)"
    # results = await x_keyword_search(...)
    # return len(results)  # или суммарный engagement
    return 15  # placeholder — замени на реальный вызов


def generate_coding_agent_prompt(candidates: List[Dict]) -> str:
    """Специальный промпт для coding agents (Claude/Grok/OpenClaw) — максимально полезный."""
    lines = ["# GitHub Alpha → Coding Agent Prompt 2026\n"]
    lines.append(
        "Ты — автономный Solana AI Coding Agent. Твоя задача: превратить эти репозитории в production-ready автономных trading agents.\n"
    )
    lines.append(
        "Фокус 2026: AI agents (ElizaOS-style), DLMM liquidity agents, Jito bundle integration, Solana Agent Kit + reinforcement learning для memecoins.\n\n"
    )

    for repo in sorted(
        candidates, key=lambda x: x.get("risk_adjusted_score", 0), reverse=True
    )[:10]:
        score = repo.get("risk_adjusted_score", 0)
        contracts = repo.get("extracted_contracts", [])
        agentic = repo.get("agentic_potential", 0)

        lines.append(f"## Repo: {repo.get('full_name')}")
        lines.append(f"Risk-Adjusted Score: {score} | Agentic Potential: {agentic}")
        lines.append(
            f"Velocity: {repo.get('velocity_score')} | Smart Money Proxy: {repo.get('smart_money_proxy')}"
        )
        if contracts:
            lines.append(
                f"Found Contracts: {', '.join(contracts)} → Интегрируй как target для sniper / liquidity provider agent"
            )
        lines.append("Next Steps для тебя (coding agent):")
        lines.append("1. Fork repo")
        lines.append("2. Интегрируй Solana Agent Kit + Jito bundles")
        lines.append(
            "3. Добавь autonomous loop: monitor → decide (RL или LLM) → execute (swap / DLMM position)"
        )
        lines.append(
            "4. Добавь risk controls: bonding-curve velocity check + dev-sell monitor"
        )
        lines.append("5. Deploy как OpenClaw / Eliza-compatible agent\n")

    lines.append(
        "\nOutput format: Полный код diff + deployment инструкция + expected daily fees (на основе похожих DLMM agents ~200 SOL/day)."
    )
    return "\n".join(lines)


def generate_coding_agent_prompt_v2(velocity_candidates: List[Dict]) -> str:
    """Улучшенный промпт для coding agents с velocity-метриками."""
    lines = [
        "# GitHub Velocity → Autonomous AI Agent Builder Prompt 2026",
        "Ты — Solana AI Coding Agent (на базе ElizaOS / Solana Agent Kit / OpenClaw).",
        "Твоя задача: превратить высок-velocity репозитории в production-ready автономных trading / liquidity / meme agents.\n",
    ]

    top_repos = sorted(
        velocity_candidates,
        key=lambda x: x.get("combined_velocity_score", 0),
        reverse=True,
    )[:8]

    for repo in top_repos:
        vel = repo.get("combined_velocity_score", 0)
        accel = repo.get("acceleration", 0)
        contracts = repo.get("extracted_contracts", [])  # из предыдущих PR

        lines.append(f"## HIGH VELOCITY REPO: {repo.get('full_name')}")
        lines.append(
            f"Combined Velocity: {vel} | Acceleration: {accel} | Age: {repo.get('age_days')} days"
        )
        if contracts:
            lines.append(f"Detected Contracts: {', '.join(contracts)}")
        lines.append("Action Plan для тебя:")
        lines.append("1. Fork репозиторий")
        lines.append("2. Интегрируй Solana Agent Kit (60+ on-chain actions)")
        lines.append("3. Добавь Jito bundles для execution + slippage protection")
        lines.append(
            "4. Реализуй monitoring loop: GitHub velocity + on-chain liquidity velocity (DexScreener)"
        )
        lines.append(
            "5. Добавь entry logic: если combined_velocity > 20 и liquidity_growth_1h > 30% → open position"
        )
        lines.append("6. Используй evidence-weighted sizing из основной системы")
        lines.append("7. Deploy как ElizaOS-compatible agent с persistent memory\n")

    lines.append("\nOutput:")
    lines.append("- Полный код (diff или новые файлы)")
    lines.append("- deployment инструкция (Docker + Helius RPC)")
    lines.append(
        "- expected edge: daily fees / winrate на основе похожих velocity проектов"
    )
    lines.append("- risk controls: dev-sell monitor + rug score check")

    return "\n".join(lines)


# Пример использования в aggregator
async def collect_github_candidates(max_candidates: int = 15):
    repos = await search_new_repos()
    enriched = []
    for repo in repos[:max_candidates]:
        enriched.append({**repo, **calculate_dev_activity_score(repo)})
    return enriched


# Enhanced version with X signals and 2026 metrics
async def collect_enhanced_github_candidates(max_candidates: int = 15):
    repos = await search_new_repos()
    enriched = []
    for repo in repos[:max_candidates]:
        repo_full_name = repo.get("full_name", "")
        x_mentions = await enrich_with_x_signals(repo_full_name)
        enhanced = calculate_enhanced_dev_activity_score(repo, x_mentions)
        enriched.append({**repo, **enhanced})
    return enriched
