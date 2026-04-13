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

        await acquire("github")  # rate limit

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


# Пример использования в aggregator
async def collect_github_candidates(max_candidates: int = 15):
    repos = await search_new_repos()
    enriched = []
    for repo in repos[:max_candidates]:
        enriched.append({**repo, **calculate_dev_activity_score(repo)})
    return enriched
