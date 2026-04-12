"""Route-level price history provider failover orchestration."""

from __future__ import annotations

from typing import Any, Callable

NON_RETRYABLE_FAILURE_CLASSES = {
    "ohlcv_not_available",
    "pool_not_found",
    "pool_resolution_failed",
    "provider_pair_not_found",
}

RETRYABLE_FAILURE_CLASSES = {
    "rate_limited_resolver",
    "rate_limited_ohlcv",
    "provider_timeout",
    "provider_http_error",
}


def _normalize_provider_order(primary_provider: str | None, configured: list[str] | None) -> list[str]:
    order: list[str] = []
    if primary_provider:
        order.append(str(primary_provider))
    for provider in configured or []:
        value = str(provider or "").strip()
        if value and value not in order:
            order.append(value)
    return order


def _route_kind(seed_kind: str | None, has_seed: bool) -> str:
    if not has_seed:
        return "token"
    if seed_kind in {"pool", "pair"}:
        return seed_kind
    return "pair"


def build_route_attempt_plan(
    *,
    primary_provider: str | None,
    provider_order: list[str] | None,
    seeded_identifier: str | None,
    seed_source: str | None,
    seed_kind: str | None,
) -> list[dict[str, Any]]:
    providers = _normalize_provider_order(primary_provider, provider_order)
    attempts: list[dict[str, Any]] = []
    for idx, provider in enumerate(providers):
        if seeded_identifier:
            attempts.append(
                {
                    "provider": provider,
                    "kind": _route_kind(seed_kind, True),
                    "pair_address": seeded_identifier,
                    "seed_source": seed_source,
                    "route_rank": len(attempts) + 1,
                    "route_group": "same_provider" if idx == 0 else "alternate_provider",
                }
            )
        attempts.append(
            {
                "provider": provider,
                "kind": "token",
                "pair_address": None,
                "seed_source": None,
                "route_rank": len(attempts) + 1,
                "route_group": "same_provider" if idx == 0 else "alternate_provider",
            }
        )
    return attempts


def _is_non_retryable(row: dict[str, Any]) -> bool:
    if row.get("provider_family_exhausted") is False:
        return False
    failure_class = str(row.get("provider_failure_class") or "")
    if bool(row.get("negative_cache_hit")):
        return True
    if failure_class in NON_RETRYABLE_FAILURE_CLASSES:
        return True
    if row.get("provider_failure_retryable") is False:
        return True
    return False


def _is_retryable(row: dict[str, Any]) -> bool:
    failure_class = str(row.get("provider_failure_class") or "")
    if bool(row.get("cooldown_applied")):
        return True
    if row.get("provider_failure_retryable") is True:
        return True
    return failure_class in RETRYABLE_FAILURE_CLASSES


def _is_terminal_success(row: dict[str, Any], *, accept_partial_usable: bool) -> bool:
    if str(row.get("price_path_status") or "") == "complete":
        return True
    return accept_partial_usable and bool(row.get("partial_but_usable_row"))


def route_price_history(
    *,
    token_context: dict[str, Any],
    provider_order: list[str] | None,
    primary_provider: str | None,
    prior_failure: dict[str, Any] | None,
    max_routes_per_token: int,
    accept_partial_usable: bool,
    cross_provider_fallback_on_retryable: bool,
    fetch_route: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    seed = str(token_context.get("selected_pool_address") or token_context.get("pair_address") or "").strip() or None
    seed_source = None
    seed_kind = None
    if token_context.get("selected_pool_address"):
        seed_source = "selected_pool_address"
        seed_kind = "pool"
    elif token_context.get("pair_address"):
        seed_source = "artifact_pair_address"
        seed_kind = "pair"

    attempts_plan = build_route_attempt_plan(
        primary_provider=primary_provider,
        provider_order=provider_order,
        seeded_identifier=seed,
        seed_source=seed_source,
        seed_kind=seed_kind,
    )
    if max_routes_per_token > 0:
        attempts_plan = attempts_plan[: max_routes_per_token]

    prior_key = None
    if prior_failure:
        prior_key = (
            str(prior_failure.get("selected_route_provider") or ""),
            str(prior_failure.get("selected_route_kind") or ""),
            str(prior_failure.get("route_seed") or ""),
        )

    route_attempts: list[dict[str, Any]] = []
    skipped_keys: set[tuple[str, str, str]] = set()
    terminal = None
    last_row: dict[str, Any] | None = None
    last_attempt: dict[str, Any] | None = None

    for attempt in attempts_plan:
        key = (attempt["provider"], attempt["kind"], str(attempt.get("pair_address") or ""))
        if key in skipped_keys:
            continue
        if prior_key and key == prior_key and (prior_failure or {}).get("provider_failure_retryable") is False:
            continue
        row = fetch_route(attempt)
        row = dict(row)
        row.setdefault("source_provider", attempt["provider"])
        last_row = row
        last_attempt = attempt

        non_retryable = _is_non_retryable(row)
        retryable = _is_retryable(row)
        if non_retryable:
            skipped_keys.add(key)

        attempt_meta = {
            "provider": attempt["provider"],
            "route_kind": attempt["kind"],
            "route_seed": attempt.get("pair_address"),
            "route_seed_source": attempt.get("seed_source"),
            "route_candidate_rank": row.get("selected_pool_candidate_rank"),
            "route_pool_address": row.get("selected_pool_address") or row.get("pool_address"),
            "route_group": attempt.get("route_group"),
            "price_path_status": row.get("price_path_status"),
            "partial_but_usable_row": bool(row.get("partial_but_usable_row")),
            "missing": bool(row.get("missing")),
            "warning": row.get("warning"),
            "provider_failure_class": row.get("provider_failure_class"),
            "provider_failure_retryable": row.get("provider_failure_retryable"),
            "cooldown_applied": bool(row.get("cooldown_applied")),
            "negative_cache_hit": bool(row.get("negative_cache_hit")),
            "provider_family_exhausted": bool(row.get("provider_family_exhausted")),
        }
        route_attempts.append(attempt_meta)

        if _is_terminal_success(row, accept_partial_usable=accept_partial_usable):
            terminal = (row, attempt)
            break

        if retryable and not cross_provider_fallback_on_retryable:
            break

    if terminal is None:
        base = dict(last_row or {})
        base.setdefault("price_path_status", "missing")
        base.setdefault("missing", True)
        base.setdefault("warning", "price_history_router_exhausted")
        base.setdefault("source_provider", primary_provider)
        base.setdefault("price_history_provider", primary_provider)
        selected_attempt = {
            "provider": (last_attempt or {}).get("provider"),
            "route_kind": (last_attempt or {}).get("kind"),
            "route_seed": (last_attempt or {}).get("pair_address"),
            "route_seed_source": (last_attempt or {}).get("seed_source"),
        } if last_attempt else None
    else:
        base, selected = terminal
        selected_attempt = {
            "provider": selected["provider"],
            "route_kind": selected["kind"],
            "route_seed": selected.get("pair_address"),
            "route_seed_source": selected.get("seed_source"),
        }

    base["price_history_route_selected"] = selected_attempt
    base["price_history_route_attempts"] = route_attempts
    base["price_history_router_status"] = "ok" if terminal is not None else "exhausted"
    base["price_history_router_warning"] = None if terminal is not None else base.get("warning")
    base["price_history_fallback_used"] = len(route_attempts) > 1
    base["selected_route_provider"] = (selected_attempt or {}).get("provider") if selected_attempt else None
    base["selected_route_kind"] = (selected_attempt or {}).get("route_kind") if selected_attempt else None
    base["selected_route_seed_source"] = (selected_attempt or {}).get("route_seed_source") if selected_attempt else None
    return base
