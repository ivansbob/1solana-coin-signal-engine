"""Retry helpers with exponential backoff for network calls."""

from __future__ import annotations

import time
import asyncio
from collections.abc import Callable
from typing import Any


def is_retryable_error(exc: Exception) -> bool:
    return isinstance(exc, (TimeoutError, ConnectionError))


def retry_decorator(max_attempts: int = 3, base_delay: float = 0.2, sleep_fn: Callable[[float], None] = time.sleep):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if not is_retryable_error(exc) or attempt >= max_attempts:
                        raise
                    sleep_sec = min(base_delay * (2 ** (attempt - 1)), 8.0)
                    sleep_fn(sleep_sec)
            if last_error is not None:
                raise last_error
            raise RuntimeError("Retry wrapper failed without executing function")
        return wrapper
    return decorator


def async_retry_decorator(max_attempts: int = 3, base_delay: float = 0.2, sleep_fn: Callable[[float], Any] = asyncio.sleep):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if not is_retryable_error(exc) or attempt >= max_attempts:
                        raise
                    sleep_sec = min(base_delay * (2 ** (attempt - 1)), 8.0)
                    await sleep_fn(sleep_sec)
            if last_error is not None:
                raise last_error
            raise RuntimeError("Async retry wrapper failed without executing function")
        return wrapper
    return decorator


# Backward compatibility
with_retry = retry_decorator()
async_with_retry = async_retry_decorator()
