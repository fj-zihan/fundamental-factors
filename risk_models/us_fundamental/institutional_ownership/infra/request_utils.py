"""Request helpers for Massive REST API calls."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
import threading
import time
from typing import ParamSpec, TypeVar

import requests

from ..config import (
    MASSIVE_MAX_RETRIES,
    MASSIVE_REQUESTS_PER_MINUTE,
    MASSIVE_RETRY_SLEEP_SECONDS,
)


P = ParamSpec("P")
T = TypeVar("T", bound=requests.Response)

_rate_lock = threading.Lock()
_next_request_at = 0.0


def massive_rate_limited(func: Callable[P, T]) -> Callable[P, T]:
    """Pace Massive REST calls to stay under the configured request rate."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        global _next_request_at

        if MASSIVE_REQUESTS_PER_MINUTE <= 0:
            return func(*args, **kwargs)

        min_interval = 60.0 / MASSIVE_REQUESTS_PER_MINUTE
        with _rate_lock:
            now = time.monotonic()
            if now < _next_request_at:
                time.sleep(_next_request_at - now)
                now = time.monotonic()
            _next_request_at = now + min_interval

        return func(*args, **kwargs)

    return wrapper


def retry_with_linear_backoff(func: Callable[P, T]) -> Callable[P, T]:
    """Retry transient Massive errors with 10, 20, 30... style backoff."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last_response: requests.Response | None = None
        last_error: requests.RequestException | None = None

        for attempt in range(MASSIVE_MAX_RETRIES + 1):
            try:
                response = func(*args, **kwargs)
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= MASSIVE_MAX_RETRIES:
                    raise
                time.sleep(MASSIVE_RETRY_SLEEP_SECONDS * (attempt + 1))
                continue

            last_response = response
            status_code = getattr(response, "status_code", 200)
            if status_code != 429 and status_code < 500:
                response.raise_for_status()
                return response

            if attempt >= MASSIVE_MAX_RETRIES:
                break

            retry_after = response.headers.get("Retry-After")
            sleep_seconds = (
                float(retry_after)
                if retry_after
                else MASSIVE_RETRY_SLEEP_SECONDS * (attempt + 1)
            )
            time.sleep(sleep_seconds)

        if last_response is not None:
            last_response.raise_for_status()
            return last_response
        if last_error is not None:
            raise last_error
        raise RuntimeError("Massive request failed before a response was created.")

    return wrapper
