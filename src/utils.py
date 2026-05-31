"""Utility functions for the monitoring pipeline."""

import json
import logging
import os
import random
import re
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import Any

import structlog
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
)


def setup_logging(name: str) -> structlog.BoundLogger:
    """Configure structlog with JSON rendering and return a bound logger.

    Args:
        name: Logger name, typically __name__ from the calling module.

    Returns:
        A bound structlog logger instance.
    """
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger(name)  # type: ignore[no-any-return]


def atomic_write(filepath: str, data: dict[str, Any] | list[Any]) -> None:
    """Write JSON data atomically to a file using a temporary file and rename.

    Args:
        filepath: Target file path for the JSON output.
        data: Dictionary or list to serialize as JSON.
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str, ensure_ascii=False)
        os.rename(tmp_path, filepath)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def atomic_read(filepath: str) -> dict[str, Any] | list[Any]:
    """Read and parse a JSON file safely.

    Args:
        filepath: Path to the JSON file.

    Returns:
        Parsed JSON content as dict or list.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
    """
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)  # type: ignore[no-any-return]


def ensure_dir(path: str) -> None:
    """Create directory if it does not exist.

    Args:
        path: Directory path to create.
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def format_timestamp(dt: datetime | None = None) -> str:
    """Return an ISO 8601 formatted timestamp with microseconds.

    Args:
        dt: Datetime object, defaults to current UTC time.

    Returns:
        ISO 8601 string with microseconds and 'Z' suffix.
    """
    if dt is None:
        dt = datetime.now(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def calculate_backoff(attempt: int, base: float = 1, max_wait: float = 64) -> float:
    """Calculate exponential backoff with random jitter.

    Args:
        attempt: Current attempt number (0-indexed).
        base: Base delay in seconds.
        max_wait: Maximum wait time in seconds.

    Returns:
        Computed backoff duration in seconds.
    """
    wait = min(max_wait, base * (2**attempt))
    jitter: float = random.uniform(0, wait * 0.5)
    result: float = wait + jitter
    return result


_utils_logger = None


def _get_utils_logger() -> structlog.BoundLogger:
    global _utils_logger
    if _utils_logger is None:
        _utils_logger = setup_logging("src.utils")
    return _utils_logger


def retry_with_backoff(
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 64.0,
) -> Callable[..., Any]:
    """Decorator that applies retry logic with exponential backoff and jitter.

    Uses tenacity under the hood.

    Args:
        max_attempts: Maximum number of attempts before giving up.
        base_delay: Initial delay between retries in seconds.
        max_delay: Maximum delay between retries in seconds.

    Returns:
        A decorator that wraps a function with retry logic.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential_jitter(initial=base_delay, max=max_delay, jitter=base_delay),
            before_sleep=before_sleep_log(logging.getLogger("src.utils"), logging.WARNING),
            reraise=True,
        )
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator


def slugify(text: str) -> str:
    """Convert text to a safe filename string.

    Args:
        text: Input text to slugify.

    Returns:
        Lowercase, alphanumeric-and-hyphen-only string.
    """
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")
