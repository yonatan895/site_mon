"""FastAPI health endpoints for the poller and sender containers.

Mounts at / and exposes:
  GET /healthz   – liveness  (always 200 unless the process is wedged)
  GET /readyz    – readiness (200 when at least one endpoint is healthy
                              and spool is not critically full)
  GET /metrics   – Prometheus text metrics
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import structlog
from fastapi import FastAPI, Response
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from .utils import configure_logging

if TYPE_CHECKING:
    from .endpoint_health import EndpointHealthChecker
    from .spool import SpoolManager

configure_logging()
logger = structlog.get_logger(__name__)

app = FastAPI(title="site-mon health", docs_url=None, redoc_url=None)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

polling_cycle_duration = Histogram(
    "polling_cycle_duration_seconds",
    "End-to-end polling cycle wall-clock time",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

api_query_errors = Counter(
    "api_query_errors_total",
    "Number of API query failures across all endpoints",
)

spool_size_mb = Gauge(
    "spool_size_mb",
    "Current spool directory size in megabytes",
)

spool_pending_files = Gauge(
    "spool_pending_files",
    "Number of pending NDJSON files in the spool directory",
)

# ---------------------------------------------------------------------------
# Runtime state injected by the container entry-point
# ---------------------------------------------------------------------------

_health_checker: EndpointHealthChecker | None = None
_spool_manager: SpoolManager | None = None
_start_time: float = time.time()


def init_health(
    health_checker_instance: EndpointHealthChecker,
    spool_manager_instance: SpoolManager,
) -> None:
    """Inject runtime dependencies. Must be called before the server starts."""
    global _health_checker, _spool_manager  # noqa: PLW0603
    _health_checker = health_checker_instance
    _spool_manager = spool_manager_instance
    logger.info("health_module_initialized")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/healthz")
def liveness() -> dict:
    """Liveness probe – always 200 while the process is running."""
    return {"status": "ok", "uptime_seconds": round(time.time() - _start_time, 1)}


@app.get("/readyz")
def readiness(response: Response) -> dict:
    """Readiness probe – 503 if no endpoints are healthy or spool is critically full."""
    issues: list[str] = []

    if _health_checker is not None:
        all_status = _health_checker.get_all_status()
        unhealthy = [name for name, s in all_status.items() if not s["healthy"]]
        if unhealthy:
            issues.append(f"Unhealthy endpoints: {', '.join(unhealthy)}")
        if all_status and all(not s["healthy"] for s in all_status.values()):
            issues.append("All endpoints are unhealthy")

    if _spool_manager is not None:
        stats = _spool_manager.get_spool_stats()
        pending = stats.get("pending_count", 0)
        size_mb = stats.get("total_size_mb", 0.0)
        spool_pending_files.set(pending)
        spool_size_mb.set(size_mb)
        if size_mb > 900:
            issues.append(f"Spool critically full: {size_mb:.1f} MB")

    if issues:
        response.status_code = 503
        logger.warning("readiness_probe_failed", issues=issues)
        return {"status": "not ready", "issues": issues}

    return {"status": "ready"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
