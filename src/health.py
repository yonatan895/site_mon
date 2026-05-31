"""Health, readiness, and metrics endpoints for the monitoring pipeline."""

import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import structlog

from .utils import setup_logging

logger = setup_logging(__name__)

# Global references set by the running component (poller or sender)
health_checker: Optional[Any] = None
spool_manager: Optional[Any] = None

# Metrics storage
_metrics_store: dict[str, Any] = {
    "spool_pending_count": 0,
    "spool_size_mb": 0.0,
    "spool_dead_letter_count": 0,
    "batch_send_duration_seconds": [],
    "batch_send_errors_total": 0,
    "endpoint_health_status": {},
    "polling_cycle_duration_seconds": [],
    "api_query_errors_total": 0,
}

# Try to use prometheus_client if available, otherwise use simple counters
try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest, REGISTRY

    PROMETHEUS_AVAILABLE = True

    spool_pending = Gauge(
        "site_mon_spool_pending_count",
        "Number of pending spool files",
    )
    spool_size = Gauge(
        "site_mon_spool_size_mb",
        "Total spool size in megabytes",
    )
    spool_dead_letter = Gauge(
        "site_mon_spool_dead_letter_count",
        "Number of dead letter files",
    )
    batch_send_duration = Histogram(
        "site_mon_batch_send_duration_seconds",
        "Duration of batch send operations",
        buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60],
    )
    batch_send_errors = Counter(
        "site_mon_batch_send_errors_total",
        "Total batch send errors",
    )
    endpoint_health = Gauge(
        "site_mon_endpoint_health",
        "Endpoint health status (1 = healthy, 0 = unhealthy)",
        ["endpoint"],
    )
    polling_cycle_duration = Histogram(
        "site_mon_polling_cycle_duration_seconds",
        "Duration of polling cycles",
        buckets=[1, 5, 10, 30, 60, 120, 300, 600],
    )
    api_query_errors = Counter(
        "site_mon_api_query_errors_total",
        "Total API query errors",
    )

except ImportError:
    PROMETHEUS_AVAILABLE = False


def _build_app() -> Any:
    """Build a FastAPI/Starlette application with health endpoints.

    Uses Starlette directly for minimal dependency footprint.

    Returns:
        A callable WSGI/ASGI application.
    """
    try:
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse, PlainTextResponse
        from starlette.routing import Route
    except ImportError:
        logger.error("starlette_not_installed")
        raise

    async def healthz(request: Any) -> JSONResponse:
        """Kubernetes liveness probe endpoint.

        Returns:
            JSON with status 'ok' if the process is alive.
        """
        return JSONResponse({
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        })

    async def readyz(request: Any) -> JSONResponse:
        """Kubernetes readiness probe endpoint.

        Checks that the spool is not full and, if a poller, at least one
        endpoint is healthy.

        Returns:
            JSON with status 'ready' or 'not_ready'.
        """
        checks = []

        if spool_manager is not None:
            stats = spool_manager.get_spool_stats()
            if stats["total_size_mb"] > 1000:
                return JSONResponse({
                    "status": "not_ready",
                    "reason": "spool_full",
                    "spool_size_mb": stats["total_size_mb"],
                })
            checks.append({"spool": "ok", "spool_size_mb": stats["total_size_mb"]})

        if health_checker is not None:
            all_statuses = health_checker.get_all_statuses()
            healthy_count = sum(
                1 for s in all_statuses.values() if s.is_healthy
            )
            if healthy_count == 0:
                return JSONResponse({
                    "status": "not_ready",
                    "reason": "no_healthy_endpoints",
                })
            checks.append({"healthy_endpoints": healthy_count})

        return JSONResponse({
            "status": "ready",
            "checks": checks,
        })

    async def metrics(request: Any) -> PlainTextResponse:
        """Prometheus metrics endpoint.

        Returns:
            Prometheus text format metrics.
        """
        if PROMETHEUS_AVAILABLE:
            _update_prometheus_metrics()
            content = generate_latest(REGISTRY).decode("utf-8")
            return PlainTextResponse(content, media_type="text/plain")

        # Fallback: simple text metrics
        lines = []
        if spool_manager is not None:
            stats = spool_manager.get_spool_stats()
            lines.append(f"# HELP site_mon_spool_pending_count Number of pending spool files")
            lines.append(f"# TYPE site_mon_spool_pending_count gauge")
            lines.append(f"site_mon_spool_pending_count {stats['pending_count']}")
            lines.append(f"# HELP site_mon_spool_size_mb Total spool size in MB")
            lines.append(f"# TYPE site_mon_spool_size_mb gauge")
            lines.append(f"site_mon_spool_size_mb {stats['total_size_mb']}")
            lines.append(f"# HELP site_mon_spool_dead_letter_count Dead letter file count")
            lines.append(f"# TYPE site_mon_spool_dead_letter_count gauge")
            lines.append(f"site_mon_spool_dead_letter_count {stats['dead_letter_count']}")

        if health_checker is not None:
            all_statuses = health_checker.get_all_statuses()
            lines.append(f"# HELP site_mon_endpoint_health_status Endpoint health (1=healthy, 0=unhealthy)")
            lines.append(f"# TYPE site_mon_endpoint_health_status gauge")
            for name, status in all_statuses.items():
                lines.append(
                    f'site_mon_endpoint_health_status{{endpoint="{name}"}} '
                    f'{1 if status.is_healthy else 0}'
                )

        lines.append("")
        return PlainTextResponse("\n".join(lines), media_type="text/plain")

    routes = [
        Route("/healthz", healthz, methods=["GET"]),
        Route("/readyz", readyz, methods=["GET"]),
        Route("/metrics", metrics, methods=["GET"]),
    ]

    app = Starlette(debug=False, routes=routes)
    logger.info("health_app_created", routes=["/healthz", "/readyz", "/metrics"])
    return app


def _update_prometheus_metrics() -> None:
    """Update Prometheus gauge values from current state."""
    if not PROMETHEUS_AVAILABLE:
        return

    if spool_manager is not None:
        stats = spool_manager.get_spool_stats()
        spool_pending.set(stats["pending_count"])
        spool_size.set(stats["total_size_mb"])
        spool_dead_letter.set(stats["dead_letter_count"])

    if health_checker is not None:
        all_statuses = health_checker.get_all_statuses()
        for name, status in all_statuses.items():
            endpoint_health.labels(endpoint=name).set(
                1 if status.is_healthy else 0
            )


# Build the application at import time
app = _build_app()


def init_health(health_checker_instance: Any = None, spool_manager_instance: Any = None) -> None:
    """Initialize global references for the health endpoints.

    Called by the main poller or sender process after component setup.

    Args:
        health_checker_instance: EndpointHealthChecker instance.
        spool_manager_instance: SpoolManager instance.
    """
    global health_checker, spool_manager
    if health_checker_instance is not None:
        health_checker = health_checker_instance
    if spool_manager_instance is not None:
        spool_manager = spool_manager_instance
    logger.info("health_endpoints_initialized")
