"""Endpoint health checker with passive failure tracking and active probing."""

import threading
import time
from collections import deque
from typing import Any

import structlog
import urllib3

from .models import SourceEndpoint
from .utils import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


class EndpointHealthChecker:
    """Tracks endpoint health via passive failure counting and active HTTP probing.

    Passive tracking: callers report success/failure after each API query.
    Active probing: a background thread periodically hits each endpoint's
    health_url (if configured) and updates the health state.
    """

    def __init__(
        self,
        endpoints: list[SourceEndpoint],
        failure_threshold: int = 3,
        probe_interval: int = 60,
        probe_timeout: int = 10,
    ) -> None:
        self.endpoints = {ep.name: ep for ep in endpoints}
        self.failure_threshold = failure_threshold
        self.probe_interval = probe_interval

        # Per-endpoint state
        self._failures: dict[str, int] = {ep.name: 0 for ep in endpoints}
        self._healthy: dict[str, bool] = {ep.name: True for ep in endpoints}
        self._recent_latencies: dict[str, deque] = {
            ep.name: deque(maxlen=20) for ep in endpoints
        }
        self._lock = threading.Lock()

        self._pool = urllib3.PoolManager(
            num_pools=len(endpoints) + 1,
            maxsize=2,
            timeout=urllib3.Timeout(total=probe_timeout),
            retries=urllib3.Retry(total=0),
        )
        self._stop_event = threading.Event()
        self._probe_thread: threading.Thread | None = None

        logger.info(
            "health_checker_initialized",
            endpoints=list(self.endpoints.keys()),
            failure_threshold=failure_threshold,
            probe_interval=probe_interval,
        )

    # ------------------------------------------------------------------
    # Passive tracking (called by pollers after each query)
    # ------------------------------------------------------------------

    def report_success(self, endpoint_name: str, latency_ms: float = 0.0) -> None:
        """Record a successful query. Resets failure count; marks endpoint healthy."""
        with self._lock:
            self._failures[endpoint_name] = 0
            self._healthy[endpoint_name] = True
            if latency_ms:
                self._recent_latencies[endpoint_name].append(latency_ms)

    def report_failure(self, endpoint_name: str, error: str = "") -> None:
        """Record a failed query. Marks endpoint unhealthy after threshold breached."""
        with self._lock:
            self._failures[endpoint_name] = self._failures.get(endpoint_name, 0) + 1
            count = self._failures[endpoint_name]
            if count >= self.failure_threshold:
                self._healthy[endpoint_name] = False
                logger.warning(
                    "endpoint_marked_unhealthy",
                    endpoint=endpoint_name,
                    failures=count,
                    error=error,
                )

    def is_healthy(self, endpoint_name: str) -> bool:
        with self._lock:
            return self._healthy.get(endpoint_name, True)

    def get_failure_count(self, endpoint_name: str) -> int:
        with self._lock:
            return self._failures.get(endpoint_name, 0)

    def get_avg_latency_ms(self, endpoint_name: str) -> float | None:
        with self._lock:
            lats = list(self._recent_latencies.get(endpoint_name, []))
        if not lats:
            return None
        return sum(lats) / len(lats)

    def get_all_status(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {
                name: {
                    "healthy": self._healthy.get(name, True),
                    "failures": self._failures.get(name, 0),
                    "avg_latency_ms": (
                        (
                            sum(self._recent_latencies[name])
                            / len(self._recent_latencies[name])
                        )
                        if self._recent_latencies.get(name)
                        else None
                    ),
                }
                for name in self.endpoints
            }

    # ------------------------------------------------------------------
    # Active probing
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background health-probe thread."""
        self._stop_event.clear()
        self._probe_thread = threading.Thread(
            target=self._probe_loop,
            name="health-probe",
            daemon=True,
        )
        self._probe_thread.start()
        logger.info("health_probe_thread_started")

    def stop(self) -> None:
        """Signal the probe thread to stop and wait for it."""
        self._stop_event.set()
        if self._probe_thread and self._probe_thread.is_alive():
            self._probe_thread.join(timeout=5)
        logger.info("health_probe_thread_stopped")

    def _probe_loop(self) -> None:
        while not self._stop_event.is_set():
            for name, endpoint in list(self.endpoints.items()):
                if not endpoint.health_url:
                    continue
                self._probe_endpoint(name, endpoint)
            self._stop_event.wait(timeout=self.probe_interval)

    def _probe_endpoint(self, name: str, endpoint: SourceEndpoint) -> None:
        start = time.monotonic()
        try:
            resp = self._pool.request("GET", endpoint.health_url)
            latency_ms = (time.monotonic() - start) * 1000
            if resp.status < 400:
                self.report_success(name, latency_ms)
                logger.debug(
                    "probe_success",
                    endpoint=name,
                    status=resp.status,
                    latency_ms=round(latency_ms, 1),
                )
            else:
                self.report_failure(
                    name,
                    error=f"HTTP {resp.status}",
                )
                logger.warning(
                    "probe_bad_status",
                    endpoint=name,
                    status=resp.status,
                )
        except Exception as exc:
            self.report_failure(name, error=str(exc))
            logger.warning("probe_failed", endpoint=name, error=str(exc))
