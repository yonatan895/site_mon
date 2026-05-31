"""Endpoint health checking with background probing thread."""

import threading
import time
from datetime import UTC, datetime

import requests

from .models import HealthStatus, SourceEndpoint
from .utils import setup_logging

logger = setup_logging(__name__)

DEFAULT_HEALTH_CHECK_INTERVAL: int = 60
DEFAULT_HEALTH_PATH: str = "/health"
DEFAULT_HEALTH_TIMEOUT: int = 10


class EndpointHealthChecker:
    """Monitors endpoint health via periodic HTTP probes in a background thread.

    Thread-safe status tracking with support for degraded state transitions.
    """

    def __init__(
        self,
        endpoints: list[SourceEndpoint],
        check_interval: int = DEFAULT_HEALTH_CHECK_INTERVAL,
        health_path: str = DEFAULT_HEALTH_PATH,
    ) -> None:
        """Initialize the health checker.

        Args:
            endpoints: List of SourceEndpoint objects to monitor.
            check_interval: Seconds between health check cycles.
            health_path: URL path to use for health probes.
        """
        self.endpoints = endpoints
        self.check_interval = check_interval
        self.health_path = health_path
        self._statuses: dict[str, HealthStatus] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        for ep in endpoints:
            self._statuses[ep.name] = HealthStatus(endpoint_name=ep.name)

        logger.info(
            "health_checker_initialized",
            endpoint_count=len(endpoints),
            check_interval=check_interval,
        )

    def start(self) -> None:
        """Start the background health probe thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("health_checker_already_running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="health-checker", daemon=True
        )
        self._thread.start()
        logger.info("health_checker_started")

    def stop(self) -> None:
        """Signal the background thread to stop and wait for it to exit."""
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("health_checker_stopped")

    def _run_loop(self) -> None:
        """Main health check loop. Runs until stop event is set."""
        logger.info("health_check_loop_started")
        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            self._run_checks()
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0, self.check_interval - elapsed)
            self._stop_event.wait(timeout=sleep_time)
        logger.info("health_check_loop_stopped")

    def _run_checks(self) -> None:
        """Execute health probes for all configured endpoints."""
        for endpoint in self.endpoints:
            try:
                self._probe_endpoint(endpoint)
            except Exception:
                logger.exception(
                    "health_probe_unexpected_error", endpoint=endpoint.name
                )

    def _probe_endpoint(self, endpoint: SourceEndpoint) -> None:
        """Send a GET request to the endpoint's health path and update status.

        Args:
            endpoint: The SourceEndpoint to probe.
        """
        url = f"{endpoint.url.rstrip('/')}{self.health_path}"
        start_time = time.monotonic()

        try:
            response = requests.get(
                url,
                timeout=DEFAULT_HEALTH_TIMEOUT,
                verify=False,
            )
            elapsed_ms = (time.monotonic() - start_time) * 1000

            is_healthy = 200 <= response.status_code < 300

            with self._lock:
                status = self._statuses[endpoint.name]
                status.last_check = datetime.now(UTC)
                status.response_time_ms = elapsed_ms

                if is_healthy:
                    if status.consecutive_failures > 0:
                        logger.info(
                            "endpoint_recovered",
                            endpoint=endpoint.name,
                            previous_failures=status.consecutive_failures,
                        )
                    status.is_healthy = True
                    status.consecutive_failures = 0
                    status.degraded_since = None
                else:
                    self._record_failure(endpoint, status, f"HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            with self._lock:
                status = self._statuses[endpoint.name]
                status.last_check = datetime.now(UTC)
                status.response_time_ms = (time.monotonic() - start_time) * 1000
                self._record_failure(endpoint, status, "timeout")

        except requests.exceptions.ConnectionError as e:
            with self._lock:
                status = self._statuses[endpoint.name]
                status.last_check = datetime.now(UTC)
                status.response_time_ms = None
                self._record_failure(endpoint, status, f"connection_error: {e}")

        except Exception as e:
            with self._lock:
                status = self._statuses[endpoint.name]
                status.last_check = datetime.now(UTC)
                status.response_time_ms = None
                self._record_failure(endpoint, status, f"error: {e}")

    def _record_failure(
        self, endpoint: SourceEndpoint, status: HealthStatus, reason: str
    ) -> None:
        """Record a failed health check and transition state if needed.

        Args:
            endpoint: The endpoint that failed.
            status: Current HealthStatus to update.
            reason: Description of the failure.
        """
        status.consecutive_failures += 1
        status.is_healthy = False

        if status.degraded_since is None:
            status.degraded_since = datetime.now(UTC)

        if status.consecutive_failures >= status.max_consecutive_failures:
            logger.error(
                "endpoint_degraded",
                endpoint=endpoint.name,
                consecutive_failures=status.consecutive_failures,
                reason=reason,
            )
        else:
            logger.warning(
                "health_check_failed",
                endpoint=endpoint.name,
                consecutive_failures=status.consecutive_failures,
                reason=reason,
            )

    def get_status(self, endpoint_name: str) -> HealthStatus:
        """Get the current health status for an endpoint.

        Args:
            endpoint_name: Name of the endpoint.

        Returns:
            HealthStatus object for the endpoint.
        """
        with self._lock:
            return self._statuses.get(
                endpoint_name,
                HealthStatus(endpoint_name=endpoint_name, is_healthy=False),
            )

    def is_healthy(self, endpoint_name: str) -> bool:
        """Check if an endpoint is currently healthy.

        Args:
            endpoint_name: Name of the endpoint.

        Returns:
            True if the endpoint is healthy.
        """
        with self._lock:
            status = self._statuses.get(endpoint_name)
            return status.is_healthy if status else False

    def get_all_statuses(self) -> dict[str, HealthStatus]:
        """Get health status for all monitored endpoints.

        Returns:
            Dictionary mapping endpoint_name -> HealthStatus.
        """
        with self._lock:
            return dict(self._statuses)
