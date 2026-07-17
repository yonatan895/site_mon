"""Source selector: picks the best endpoint(s) for each polling cycle.

Supports three strategies:
  round_robin   – cycles through healthy endpoints in order
  primary_only  – always uses the first healthy endpoint
  weighted      – probabilistic selection based on configured weights
"""

import random
import threading
from typing import Any

import structlog

from .endpoint_health import EndpointHealthChecker
from .models import SelectionPolicy, SiteConfig, SourceEndpoint
from .utils import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


class SourceSelector:
    """Selects healthy source endpoints according to the configured policy.

    Thread-safe: internal counters are protected by a lock.
    """

    def __init__(
        self,
        platform: str,
        site_configs: dict[str, SiteConfig],
        health_checker: EndpointHealthChecker,
        policy: SelectionPolicy | None = None,
    ) -> None:
        self.platform = platform
        self.site_configs = site_configs
        self.health_checker = health_checker
        self.policy = policy or SelectionPolicy()
        self._lock = threading.Lock()
        self._rr_index: int = 0

        all_endpoints = [
            ep
            for sc in site_configs.values()
            for ep in sc.endpoints
            if ep.platform.lower() == platform.lower()
        ]
        self._all_endpoints: list[SourceEndpoint] = all_endpoints

        logger.info(
            "source_selector_initialized",
            platform=platform,
            strategy=self.policy.strategy,
            endpoint_count=len(self._all_endpoints),
        )

    def get_active_endpoints(self) -> list[SourceEndpoint]:
        """Return the endpoint(s) to use for the next polling cycle.

        Only healthy endpoints are considered.
        """
        healthy = [
            ep
            for ep in self._all_endpoints
            if self.health_checker.is_healthy(ep.name)
        ]
        if not healthy:
            logger.warning(
                "no_healthy_endpoints",
                platform=self.platform,
                total=len(self._all_endpoints),
            )
            return []

        strategy = self.policy.strategy
        if strategy == "primary_only":
            return [healthy[0]]
        if strategy == "weighted":
            return [self._weighted_choice(healthy)]
        # default: round_robin – return all healthy, ordered from current index
        return self._round_robin_order(healthy)

    def _round_robin_order(
        self, healthy: list[SourceEndpoint]
    ) -> list[SourceEndpoint]:
        """Return healthy endpoints starting from the current round-robin position."""
        with self._lock:
            idx = self._rr_index % len(healthy)
            self._rr_index = (self._rr_index + 1) % len(healthy)
        return healthy[idx:] + healthy[:idx]

    def _weighted_choice(self, healthy: list[SourceEndpoint]) -> SourceEndpoint:
        """Pick one endpoint probabilistically based on configured weights."""
        weights = [
            float(self.policy.weights.get(ep.name, 1.0)) for ep in healthy
        ]
        return random.choices(healthy, weights=weights, k=1)[0]

    def get_endpoint_stats(self) -> dict[str, Any]:
        """Return health + latency stats for all known endpoints."""
        return self.health_checker.get_all_status()
