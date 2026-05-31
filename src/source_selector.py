"""Source endpoint selection based on health status and platform policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .models import SiteConfig, SourceEndpoint
from .utils import setup_logging

if TYPE_CHECKING:
    from .endpoint_health import EndpointHealthChecker

logger = setup_logging(__name__)

# Platforms that use primary/backup failover pattern
FAILOVER_PLATFORMS = {"hmc", "ts7700"}

# Platforms where all healthy endpoints are used simultaneously
MULTI_ENDPOINT_PLATFORMS = {"ds", "csm"}


class SourceSelector:
    """Selects active source endpoints based on health and policy rules.

    For HMC/TS7700 platforms, only the primary endpoint is used unless unhealthy,
    in which case the backup takes over. For DS8K/CSM platforms, all healthy
    endpoints are used simultaneously.
    """

    def __init__(
        self,
        platform: str,
        site_configs: dict[str, SiteConfig],
        health_checker: EndpointHealthChecker,
        policy: dict[str, Any],
    ) -> None:
        """Initialize the source selector.

        Args:
            platform: Platform identifier.
            site_configs: Dictionary of site_name -> SiteConfig.
            health_checker: EndpointHealthChecker instance for health lookups.
            policy: Source selection policy dictionary.
        """
        self.platform = platform
        self.site_configs = site_configs
        self.health_checker = health_checker
        self.policy = policy
        logger.info(
            "source_selector_initialized",
            platform=platform,
            sites=list(site_configs.keys()),
        )

    def get_active_endpoints(self) -> list[SourceEndpoint]:
        """Determine which endpoints should be polled based on health and policy.

        Returns:
            List of SourceEndpoint objects that are currently active.
        """
        active: list[SourceEndpoint] = []

        for site_name, site_config in self.site_configs.items():
            endpoints = site_config.endpoints
            if not endpoints:
                logger.warning("no_endpoints_configured", site=site_name)
                continue

            if self.platform.lower() in FAILOVER_PLATFORMS:
                selected = self._select_failover(site_name, endpoints)
            else:
                selected = self._select_all_healthy(site_name, endpoints)

            active.extend(selected)

        logger.info(
            "active_endpoints_selected",
            platform=self.platform,
            count=len(active),
            endpoints=[ep.name for ep in active],
        )
        return active

    def _select_failover(
        self, site_name: str, endpoints: list[SourceEndpoint]
    ) -> list[SourceEndpoint]:
        """Select primary or backup for failover platforms (HMC, TS7700).

        Args:
            site_name: Site name for logging.
            endpoints: List of all configured endpoints for the site.

        Returns:
            List containing the single active endpoint.
        """
        primary_endpoints = [ep for ep in endpoints if _endpoint_is_primary(ep, site_name)]
        backup_endpoints = [ep for ep in endpoints if not _endpoint_is_primary(ep, site_name)]

        primary = primary_endpoints[0] if primary_endpoints else None
        backup = backup_endpoints[0] if backup_endpoints else None

        # Check primary health
        if primary and self.validate_endpoint(primary):
            logger.debug("using_primary_endpoint", site=site_name, endpoint=primary.name)
            return [primary]

        if backup and self.validate_endpoint(backup):
            logger.info("failing_over_to_backup", site=site_name, endpoint=backup.name)
            return [backup]

        # Neither healthy - return primary as last resort for logging
        if primary:
            logger.error("all_endpoints_unhealthy", site=site_name)
            return [primary]

        logger.error("no_viable_endpoints", site=site_name)
        return []

    def _select_all_healthy(
        self, site_name: str, endpoints: list[SourceEndpoint]
    ) -> list[SourceEndpoint]:
        """Select all healthy endpoints for multi-endpoint platforms (DS8K, CSM).

        Args:
            site_name: Site name for logging.
            endpoints: List of all configured endpoints for the site.

        Returns:
            List of healthy endpoints.
        """
        healthy = [ep for ep in endpoints if self.validate_endpoint(ep)]
        unhealthy_count = len(endpoints) - len(healthy)
        if unhealthy_count > 0:
            logger.warning(
                "some_endpoints_unhealthy",
                site=site_name,
                healthy=len(healthy),
                unhealthy=unhealthy_count,
            )
        return healthy

    def validate_endpoint(self, endpoint: SourceEndpoint) -> bool:
        """Check if an endpoint passes health criteria.

        Args:
            endpoint: The SourceEndpoint to validate.

        Returns:
            True if the endpoint is considered healthy.
        """
        is_healthy = self.health_checker.is_healthy(endpoint.name)
        if not is_healthy:
            status = self.health_checker.get_status(endpoint.name)
            logger.warning(
                "endpoint_unhealthy",
                endpoint=endpoint.name,
                consecutive_failures=status.consecutive_failures if status else 0,
            )
        return is_healthy


def _endpoint_is_primary(endpoint: SourceEndpoint, site_name: str) -> bool:
    """Determine if an endpoint is the primary for its site.

    Heuristic: primary endpoints have 'primary' or 'main' in their name,
    or are the first endpoint without 'backup' or 'dr' or 'secondary' in name.

    Args:
        endpoint: The endpoint to check.
        site_name: Site name for context.

    Returns:
        True if the endpoint is considered primary.
    """
    name_lower = endpoint.name.lower()
    if "primary" in name_lower or "_pri" in name_lower:
        return True
    return not ("backup" in name_lower or "dr" in name_lower or "secondary" in name_lower)
