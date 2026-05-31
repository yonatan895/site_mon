from unittest.mock import MagicMock

import pytest

from src.models import SiteConfig, SourceEndpoint
from src.source_selector import SourceSelector, _endpoint_is_primary


@pytest.fixture
def primary_endpoint() -> SourceEndpoint:
    return SourceEndpoint(
        name="hmc-primary",
        url="https://primary.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/test",
    )


@pytest.fixture
def backup_endpoint() -> SourceEndpoint:
    return SourceEndpoint(
        name="hmc-backup-dr",
        url="https://backup.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/test",
    )


@pytest.fixture
def multi_endpoint_primary() -> SourceEndpoint:
    return SourceEndpoint(
        name="ds-main",
        url="https://ds-main.example.com",
        platform="ds",
        site="main",
        auth_type="basic",
        creds_vault_path="secret/test",
    )


@pytest.fixture
def multi_endpoint_secondary() -> SourceEndpoint:
    return SourceEndpoint(
        name="ds-dr",
        url="https://ds-dr.example.com",
        platform="ds",
        site="main",
        auth_type="basic",
        creds_vault_path="secret/test",
    )


@pytest.fixture
def mock_health_checker() -> MagicMock:
    hc = MagicMock()
    return hc


class TestFailoverMode:
    def test_primary_healthy_selects_primary(
        self,
        primary_endpoint: SourceEndpoint,
        backup_endpoint: SourceEndpoint,
        mock_health_checker: MagicMock,
    ) -> None:
        mock_health_checker.is_healthy.return_value = True
        site_config = SiteConfig(
            site_name="primary",
            platform="hmc",
            endpoints=[primary_endpoint, backup_endpoint],
        )
        selector = SourceSelector(
            platform="hmc",
            site_configs={"primary": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        active = selector.get_active_endpoints()
        assert len(active) == 1
        assert active[0].name == "hmc-primary"

    def test_failover_to_backup_when_primary_unhealthy(
        self,
        primary_endpoint: SourceEndpoint,
        backup_endpoint: SourceEndpoint,
        mock_health_checker: MagicMock,
    ) -> None:
        def is_healthy(name: str) -> bool:
            return "backup" in name or "dr" in name

        mock_health_checker.is_healthy.side_effect = is_healthy
        site_config = SiteConfig(
            site_name="primary",
            platform="hmc",
            endpoints=[primary_endpoint, backup_endpoint],
        )
        selector = SourceSelector(
            platform="hmc",
            site_configs={"primary": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        active = selector.get_active_endpoints()
        assert len(active) == 1
        assert active[0].name == "hmc-backup-dr"

    def test_both_unhealthy_returns_primary_as_last_resort(
        self,
        primary_endpoint: SourceEndpoint,
        backup_endpoint: SourceEndpoint,
        mock_health_checker: MagicMock,
    ) -> None:
        mock_health_checker.is_healthy.return_value = False
        site_config = SiteConfig(
            site_name="primary",
            platform="hmc",
            endpoints=[primary_endpoint, backup_endpoint],
        )
        selector = SourceSelector(
            platform="hmc",
            site_configs={"primary": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        active = selector.get_active_endpoints()
        assert len(active) == 1
        assert active[0].name == "hmc-primary"


class TestMultiEndpointMode:
    def test_all_healthy_returns_all(
        self,
        multi_endpoint_primary: SourceEndpoint,
        multi_endpoint_secondary: SourceEndpoint,
        mock_health_checker: MagicMock,
    ) -> None:
        mock_health_checker.is_healthy.return_value = True
        site_config = SiteConfig(
            site_name="main",
            platform="ds",
            endpoints=[multi_endpoint_primary, multi_endpoint_secondary],
        )
        selector = SourceSelector(
            platform="ds",
            site_configs={"main": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        active = selector.get_active_endpoints()
        assert len(active) == 2

    def test_some_unhealthy_returns_healthy_subset(
        self,
        multi_endpoint_primary: SourceEndpoint,
        multi_endpoint_secondary: SourceEndpoint,
        mock_health_checker: MagicMock,
    ) -> None:
        def is_healthy(name: str) -> bool:
            return "dr" not in name

        mock_health_checker.is_healthy.side_effect = is_healthy
        site_config = SiteConfig(
            site_name="main",
            platform="ds",
            endpoints=[multi_endpoint_primary, multi_endpoint_secondary],
        )
        selector = SourceSelector(
            platform="ds",
            site_configs={"main": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        active = selector.get_active_endpoints()
        assert len(active) == 1
        assert active[0].name == "ds-main"

    def test_all_unhealthy_returns_empty(
        self,
        multi_endpoint_primary: SourceEndpoint,
        multi_endpoint_secondary: SourceEndpoint,
        mock_health_checker: MagicMock,
    ) -> None:
        mock_health_checker.is_healthy.return_value = False
        site_config = SiteConfig(
            site_name="main",
            platform="ds",
            endpoints=[multi_endpoint_primary, multi_endpoint_secondary],
        )
        selector = SourceSelector(
            platform="ds",
            site_configs={"main": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        active = selector.get_active_endpoints()
        assert len(active) == 0


class TestEndpointIsPrimary:
    def test_primary_in_name(self) -> None:
        ep = SourceEndpoint(
            name="some-primary-ep",
            url="https://example.com",
            platform="hmc",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert _endpoint_is_primary(ep, "main") is True

    def test_backup_in_name(self) -> None:
        ep = SourceEndpoint(
            name="some-backup-ep",
            url="https://example.com",
            platform="hmc",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert _endpoint_is_primary(ep, "main") is False

    def test_dr_in_name(self) -> None:
        ep = SourceEndpoint(
            name="ep-dr",
            url="https://example.com",
            platform="hmc",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert _endpoint_is_primary(ep, "main") is False

    def test_secondary_in_name(self) -> None:
        ep = SourceEndpoint(
            name="ep-secondary",
            url="https://example.com",
            platform="hmc",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert _endpoint_is_primary(ep, "main") is False


class TestValidateEndpoint:
    def test_healthy_endpoint(self, mock_health_checker: MagicMock) -> None:
        mock_health_checker.is_healthy.return_value = True
        ep = SourceEndpoint(
            name="test-ep",
            url="https://example.com",
            platform="hmc",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        site_config = SiteConfig(
            site_name="main", platform="hmc", endpoints=[ep]
        )
        selector = SourceSelector(
            platform="hmc",
            site_configs={"main": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        assert selector.validate_endpoint(ep) is True

    def test_unhealthy_endpoint(self, mock_health_checker: MagicMock) -> None:
        mock_health_checker.is_healthy.return_value = False
        mock_health_checker.get_status.return_value = MagicMock(
            consecutive_failures=5
        )
        ep = SourceEndpoint(
            name="test-ep",
            url="https://example.com",
            platform="hmc",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        site_config = SiteConfig(
            site_name="main", platform="hmc", endpoints=[ep]
        )
        selector = SourceSelector(
            platform="hmc",
            site_configs={"main": site_config},
            health_checker=mock_health_checker,
            policy={},
        )
        assert selector.validate_endpoint(ep) is False
