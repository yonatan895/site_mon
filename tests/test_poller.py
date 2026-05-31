import json
from unittest.mock import MagicMock

import pytest

from src.models import PlatformRule, PollingEvent, SourceEndpoint


@pytest.fixture
def mock_poller_deps() -> dict:
    platform_rules = {
        "cpc-stats": PlatformRule(
            name="cpc-stats",
            data_type="cpc-stats",
            sourcetype="hmc:cpc_stats",
            index="mainframe_metrics",
        ),
    }
    site_configs = {
        "primary": MagicMock(
            site_name="primary",
            platform="hmc",
            endpoints=[MagicMock(name="hmc-primary")],
            data_types=["cpc-stats"],
        )
    }
    policy = {"failover_mode": "primary_backup"}

    health_checker = MagicMock()
    health_checker.is_healthy.return_value = True
    health_checker.get_all_statuses.return_value = {}

    source_selector = MagicMock()
    endpoint = SourceEndpoint(
        name="hmc-primary",
        url="https://hmc.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/test",
    )
    source_selector.get_active_endpoints.return_value = [endpoint]

    return {
        "platform_rules": platform_rules,
        "site_configs": site_configs,
        "policy": policy,
        "health_checker": health_checker,
        "source_selector": source_selector,
    }


class TestEventsToHECLines:
    def test_single_event(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        event = PollingEvent(
            platform="hmc",
            site="primary",
            data_type="cpc-stats",
            sourcetype="hmc:cpc_stats",
            index="mainframe_metrics",
            fields={"cpc_name": "CPCA"},
        )
        lines = poller._events_to_hec_lines(event, endpoint)
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["host"] == "test-ep"
        assert parsed["source"] == "hmc:cpc-stats"
        assert parsed["sourcetype"] == "hmc:cpc_stats"
        assert parsed["index"] == "mainframe_metrics"

    def test_list_of_events(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        events = [
            PollingEvent(
                platform="hmc",
                site="primary",
                data_type="cpc-stats",
                sourcetype="hmc:cpc_stats",
                index="mainframe_metrics",
                fields={"cpc_name": "CPA"},
            ),
            PollingEvent(
                platform="hmc",
                site="primary",
                data_type="cpc-stats",
                sourcetype="hmc:cpc_stats",
                index="mainframe_metrics",
                fields={"cpc_name": "CPB"},
            ),
        ]
        lines = poller._events_to_hec_lines(events, endpoint)
        assert len(lines) == 2

    def test_empty_list(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        lines = poller._events_to_hec_lines([], endpoint)
        assert lines == []

    def test_none_returns_empty(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        lines = poller._events_to_hec_lines(None, endpoint)
        assert lines == []


class TestPollingEventToHEC:
    def test_correct_format(self) -> None:

        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        event = PollingEvent(
            platform="hmc",
            site="primary",
            data_type="cpc-stats",
            sourcetype="hmc:cpc_stats",
            index="mainframe_metrics",
        )
        hec = poller._polling_event_to_hec(event, endpoint)
        assert hec["host"] == "test-ep"
        assert hec["source"] == "hmc:cpc-stats"
        assert hec["sourcetype"] == "hmc:cpc_stats"
        assert hec["index"] == "mainframe_metrics"
        assert "time" in hec
        assert "event" in hec


class TestDictToHECLine:
    def test_correct_format(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        event_dict = {"cpc_name": "CPA", "sourcetype": "hmc:custom"}
        hec = poller._dict_to_hec_line(event_dict, endpoint)
        assert hec["host"] == "test-ep"
        assert hec["sourcetype"] == "hmc:custom"
        assert "time" in hec
        assert "event" in hec


class TestCreateClient:
    def test_hmc_client(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        from src.poller import HMCClient

        client = poller._create_client(endpoint)
        assert isinstance(client, HMCClient)

    def test_ds8k_client(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="ds",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        from src.poller import DS8000Client

        client = poller._create_client(endpoint)
        assert isinstance(client, DS8000Client)

    def test_csm_client(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="csm",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        from src.poller import CSMClient

        client = poller._create_client(endpoint)
        assert isinstance(client, CSMClient)

    def test_ts7700_client(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="ts7700",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        from src.poller import TS7700Client

        client = poller._create_client(endpoint)
        assert isinstance(client, TS7700Client)

    def test_unsupported_platform_raises(self) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="unknown",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        with pytest.raises(ValueError, match="Unsupported platform"):
            poller._create_client(endpoint)
