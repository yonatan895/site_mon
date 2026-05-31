import contextlib
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from src.models import PlatformRule, PollingEvent, SourceEndpoint
from src.spool import SpoolManager


@pytest.fixture
def mock_endpoint() -> SourceEndpoint:
    return SourceEndpoint(
        name="hmc-primary",
        url="https://hmc.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/test",
    )


@pytest.fixture
def endpoint() -> SourceEndpoint:
    return SourceEndpoint(
        name="test-ep",
        url="https://test.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/test",
    )


class TestEventsToHECLines:
    def test_single_event(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
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
        assert parsed["sourcetype"] == "hmc:cpc_stats"

    def test_list_of_events(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
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

    def test_empty_list(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        assert poller._events_to_hec_lines([], endpoint) == []

    def test_none_returns_empty(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        assert poller._events_to_hec_lines(None, endpoint) == []

    def test_dict_events(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        lines = poller._events_to_hec_lines([{"cpc_name": "CPA"}], endpoint)
        assert len(lines) == 1


class TestPollingEventToHEC:
    def test_correct_format(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        event = PollingEvent(
            platform="hmc",
            site="primary",
            data_type="cpc-stats",
            sourcetype="hmc:cpc_stats",
            index="mainframe_metrics",
        )
        hec = poller._polling_event_to_hec(event, endpoint)
        assert hec["host"] == "test-ep"
        assert hec["sourcetype"] == "hmc:cpc_stats"
        assert "time" in hec
        assert "event" in hec


class TestDictToHECLine:
    def test_correct_format(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        hec = poller._dict_to_hec_line({"cpc_name": "CPA", "sourcetype": "hmc:custom"}, endpoint)
        assert hec["host"] == "test-ep"
        assert hec["sourcetype"] == "hmc:custom"

    def test_fallback_sourcetype(self, endpoint: SourceEndpoint) -> None:
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        hec = poller._dict_to_hec_line({"cpc_name": "CPA"}, endpoint)
        assert hec["sourcetype"] == "hmc:data"


class TestCreateClient:
    def test_hmc_client(self) -> None:
        from src.poller import HMCClient
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
        assert isinstance(poller._create_client(endpoint), HMCClient)

    def test_ds8k_client(self) -> None:
        from src.poller import DS8000Client
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
        assert isinstance(poller._create_client(endpoint), DS8000Client)

    def test_ds8000_client(self) -> None:
        from src.poller import DS8000Client
        from src.poller import Poller as PollerCls

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="ds8k",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert isinstance(poller._create_client(endpoint), DS8000Client)

    def test_csm_client(self) -> None:
        from src.poller import CSMClient
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
        assert isinstance(poller._create_client(endpoint), CSMClient)

    def test_ts7700_client(self) -> None:
        from src.poller import Poller as PollerCls
        from src.poller import TS7700Client

        poller = PollerCls.__new__(PollerCls)
        endpoint = SourceEndpoint(
            name="test-ep",
            url="https://test.example.com",
            platform="ts7700",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert isinstance(poller._create_client(endpoint), TS7700Client)

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


class TestHMCClient:
    @pytest.fixture
    def hmc_endpoint(self) -> SourceEndpoint:
        return SourceEndpoint(
            name="hmc-test",
            url="https://hmc.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )

    @pytest.fixture
    def mock_session(self) -> MagicMock:
        return MagicMock()

    def test_connect(self, hmc_endpoint: SourceEndpoint, mock_session: MagicMock) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_zhmc.Session.return_value = mock_session
            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.poller import HMCClient

                client = HMCClient(hmc_endpoint)
                result = client._connect()
                assert result is mock_session

    def test_connect_reuses_session(
        self, hmc_endpoint: SourceEndpoint, mock_session: MagicMock
    ) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_zhmc.Session.return_value = mock_session
            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.poller import HMCClient

                client = HMCClient(hmc_endpoint)
                client._connect()
                client._connect()
                assert mock_zhmc.Session.call_count == 1

    def test_load_creds(self, hmc_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "myuser", "HMC_PRIMARY_PASSWORD": "mypass"}
        ):
            from src.poller import HMCClient

            client = HMCClient(hmc_endpoint)
            creds = client._load_creds()
            assert creds["username"] == "myuser"
            assert creds["password"] == "mypass"

    def test_load_creds_defaults(self, hmc_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {}, clear=True):
            from src.poller import HMCClient

            client = HMCClient(hmc_endpoint)
            creds = client._load_creds()
            assert creds["username"] == "admin"
            assert creds["password"] == ""

    def test_query_cpc_stats(self, hmc_endpoint: SourceEndpoint, mock_session: MagicMock) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_zhmc.Session.return_value = mock_session
            mock_cpc = MagicMock()
            mock_cpc.properties = {"cpc_name": "CPCA", "status": "operating"}
            mock_cpc.pull_full_properties = MagicMock()
            mock_client = MagicMock()
            mock_client.cpcs.list.return_value = [mock_cpc]
            mock_zhmc.Client.return_value = mock_client
            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.poller import HMCClient

                client = HMCClient(hmc_endpoint)
                result = client.query(
                    "cpc-stats",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["cpc_name"] == "CPCA"

    def test_query_lpars(self, hmc_endpoint: SourceEndpoint, mock_session: MagicMock) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_zhmc.Session.return_value = mock_session
            mock_lpar = MagicMock()
            mock_lpar.properties = {"name": "LPAR01", "status": "active"}
            mock_lpar.pull_full_properties = MagicMock()
            mock_cpc = MagicMock()
            mock_cpc.properties = {"name": "CPCA"}
            mock_cpc.lpars.list.return_value = [mock_lpar]
            mock_client = MagicMock()
            mock_client.cpcs.list.return_value = [mock_cpc]
            mock_zhmc.Client.return_value = mock_client
            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.poller import HMCClient

                client = HMCClient(hmc_endpoint)
                result = client.query(
                    "lpars",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["name"] == "LPAR01"
                assert result[0]["cpc_name"] == "CPCA"

    def test_query_chpids(self, hmc_endpoint: SourceEndpoint, mock_session: MagicMock) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_zhmc.Session.return_value = mock_session
            mock_adapter = MagicMock()
            mock_adapter.properties = {"adapter_id": "CHP01"}
            mock_adapter.pull_full_properties = MagicMock()
            mock_cpc = MagicMock()
            mock_cpc.properties = {"name": "CPCA"}
            mock_cpc.adapters.list.return_value = [mock_adapter]
            mock_client = MagicMock()
            mock_client.cpcs.list.return_value = [mock_cpc]
            mock_zhmc.Client.return_value = mock_client
            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.poller import HMCClient

                client = HMCClient(hmc_endpoint)
                result = client.query(
                    "chpid",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["adapter_id"] == "CHP01"

    def test_query_unknown_type(
        self, hmc_endpoint: SourceEndpoint, mock_session: MagicMock
    ) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_zhmc.Session.return_value = mock_session
            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.poller import HMCClient

                client = HMCClient(hmc_endpoint)
                result = client.query(
                    "unknown",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result == []


class TestDS8000Client:
    @pytest.fixture
    def ds_endpoint(self) -> SourceEndpoint:
        return SourceEndpoint(
            name="ds-test",
            url="https://ds.example.com",
            platform="ds",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )

    def test_connect(self, ds_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"DS_MAIN_USERNAME": "admin", "DS_MAIN_PASSWORD": "pass"}):
            mock_pyds8k = MagicMock()
            mock_conn = MagicMock()
            mock_pyds8k.client.DS8KClient.return_value = mock_conn
            with patch.dict(
                sys.modules, {"pyds8k": mock_pyds8k, "pyds8k.client": mock_pyds8k.client}
            ):
                from src.poller import DS8000Client

                client = DS8000Client(ds_endpoint)
                result = client._connect()
                assert result is mock_conn

    def test_query_arrays(self, ds_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"DS_MAIN_USERNAME": "admin", "DS_MAIN_PASSWORD": "pass"}):
            mock_pyds8k = MagicMock()
            mock_system = MagicMock()
            mock_system.id = "sys1"
            mock_system.name = "DS8K-1"
            mock_system.state = "online"
            mock_system.capacity = "100TB"
            mock_system.bundle_version = "8.5"
            mock_conn = MagicMock()
            mock_conn.get_systems.return_value = [mock_system]
            mock_pyds8k.client.DS8KClient.return_value = mock_conn
            with patch.dict(
                sys.modules, {"pyds8k": mock_pyds8k, "pyds8k.client": mock_pyds8k.client}
            ):
                from src.poller import DS8000Client

                client = DS8000Client(ds_endpoint)
                result = client.query(
                    "arrays",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["id"] == "sys1"

    def test_query_ports(self, ds_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"DS_MAIN_USERNAME": "admin", "DS_MAIN_PASSWORD": "pass"}):
            mock_pyds8k = MagicMock()
            mock_port = MagicMock()
            mock_port.id = "port1"
            mock_port.wwpn = "wwpn1"
            mock_port.state = "online"
            mock_port.speed = "16G"
            mock_port.type = "FICON"
            mock_system = MagicMock()
            mock_system.id = "sys1"
            mock_conn = MagicMock()
            mock_conn.get_systems.return_value = [mock_system]
            mock_conn.get_ioports.return_value = [mock_port]
            mock_pyds8k.client.DS8KClient.return_value = mock_conn
            with patch.dict(
                sys.modules, {"pyds8k": mock_pyds8k, "pyds8k.client": mock_pyds8k.client}
            ):
                from src.poller import DS8000Client

                client = DS8000Client(ds_endpoint)
                result = client.query(
                    "ports",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["wwpn"] == "wwpn1"

    def test_query_ranks(self, ds_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"DS_MAIN_USERNAME": "admin", "DS_MAIN_PASSWORD": "pass"}):
            mock_pyds8k = MagicMock()
            mock_rank = MagicMock()
            mock_rank.id = "rank1"
            mock_rank.state = "online"
            mock_rank.capacity = "50TB"
            mock_rank.raid_type = "RAID5"
            mock_system = MagicMock()
            mock_system.id = "sys1"
            mock_conn = MagicMock()
            mock_conn.get_systems.return_value = [mock_system]
            mock_conn.get_ranks.return_value = [mock_rank]
            mock_pyds8k.client.DS8KClient.return_value = mock_conn
            with patch.dict(
                sys.modules, {"pyds8k": mock_pyds8k, "pyds8k.client": mock_pyds8k.client}
            ):
                from src.poller import DS8000Client

                client = DS8000Client(ds_endpoint)
                result = client.query(
                    "ranks",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["raid_type"] == "RAID5"

    def test_query_replication(self, ds_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"DS_MAIN_USERNAME": "admin", "DS_MAIN_PASSWORD": "pass"}):
            mock_pyds8k = MagicMock()
            mock_pair = MagicMock()
            mock_pair.id = "pair1"
            mock_pair.source_volume = "vol1"
            mock_pair.target_volume = "vol2"
            mock_pair.state = "full_duplex"
            mock_pair.type = "metro_mirror"
            mock_system = MagicMock()
            mock_system.id = "sys1"
            mock_conn = MagicMock()
            mock_conn.get_systems.return_value = [mock_system]
            mock_conn.get_copy_services.return_value = [mock_pair]
            mock_pyds8k.client.DS8KClient.return_value = mock_conn
            with patch.dict(
                sys.modules, {"pyds8k": mock_pyds8k, "pyds8k.client": mock_pyds8k.client}
            ):
                from src.poller import DS8000Client

                client = DS8000Client(ds_endpoint)
                result = client.query(
                    "replication",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["state"] == "full_duplex"

    def test_query_unknown_type(self, ds_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"DS_MAIN_USERNAME": "admin", "DS_MAIN_PASSWORD": "pass"}):
            mock_pyds8k = MagicMock()
            mock_pyds8k.client.DS8KClient.return_value = MagicMock()
            with patch.dict(
                sys.modules, {"pyds8k": mock_pyds8k, "pyds8k.client": mock_pyds8k.client}
            ):
                from src.poller import DS8000Client

                client = DS8000Client(ds_endpoint)
                result = client.query(
                    "unknown",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result == []


class TestCSMClient:
    @pytest.fixture
    def csm_endpoint(self) -> SourceEndpoint:
        return SourceEndpoint(
            name="csm-test",
            url="https://csm.example.com",
            platform="csm",
            site="main",
            auth_type="basic",
            creds_vault_path="secret/test",
        )

    def test_connect(self, csm_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"CSM_MAIN_USERNAME": "admin", "CSM_MAIN_PASSWORD": "pass"}):
            mock_pycsm = MagicMock()
            mock_conn = MagicMock()
            mock_pycsm.CSMClient.return_value = mock_conn
            with patch.dict(sys.modules, {"pycsm": mock_pycsm}):
                from src.poller import CSMClient

                client = CSMClient(csm_endpoint)
                result = client._connect()
                assert result is mock_conn

    def test_query_sessions(self, csm_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"CSM_MAIN_USERNAME": "admin", "CSM_MAIN_PASSWORD": "pass"}):
            mock_pycsm = MagicMock()
            mock_session = MagicMock()
            mock_session.id = "sess1"
            mock_session.name = "test-session"
            mock_session.state = "active"
            mock_session.role = "source"
            mock_session.type = "metro_mirror"
            mock_conn = MagicMock()
            mock_conn.get_sessions.return_value = [mock_session]
            mock_pycsm.CSMClient.return_value = mock_conn
            with patch.dict(sys.modules, {"pycsm": mock_pycsm}):
                from src.poller import CSMClient

                client = CSMClient(csm_endpoint)
                result = client.query(
                    "sessions",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["name"] == "test-session"

    def test_query_policies(self, csm_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"CSM_MAIN_USERNAME": "admin", "CSM_MAIN_PASSWORD": "pass"}):
            mock_pycsm = MagicMock()
            mock_policy = MagicMock()
            mock_policy.id = "pol1"
            mock_policy.name = "test-policy"
            mock_policy.type = "metro_mirror"
            mock_policy.is_active = True
            mock_conn = MagicMock()
            mock_conn.get_policies.return_value = [mock_policy]
            mock_pycsm.CSMClient.return_value = mock_conn
            with patch.dict(sys.modules, {"pycsm": mock_pycsm}):
                from src.poller import CSMClient

                client = CSMClient(csm_endpoint)
                result = client.query(
                    "policies",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["name"] == "test-policy"

    def test_query_replication(self, csm_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"CSM_MAIN_USERNAME": "admin", "CSM_MAIN_PASSWORD": "pass"}):
            mock_pycsm = MagicMock()
            mock_conn = MagicMock()
            mock_conn.get_replication_status.return_value = [{"status": "synced"}]
            mock_pycsm.CSMClient.return_value = mock_conn
            with patch.dict(sys.modules, {"pycsm": mock_pycsm}):
                from src.poller import CSMClient

                client = CSMClient(csm_endpoint)
                result = client.query(
                    "replication",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert len(result) == 1
                assert result[0]["status"] == "synced"

    def test_query_unknown_type(self, csm_endpoint: SourceEndpoint) -> None:
        with patch.dict(os.environ, {"CSM_MAIN_USERNAME": "admin", "CSM_MAIN_PASSWORD": "pass"}):
            mock_pycsm = MagicMock()
            mock_pycsm.CSMClient.return_value = MagicMock()
            with patch.dict(sys.modules, {"pycsm": mock_pycsm}):
                from src.poller import CSMClient

                client = CSMClient(csm_endpoint)
                result = client.query(
                    "unknown",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result == []


class TestTS7700Client:
    @pytest.fixture
    def ts_endpoint(self) -> SourceEndpoint:
        return SourceEndpoint(
            name="ts-test",
            url="https://ts.example.com",
            platform="ts7700",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )

    def test_get_session(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_session = MagicMock()
            mock_requests.Session.return_value = mock_session
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client._get_session()
                assert result is mock_session

    def test_query_cluster(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"cluster_name": "test-cluster"}
            mock_resp.raise_for_status = MagicMock()
            mock_session = MagicMock()
            mock_session.get.return_value = mock_resp
            mock_requests.Session.return_value = mock_session
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client.query(
                    "cluster",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result["cluster_name"] == "test-cluster"

    def test_query_cache(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"cache_used_pct": 75}
            mock_resp.raise_for_status = MagicMock()
            mock_session = MagicMock()
            mock_session.get.return_value = mock_resp
            mock_requests.Session.return_value = mock_session
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client.query(
                    "cache",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result["cache_used_pct"] == 75

    def test_query_drives(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_resp = MagicMock()
            mock_resp.json.return_value = [{"drive_id": "D1"}]
            mock_resp.raise_for_status = MagicMock()
            mock_session = MagicMock()
            mock_session.get.return_value = mock_resp
            mock_requests.Session.return_value = mock_session
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client.query(
                    "drives",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result[0]["drive_id"] == "D1"

    def test_query_replication(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"replication_state": "active"}
            mock_resp.raise_for_status = MagicMock()
            mock_session = MagicMock()
            mock_session.get.return_value = mock_resp
            mock_requests.Session.return_value = mock_session
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client.query(
                    "replication",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result["replication_state"] == "active"

    def test_query_request_exception(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_resp = MagicMock()
            mock_resp.raise_for_status.side_effect = Exception("fail")
            mock_session = MagicMock()
            mock_session.get.return_value = mock_resp
            mock_requests.Session.return_value = mock_session
            mock_requests.exceptions.RequestException = Exception
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client.query(
                    "cluster",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result == []

    def test_query_unknown_type(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "admin", "TS7700_PRIMARY_PASSWORD": "pass"}
        ):
            mock_requests = MagicMock()
            mock_requests.Session.return_value = MagicMock()
            with patch.dict(sys.modules, {"requests": mock_requests}):
                from src.poller import TS7700Client

                client = TS7700Client(ts_endpoint)
                result = client.query(
                    "unknown",
                    PlatformRule(name="test", data_type="test", sourcetype="test", index="test"),
                )
                assert result == []

    def test_load_creds(self, ts_endpoint: SourceEndpoint) -> None:
        with patch.dict(
            os.environ, {"TS7700_PRIMARY_USERNAME": "u", "TS7700_PRIMARY_PASSWORD": "p"}
        ):
            from src.poller import TS7700Client

            client = TS7700Client(ts_endpoint)
            creds = client._load_creds()
            assert creds["username"] == "u"


class TestPollerRunOnce:
    def test_run_once_writes_spool(self, tmp_spool_dir: str) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_cpc = MagicMock()
            mock_cpc.properties = {"cpc_name": "CPCA", "status": "operating"}
            mock_cpc.pull_full_properties = MagicMock()
            mock_client = MagicMock()
            mock_client.cpcs.list.return_value = [mock_cpc]
            mock_zhmc.Client.return_value = mock_client
            mock_zhmc.Session.return_value = MagicMock()

            with patch.dict(sys.modules, {"zhmcclient": mock_zhmc}):
                from src.models import PlatformRule, SiteConfig, SourceEndpoint
                from src.poller import Poller

                p = Poller(
                    platform="hmc", rules_dir="tests/fixtures/rules", spool_dir=tmp_spool_dir
                )

                p.health_checker = MagicMock()
                p.health_checker.is_healthy.return_value = True
                p.health_checker.get_status.return_value = MagicMock(consecutive_failures=0)

                p.source_selector.get_active_endpoints = MagicMock(
                    return_value=[
                        SourceEndpoint(
                            name="hmc-primary",
                            url="https://test.example.com",
                            platform="hmc",
                            site="primary",
                            auth_type="basic",
                            creds_vault_path="secret/test",
                        )
                    ]
                )
                p.site_configs = {
                    "primary": SiteConfig(
                        site_name="primary",
                        platform="hmc",
                        endpoints=[
                            SourceEndpoint(
                                name="hmc-primary",
                                url="https://test.example.com",
                                platform="hmc",
                                site="primary",
                                auth_type="basic",
                                creds_vault_path="secret/test",
                            )
                        ],
                        data_types=["cpc-stats"],
                    )
                }
                p.platform_rules = {
                    "cpc-stats": PlatformRule(
                        name="cpc-stats",
                        data_type="cpc-stats",
                        sourcetype="hmc:cpc",
                        index="mainframe_metrics",
                    )
                }

                events_written = p.run_once()
                assert events_written > 0
                spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
                assert len(spool_mgr.list_pending()) == 1

    def test_run_once_no_active_endpoints(self, tmp_spool_dir: str) -> None:
        from src.poller import Poller

        p = Poller(platform="hmc", rules_dir="tests/fixtures/rules", spool_dir=tmp_spool_dir)
        p.source_selector.get_active_endpoints = MagicMock(return_value=[])
        assert p.run_once() == 0

    def test_run_forever_keyboard_interrupt(self, tmp_spool_dir: str) -> None:
        from src.poller import Poller

        p = Poller(platform="hmc", rules_dir="tests/fixtures/rules", spool_dir=tmp_spool_dir)
        p.health_checker = MagicMock()
        p.source_selector.get_active_endpoints = MagicMock(return_value=[])
        p.health_checker.start = MagicMock()
        p.health_checker.stop = MagicMock()

        p.run_once = MagicMock(side_effect=KeyboardInterrupt)
        with contextlib.suppress(KeyboardInterrupt):
            p.run_forever(interval_seconds=300)
        p.health_checker.start.assert_called_once()
        p.health_checker.stop.assert_called_once()


class TestPollerInit:
    def test_basic_initialization(self, tmp_spool_dir: str) -> None:
        from src.poller import Poller

        p = Poller(platform="hmc", rules_dir="tests/fixtures/rules", spool_dir=tmp_spool_dir)
        assert p.platform == "hmc"
        assert p.health_checker is not None
        assert p.source_selector is not None
        assert p.spool_manager is not None


class TestBaseAPIClient:
    def test_query_not_implemented(self) -> None:
        from src.models import SourceEndpoint
        from src.poller import BaseAPIClient

        ep = SourceEndpoint(
            name="test",
            url="https://example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        client = BaseAPIClient(ep)
        with pytest.raises(NotImplementedError):
            client.query(
                "any", PlatformRule(name="test", data_type="test", sourcetype="test", index="test")
            )


class TestPollerMain:
    def test_main_sets_up_and_starts(self, tmp_spool_dir: str) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    "PLATFORM": "hmc",
                    "RULES_DIR": "tests/fixtures/rules",
                    "SPOOL_DIR": tmp_spool_dir,
                    "HEALTH_PORT": "9191",
                    "POLL_INTERVAL_SECONDS": "1",
                },
            ),
            patch("src.poller.uvicorn"),
            patch("src.poller.Poller.run_forever") as mock_run,
        ):
            mock_run.side_effect = SystemExit
            from src.poller import main as poller_main

            with contextlib.suppress(SystemExit, KeyboardInterrupt):
                poller_main()
