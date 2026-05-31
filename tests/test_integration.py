import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from src.models import PlatformRule, SiteConfig, SourceEndpoint
from src.spool import SpoolManager


class TestEndToEndPipeline:
    @pytest.mark.integration
    def test_poller_to_sender_flow(self, tmp_spool_dir: str) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
        ndjson_content = (
            '{"event": {"cpc_name": "CPA", "cpu_usage_pct": 45.2}}\n'
            '{"event": {"cpc_name": "CPB", "cpu_usage_pct": 72.8}}\n'
        )
        filename = spool_mgr.write_ndjson(ndjson_content, batch_id="test-batch")
        pending = spool_mgr.list_pending()
        assert filename in pending
        entries = spool_mgr.read_ndjson_batch(max_files=10)
        assert len(entries) == 1
        assert entries[0].content == ndjson_content
        spool_mgr.ack_file(entries[0].filename)
        assert len(spool_mgr.list_pending()) == 0

    @pytest.mark.integration
    def test_dead_letter_after_max_retries(self, tmp_spool_dir: str) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
        spool_mgr.write_ndjson('{"event": "test"}\n', batch_id="retry-batch")
        for attempt in range(6):
            entries = spool_mgr.read_ndjson_batch(max_files=10)
            if not entries:
                break
            spool_mgr.nack_file(entries[0].filename, f"attempt {attempt + 1}")
        pending = spool_mgr.list_pending()
        stats = spool_mgr.get_spool_stats()
        assert len(pending) == 0
        assert stats["dead_letter_count"] >= 1

    @pytest.mark.integration
    def test_poller_gathers_and_writes(self, tmp_spool_dir: str) -> None:
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

                events = p.run_once()
                assert events > 0

                spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
                entries = spool_mgr.read_ndjson_batch(max_files=10)
                assert len(entries) == 1
                content = entries[0].content
                assert "cpc_name" in content
                assert "operating" in content
                spool_mgr.ack_file(entries[0].filename)

    @pytest.mark.integration
    def test_sender_delivers_to_hec(self, tmp_spool_dir: str) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
        events = [
            {"time": "1717171200.000000", "event": {"cpu_pct": 50}},
            {"time": "1717171200.000001", "event": {"cpu_pct": 75}},
        ]
        ndjson = "\n".join(json.dumps(e) for e in events) + "\n"
        spool_mgr.write_ndjson(ndjson, batch_id="hec-test")

        with patch("src.sender.SplunkHECClient") as mock_hec_cls:
            mock_hec = MagicMock()
            mock_hec.send_ndjson.return_value = True
            mock_hec_cls.return_value = mock_hec
            from src.sender import Sender

            sender = Sender(
                spool_dir=tmp_spool_dir, hec_url="https://splunk.test:8088", hec_token="token"
            )
            sender.spool_manager = spool_mgr
            sender.hec_client = mock_hec

            result = sender.run_once()
            assert result == 1
            mock_hec.send_ndjson.assert_called_once_with(ndjson)
            sender.hec_client.close()

    @pytest.mark.integration
    def test_end_to_end_poller_to_sender(self, tmp_spool_dir: str) -> None:
        with patch.dict(
            os.environ, {"HMC_PRIMARY_USERNAME": "admin", "HMC_PRIMARY_PASSWORD": "pass"}
        ):
            mock_zhmc = MagicMock()
            mock_cpc = MagicMock()
            mock_cpc.properties = {"cpc_name": "E2E-CPC", "status": "operating"}
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

                events = p.run_once()
                assert events > 0

            spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
            pending = spool_mgr.list_pending()
            assert len(pending) == 1

            with patch("src.sender.SplunkHECClient") as mock_hec_cls:
                mock_hec = MagicMock()
                mock_hec.send_ndjson.return_value = True
                mock_hec_cls.return_value = mock_hec
                from src.sender import Sender

                sender = Sender(
                    spool_dir=tmp_spool_dir, hec_url="https://splunk.test:8088", hec_token="token"
                )
                sender.spool_manager = spool_mgr
                sender.hec_client = mock_hec

                delivered = sender.run_once()
                assert delivered == 1
                call_arg = mock_hec.send_ndjson.call_args[0][0]
                assert "E2E-CPC" in call_arg
                sender.hec_client.close()
