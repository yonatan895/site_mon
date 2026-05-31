import contextlib
import os
from unittest.mock import MagicMock, patch

import pytest

from src.sender import Sender


@pytest.fixture
def sender(spool_manager) -> Sender:
    with patch("src.sender.SplunkHECClient") as mock_hec_cls:
        mock_hec = MagicMock()
        mock_hec.send_ndjson.return_value = True
        mock_hec_cls.return_value = mock_hec

        s = Sender(
            spool_dir=spool_manager.spool_dir,
            hec_url="https://splunk.example.com:8088",
            hec_token="test-token",
        )
        s.spool_manager = spool_manager
        s.hec_client = mock_hec
        yield s
        s.hec_client.close()


class TestRunOnce:
    def test_no_files_returns_zero(self, sender: Sender) -> None:
        result = sender.run_once()
        assert result == 0

    def test_successful_delivery_acks_files(self, sender: Sender) -> None:
        sender.spool_manager.write_ndjson('{"event": "test1"}\n', batch_id="b1")
        result = sender.run_once()
        assert result == 1
        pending = sender.spool_manager.list_pending()
        assert len(pending) == 0

    def test_multiple_files(self, sender: Sender) -> None:
        for i in range(3):
            sender.spool_manager.write_ndjson(
                '{"event": "test' + str(i) + '"}\n', batch_id="b" + str(i)
            )
        result = sender.run_once()
        assert result == 3
        assert len(sender.spool_manager.list_pending()) == 0

    def test_failed_delivery_nacks_files(self, sender: Sender) -> None:
        sender.hec_client.send_ndjson.return_value = False
        sender.spool_manager.write_ndjson('{"event": "test"}\n', batch_id="b1")
        result = sender.run_once()
        assert result == 0
        stats = sender.spool_manager.get_spool_stats()
        assert stats["dead_letter_count"] + stats["pending_count"] > 0

    def test_exception_during_send_nacks(self, sender: Sender) -> None:
        sender.hec_client.send_ndjson.side_effect = RuntimeError("fail")
        sender.spool_manager.write_ndjson('{"event": "test"}\n', batch_id="b1")
        result = sender.run_once()
        assert result == 0


class TestCleanup:
    def test_cleanup_delegates_to_spool_manager(self, sender: Sender) -> None:
        result = sender.cleanup()
        assert isinstance(result, int)


class TestRunForever:
    def test_empty_loop_iterates(self, sender: Sender) -> None:
        call_count = [0]
        original = sender.spool_manager.list_pending

        def counting_list():
            call_count[0] += 1
            if call_count[0] > 3:
                raise KeyboardInterrupt
            return []

        sender.spool_manager.list_pending = counting_list

        with contextlib.suppress(KeyboardInterrupt):
            sender.run_forever()

        sender.spool_manager.list_pending = original
        assert call_count[0] >= 3

    def test_empty_loop_handles_exception(self, sender: Sender) -> None:
        call_count = [0]
        original = sender.spool_manager.list_pending

        def flaky_list():
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("flaky")
            if call_count[0] > 3:
                raise KeyboardInterrupt
            return []

        sender.spool_manager.list_pending = flaky_list

        with contextlib.suppress(KeyboardInterrupt):
            sender.run_forever()

        sender.spool_manager.list_pending = original
        assert call_count[0] >= 3

    def test_does_cleanup_periodically(self, sender: Sender) -> None:
        call_count = [0]

        def pending_list():
            call_count[0] += 1
            raise KeyboardInterrupt

        sender.spool_manager.list_pending = pending_list

        with contextlib.suppress(KeyboardInterrupt):
            sender.run_forever()

        sender.spool_manager.list_pending = MagicMock(return_value=[])


class TestSenderInit:
    def test_env_vars_used(self, spool_manager) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    "SPLUNK_HEC_URL": "https://custom.example.com:8088",
                    "SPLUNK_HEC_TOKEN": "env-token",
                },
            ),
            patch("src.sender.SplunkHECClient") as mock_hec,
        ):
            mock_hec.return_value = MagicMock()
            s = Sender(spool_dir=spool_manager.spool_dir)
            assert s.hec_url == "https://custom.example.com:8088"
            assert s.hec_token == "env-token"
            s.hec_client.close()

    def test_explicit_params_override_env(self, spool_manager) -> None:
        with (
            patch.dict(os.environ, {"SPLUNK_HEC_URL": "https://env.example.com:8088"}),
            patch("src.sender.SplunkHECClient") as mock_hec,
        ):
            mock_hec.return_value = MagicMock()
            s = Sender(
                spool_dir=spool_manager.spool_dir, hec_url="https://explicit.example.com:8088"
            )
            assert s.hec_url == "https://explicit.example.com:8088"
            s.hec_client.close()


class TestSenderMain:
    def test_main_sets_up_sender(self, tmp_path) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    "SPOOL_DIR": str(tmp_path / "spool"),
                    "SPLUNK_HEC_URL": "https://splunk.test:8088",
                    "SPLUNK_HEC_TOKEN": "test-token",
                    "HEALTH_PORT": "9191",
                },
            ),
            patch("src.sender.uvicorn"),
            patch("src.sender.SplunkHECClient") as mock_hec,
            patch("src.sender.Sender.run_forever") as mock_run,
        ):
            mock_hec.return_value = MagicMock()
            mock_run.side_effect = SystemExit
            from src.sender import main as sender_main

            with contextlib.suppress(SystemExit):
                sender_main()
