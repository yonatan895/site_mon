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
