import gzip
import json
from unittest.mock import MagicMock, patch

import pytest

from src.splunk_hec import SplunkHECClient


def _make_response(status: int = 200, data: object = b"", headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status = status
    if isinstance(data, dict):
        resp.data = json.dumps(data).encode("utf-8")
    else:
        resp.data = data if isinstance(data, bytes) else b""
    resp.headers = headers or {}
    return resp


class TestSplunkHECInit:
    def test_initialization(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.return_value = _make_response(201, {"ackId": "12345"})
            client = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
            )
            assert client.channel_id == "12345"
            client.close()

    def test_ack_disabled(self) -> None:
        client = SplunkHECClient(
            hec_url="https://splunk.example.com:8088",
            hec_token="test-token",
            ack_enabled=False,
        )
        assert client.channel_id is None
        client.close()

    def test_ack_creation_failure_falls_back(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.return_value = _make_response(500, b"error")
            client = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
            )
            assert client.ack_enabled is False
            client.close()


class TestSendNDJSON:
    @pytest.fixture
    def client(self) -> SplunkHECClient:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.return_value = _make_response(200)
            c = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
                ack_enabled=False,
            )
            c._mock = mock_req
            yield c
            c.close()

    def test_successful_send(self, client: SplunkHECClient) -> None:
        client._mock.return_value = _make_response(200)
        result = client.send_ndjson('{"event": "test"}\n')
        assert result is True

    def test_empty_content_returns_true(self, client: SplunkHECClient) -> None:
        result = client.send_ndjson("   \n  ")
        assert result is True

    def test_failure_returns_false(self, client: SplunkHECClient) -> None:
        client._mock.return_value = _make_response(400)
        result = client.send_ndjson('{"event": "test"}\n')
        assert result is False

    def test_gzip_content_encoding(self, client: SplunkHECClient) -> None:
        client._mock.return_value = _make_response(200)
        client.send_ndjson('{"event": "test"}\n')
        call_kwargs = client._mock.call_args
        assert call_kwargs is not None
        headers = call_kwargs[1].get("headers", {})
        assert headers.get("Content-Encoding", "").lower() == "gzip"

    def test_payload_is_gzipped(self, client: SplunkHECClient) -> None:
        payload = '{"event": "test"}\n'
        client._mock.return_value = _make_response(200)
        client.send_ndjson(payload)
        call_kwargs = client._mock.call_args
        assert call_kwargs is not None
        compressed = call_kwargs[1].get("body", b"")
        decompressed = gzip.decompress(compressed).decode("utf-8")
        assert decompressed == payload

    def test_channel_header_when_ack_enabled(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.side_effect = [
                _make_response(201, {"ackId": "ch-99"}),
                _make_response(200),
            ]
            c = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
                ack_enabled=True,
            )
            c.send_ndjson('{"event": "test"}\n')
            assert mock_req.call_count >= 2
            headers = mock_req.call_args[1].get("headers", {})
            assert headers.get("X-Splunk-Request-Channel") == "ch-99"
            c.close()


class TestRetryBehavior:
    def test_retry_on_429(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.side_effect = [
                _make_response(429, headers={"Retry-After": "1"}),
                _make_response(200),
            ]
            c = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
                ack_enabled=False,
            )
            result = c.send_ndjson('{"event": "test"}\n')
            assert result is True
            assert mock_req.call_count >= 2
            c.close()

    def test_retry_on_503(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.side_effect = [
                _make_response(503, headers={"Retry-After": "1"}),
                _make_response(200),
            ]
            c = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
                ack_enabled=False,
            )
            result = c.send_ndjson('{"event": "test"}\n')
            assert result is True
            assert mock_req.call_count >= 2
            c.close()

    def test_no_retry_on_400(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.return_value = _make_response(400)
            c = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
                ack_enabled=False,
            )
            result = c.send_ndjson('{"event": "test"}\n')
            assert result is False
            assert mock_req.call_count == 1
            c.close()


class TestClose:
    def test_close_clears_pool(self) -> None:
        with patch("urllib3.PoolManager.request") as mock_req:
            mock_req.return_value = _make_response(201, {"ackId": "12345"})
            client = SplunkHECClient(
                hec_url="https://splunk.example.com:8088",
                hec_token="test-token",
                ack_enabled=False,
            )
            client.close()
            assert client.pool is not None
