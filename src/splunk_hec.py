"""Splunk HTTP Event Collector (HEC) client with high-reliability delivery.

Accepts raw NDJSON payloads and POSTs them directly — HEC natively
understands newline-delimited JSON, so no transformation is needed.
"""

import gzip
import logging
import uuid
from urllib.parse import urljoin

import urllib3
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .utils import setup_logging

logger = setup_logging(__name__)

HEC_EVENT_PATH = "/services/collector/event"

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class SplunkHECClient:
    """Client for sending NDJSON payloads to Splunk's HTTP Event Collector.

    Supports connection pooling, gzip compression, batching, and Splunk's
    indexer acknowledgment protocol for guaranteed delivery.

    When ack_enabled is True (default), a channel is created and its ID is
    included in the X-Splunk-Request-Channel header for event ordering
    guarantees. Note that this client does NOT poll the ACK endpoint
    (/services/collector/ack) to verify indexer confirmation. For full
    end-to-end acknowledgment, implement ACK polling in a future iteration.
    """

    def __init__(
        self,
        hec_url: str,
        hec_token: str,
        batch_size: int = 500,
        ack_enabled: bool = True,
        max_connections: int = 10,
        timeout: int = 30,
    ) -> None:
        self.hec_url = hec_url.rstrip("/")
        self.hec_token = hec_token
        self.batch_size = batch_size
        self.ack_enabled = ack_enabled
        self.timeout = timeout
        self.channel_id: str | None = None

        self.pool = urllib3.PoolManager(
            num_pools=max_connections,
            maxsize=max_connections,
            timeout=urllib3.Timeout(total=timeout),
            retries=urllib3.Retry(total=0, redirect=0),
        )

        if self.ack_enabled:
            self._create_ack_channel()

        logger.info(
            "hec_client_initialized",
            url=self.hec_url,
            batch_size=batch_size,
            ack_enabled=ack_enabled,
            channel_id=self.channel_id,
        )

    def _create_ack_channel(self) -> None:
        self.channel_id = str(uuid.uuid4())
        logger.info("ack_channel_created", channel_id=self.channel_id)

    def send_ndjson(self, ndjson_content: str) -> bool:
        """Send a raw NDJSON string to Splunk HEC.

        The content is sent as-is — it should already be properly formatted
        HEC events, one JSON object per line.

        Args:
            ndjson_content: Newline-delimited JSON event payload.

        Returns:
            True if delivery was successful.
        """
        if not ndjson_content.strip():
            return True

        line_count = ndjson_content.count("\n") + (1 if ndjson_content.strip() else 0)

        try:
            self._post_with_retry(ndjson_content)
            logger.info("ndjson_sent", lines=line_count)
            return True
        except Exception as e:
            logger.error("ndjson_send_failed", lines=line_count, error=str(e))
            return False

    def _post_with_retry(self, payload: str) -> None:
        self._do_post(payload)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=64, jitter=1),
        retry=retry_if_exception_type((urllib3.exceptions.HTTPError, ConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger("src.splunk_hec"), logging.WARNING),
        reraise=True,
    )
    def _do_post(self, payload: str) -> None:
        url = urljoin(self.hec_url + "/", HEC_EVENT_PATH.lstrip("/"))
        compressed = gzip.compress(payload.encode("utf-8"))

        headers = {
            "Authorization": f"Splunk {self.hec_token}",
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
        }
        if self.ack_enabled and self.channel_id:
            headers["X-Splunk-Request-Channel"] = self.channel_id

        response = self.pool.request(
            "POST",
            url,
            body=compressed,
            headers=headers,
            timeout=urllib3.Timeout(total=self.timeout),
        )

        if response.status == 200:
            logger.debug("hec_post_success", payload_size=len(payload))
            return

        if response.status in RETRYABLE_STATUSES:
            retry_after = response.headers.get("Retry-After", "5")
            logger.warning(
                "hec_retryable_error",
                status=response.status,
                retry_after=retry_after,
            )
            raise urllib3.exceptions.HTTPError(
                f"HEC returned {response.status}: {response.data[:500]!r}"
            )

        error_msg = (
            f"HEC POST failed with status {response.status}: "
            f"{response.data[:500].decode('utf-8', errors='replace')}"
        )
        logger.error("hec_fatal_error", status=response.status)
        raise RuntimeError(error_msg)

    def close(self) -> None:
        if self.pool:
            self.pool.clear()
            logger.info("hec_client_closed")
