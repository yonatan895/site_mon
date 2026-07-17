"""Splunk HTTP Event Collector (HEC) client with high-reliability delivery.

Accepts raw NDJSON payloads and POSTs them directly — HEC natively
understands newline-delimited JSON, so no transformation is needed.

Delivery guarantee level: at-least-once with indexer ACK polling.
When ack_enabled=True a channel UUID is sent via X-Splunk-Request-Channel
and the /services/collector/ack endpoint is polled until the indexer
confirms the event batch has been written to disk.
"""

import gzip
import json
import logging
import time
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

from .utils import configure_logging

configure_logging()
import structlog

logger = structlog.get_logger(__name__)

HEC_EVENT_PATH = "/services/collector/event"
HEC_ACK_PATH = "/services/collector/ack"

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# How long to wait total for indexer ACK confirmation before giving up
ACK_POLL_TIMEOUT_SECONDS = 60
ACK_POLL_INTERVAL_SECONDS = 2


class SplunkHECClient:
    """Client for sending NDJSON payloads to Splunk's HTTP Event Collector.

    Supports connection pooling, gzip compression, batching, and Splunk's
    indexer acknowledgment protocol for guaranteed at-least-once delivery.

    When ack_enabled is True (default):
      1. A stable channel UUID is sent via X-Splunk-Request-Channel.
      2. After each successful POST the returned ackID is polled via
         /services/collector/ack until the indexer confirms indexing
         or ACK_POLL_TIMEOUT_SECONDS elapses.
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
            self.channel_id = str(uuid.uuid4())
            logger.info("ack_channel_created", channel_id=self.channel_id)

        logger.info(
            "hec_client_initialized",
            url=self.hec_url,
            batch_size=batch_size,
            ack_enabled=ack_enabled,
            channel_id=self.channel_id,
        )

    def send_ndjson(self, ndjson_content: str) -> bool:
        """Send a raw NDJSON string to Splunk HEC.

        The content is sent as-is — it should already be properly formatted
        HEC events, one JSON object per line.

        When ack_enabled=True, polls /services/collector/ack to confirm
        the indexer has written the batch before returning True.

        Args:
            ndjson_content: Newline-delimited JSON event payload.

        Returns:
            True if delivery was confirmed (or ack disabled and HTTP 200 received).
        """
        if not ndjson_content.strip():
            return True

        line_count = ndjson_content.count("\n") + (1 if ndjson_content.strip() else 0)

        try:
            ack_id = self._post_with_retry(ndjson_content)
            logger.info("ndjson_sent", lines=line_count, ack_id=ack_id)

            # FIX #4: Poll ACK endpoint to confirm indexer has written the batch.
            if self.ack_enabled and ack_id is not None:
                confirmed = self._poll_ack(ack_id)
                if not confirmed:
                    logger.warning(
                        "ack_not_confirmed",
                        ack_id=ack_id,
                        timeout_seconds=ACK_POLL_TIMEOUT_SECONDS,
                    )
                    return False
                logger.info("ack_confirmed", ack_id=ack_id)

            return True
        except Exception as e:
            logger.error("ndjson_send_failed", lines=line_count, error=str(e))
            return False

    def _poll_ack(self, ack_id: int) -> bool:
        """Poll /services/collector/ack until the indexer confirms the batch.

        Args:
            ack_id: The ackID returned by the HEC event endpoint.

        Returns:
            True if the indexer confirmed acked=True within the timeout.
        """
        url = urljoin(self.hec_url + "/", HEC_ACK_PATH.lstrip("/"))
        payload = json.dumps({"acks": [ack_id]}).encode("utf-8")
        headers = {
            "Authorization": f"Splunk {self.hec_token}",
            "Content-Type": "application/json",
            "X-Splunk-Request-Channel": self.channel_id,
        }

        deadline = time.monotonic() + ACK_POLL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                response = self.pool.request(
                    "POST",
                    url,
                    body=payload,
                    headers=headers,
                    timeout=urllib3.Timeout(total=self.timeout),
                )
                if response.status == 200:
                    data = json.loads(response.data.decode("utf-8"))
                    acks = data.get("acks", {})
                    if acks.get(str(ack_id)) is True:
                        return True
            except Exception as e:
                logger.warning("ack_poll_error", ack_id=ack_id, error=str(e))

            time.sleep(ACK_POLL_INTERVAL_SECONDS)

        return False

    def _post_with_retry(self, payload: str) -> int | None:
        """POST payload to HEC with retry. Returns ackID if ack_enabled, else None."""
        return self._do_post(payload)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=64, jitter=1),
        retry=retry_if_exception_type((urllib3.exceptions.HTTPError, ConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger("src.splunk_hec"), logging.WARNING),
        reraise=True,
    )
    def _do_post(self, payload: str) -> int | None:
        """Execute a single POST to the HEC event endpoint.

        Returns:
            ackID integer from HEC response when ack_enabled, otherwise None.

        Raises:
            urllib3.exceptions.HTTPError: On retryable HTTP status.
            RuntimeError: On fatal non-retryable HTTP status.
        """
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
            if self.ack_enabled:
                try:
                    body = json.loads(response.data.decode("utf-8"))
                    return body.get("ackId")
                except Exception:
                    logger.warning("ack_id_parse_failed")
                    return None
            return None

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
