"""Sender: drains the spool and delivers NDJSON batches to Splunk HEC.

Runs as a long-lived process (or one-shot via run_once) inside its own
container.  The spool directory is a shared volume between the poller
and sender containers.

Delivery guarantee: at-least-once.  Failed batches are nack'd and retried
up to SpoolManager.MAX_RETRIES times before being moved to dead-letter.
"""

import os
import signal
import threading
import time

import structlog
import uvicorn

from .splunk_hec import SplunkHECClient
from .spool import SpoolManager
from .utils import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


class Sender:
    """Drains the spool and delivers NDJSON to Splunk HEC.

    Lifecycle::

        sender = Sender()
        sender.run_once()      # process all pending files once
        sender.run_forever()   # loop until SIGTERM / KeyboardInterrupt
    """

    def __init__(
        self,
        spool_dir: str = "/spool",
        hec_url: str = "",
        hec_token: str = "",
        batch_size: int = 500,
        drain_interval_seconds: int = 5,
    ) -> None:
        hec_url = hec_url or os.environ.get("HEC_URL", "")
        hec_token = hec_token or os.environ.get("HEC_TOKEN", "")

        if not hec_url or not hec_token:
            raise ValueError(
                "HEC_URL and HEC_TOKEN must be provided (env or constructor args)"
            )

        self.drain_interval = drain_interval_seconds
        self.spool = SpoolManager(spool_dir)
        self.hec = SplunkHECClient(
            hec_url=hec_url,
            hec_token=hec_token,
            batch_size=batch_size,
        )

        logger.info(
            "sender_initialized",
            spool_dir=spool_dir,
            hec_url=hec_url,
            batch_size=batch_size,
        )

    def run_once(self) -> int:
        """Drain all pending spool files and return the count delivered."""
        entries = self.spool.read_ndjson_batch()
        if not entries:
            return 0

        delivered = 0
        for entry in entries:
            ok = self.hec.send_ndjson(entry.content)
            if ok:
                self.spool.ack_file(entry.filename)
                delivered += 1
                logger.info("batch_delivered", filename=entry.filename)
            else:
                self.spool.nack_file(
                    entry.filename,
                    error="HEC delivery failed",
                )
                logger.warning(
                    "batch_nacked",
                    filename=entry.filename,
                    retry_count=entry.retry_count,
                )

        logger.info(
            "drain_cycle_complete",
            total=len(entries),
            delivered=delivered,
            failed=len(entries) - delivered,
        )
        return delivered

    def run_forever(self) -> None:
        """Drain loop: runs until SIGTERM or KeyboardInterrupt."""
        logger.info("sender_loop_started", drain_interval=self.drain_interval)
        stop_event = threading.Event()

        def _handle_shutdown(signum: int, _frame: object) -> None:
            logger.info("sender_shutdown_signal", signal=signum)
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_shutdown)
        try:
            while not stop_event.is_set():
                try:
                    self.run_once()
                except Exception:
                    logger.exception("drain_cycle_error")
                stop_event.wait(timeout=self.drain_interval)
        except KeyboardInterrupt:
            logger.info("sender_interrupted")
        finally:
            self.hec.close()


def main() -> None:
    """Entry point for the sender container."""
    spool_dir = os.environ.get("SPOOL_DIR", "/spool")
    hec_url = os.environ.get("HEC_URL", "")
    hec_token = os.environ.get("HEC_TOKEN", "")
    batch_size = int(os.environ.get("HEC_BATCH_SIZE", "500"))
    drain_interval = int(os.environ.get("DRAIN_INTERVAL_SECONDS", "5"))

    sender = Sender(
        spool_dir=spool_dir,
        hec_url=hec_url,
        hec_token=hec_token,
        batch_size=batch_size,
        drain_interval_seconds=drain_interval,
    )

    from .health import app as health_app
    from .health import init_health
    from .endpoint_health import EndpointHealthChecker

    init_health(
        health_checker_instance=EndpointHealthChecker([]),
        spool_manager_instance=sender.spool,
    )

    port = int(os.environ.get("HEALTH_PORT", "8080"))
    server_thread = threading.Thread(
        target=uvicorn.run,
        args=(health_app,),
        kwargs={"host": "0.0.0.0", "port": port, "log_level": "warning"},
        daemon=True,
    )
    server_thread.start()
    sender.run_forever()


if __name__ == "__main__":
    main()
