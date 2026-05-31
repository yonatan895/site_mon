"""Spool watcher: reads NDJSON spool files and sends them to Splunk HEC."""

import os
import threading
import time

import uvicorn

from .splunk_hec import SplunkHECClient
from .spool import SpoolManager
from .utils import ensure_dir, setup_logging

logger = setup_logging(__name__)

DEFAULT_WATCH_INTERVAL = 1.0
DEFAULT_BATCH_FILES = 50


class Sender:
    """Watches the shared spool directory and sends NDJSON payloads to Splunk HEC.

    Continuously reads pending spool files, sends their raw NDJSON content
    directly to HEC, and acknowledges or retries based on delivery status.
    """

    def __init__(
        self,
        spool_dir: str = "/spool",
        hec_url: str | None = None,
        hec_token: str | None = None,
    ) -> None:
        ensure_dir(spool_dir)

        self.spool_manager = SpoolManager(spool_dir)

        self.hec_url = hec_url or os.environ.get(
            "SPLUNK_HEC_URL", "https://localhost:8088"
        )
        self.hec_token = hec_token or os.environ.get("SPLUNK_HEC_TOKEN", "")

        self.hec_client = SplunkHECClient(
            hec_url=self.hec_url,
            hec_token=self.hec_token,
            batch_size=int(os.environ.get("SPLUNK_HEC_BATCH_SIZE", "500")),
            ack_enabled=os.environ.get(
                "SPLUNK_HEC_ACK_ENABLED", "true"
            ).lower()
            == "true",
            max_connections=int(os.environ.get("SPLUNK_HEC_MAX_CONNECTIONS", "10")),
            timeout=int(os.environ.get("SPLUNK_HEC_TIMEOUT", "30")),
        )

        logger.info(
            "sender_initialized",
            spool_dir=spool_dir,
            hec_url=self.hec_url,
        )

    def run_once(self) -> int:
        """Execute a single send cycle.

        Reads pending NDJSON files, sends content directly to HEC,
        and acknowledges or retries each file.

        Returns:
            Number of files successfully sent.
        """
        entries = self.spool_manager.read_ndjson_batch(
            max_files=DEFAULT_BATCH_FILES
        )
        if not entries:
            return 0

        logger.info("sender_cycle_start", files=len(entries))

        success_count = 0
        for entry in entries:
            try:
                ok = self.hec_client.send_ndjson(entry.content)
                if ok:
                    self.spool_manager.ack_file(entry.filename)
                    success_count += 1
                else:
                    self.spool_manager.nack_file(
                        entry.filename, error="hec_send_returned_false"
                    )
            except Exception:
                logger.exception(
                    "send_failed", filename=entry.filename
                )
                self.spool_manager.nack_file(
                    entry.filename, error="hec_send_exception"
                )

        failure_count = len(entries) - success_count
        logger.info(
            "sender_cycle_complete",
            success=success_count,
            failure=failure_count,
        )
        return success_count

    def cleanup(self) -> int:
        return self.spool_manager.cleanup_old_files(max_age_hours=24)

    def run_forever(self) -> None:
        """Run the sender continuously, watching for new spool files."""
        logger.info(
            "sender_loop_started", watch_interval=DEFAULT_WATCH_INTERVAL
        )
        last_cleanup = time.monotonic()

        try:
            while True:
                try:
                    pending = self.spool_manager.list_pending()
                    if pending:
                        self.run_once()

                    if time.monotonic() - last_cleanup > 3600:
                        self.cleanup()
                        last_cleanup = time.monotonic()

                except Exception:
                    logger.exception("sender_cycle_error")

                time.sleep(DEFAULT_WATCH_INTERVAL)

        except KeyboardInterrupt:
            logger.info("sender_interrupted")
        finally:
            self.hec_client.close()
            logger.info("sender_stopped")


def main() -> None:
    spool_dir = os.environ.get("SPOOL_DIR", "/spool")
    hec_url = os.environ.get("SPLUNK_HEC_URL", "")
    hec_token = os.environ.get("SPLUNK_HEC_TOKEN", "")

    sender = Sender(
        spool_dir=spool_dir, hec_url=hec_url, hec_token=hec_token
    )

    from .health import app as health_app
    from .health import init_health

    init_health(spool_manager_instance=sender.spool_manager)

    port = int(os.environ.get("HEALTH_PORT", "8081"))
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
