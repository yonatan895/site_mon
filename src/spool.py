"""Spool management for reliable NDJSON event delivery between containers."""

import contextlib
import os
import threading
import time
import uuid
from pathlib import Path
from typing import NamedTuple

from .utils import ensure_dir, setup_logging

logger = setup_logging(__name__)

NDJSON_EXTENSION = ".ndjson"
TEMP_EXTENSION = ".tmp"
PROCESSING_EXTENSION = ".processing"
DEAD_LETTER_DIRNAME = "dead_letter"
MAX_RETRIES = 5


class SpoolEntry(NamedTuple):
    filename: str
    content: str
    retry_count: int


class SpoolFullError(Exception):
    """Raised when the spool directory exceeds the configured maximum size."""


def _parse_filename(filename: str) -> tuple[str, int, str]:
    base = filename
    if base.endswith(PROCESSING_EXTENSION):
        base = base.rsplit(PROCESSING_EXTENSION, 1)[0]
    elif base.endswith(NDJSON_EXTENSION):
        base = base.rsplit(NDJSON_EXTENSION, 1)[0]
    parts = base.rsplit("_", 2)
    if len(parts) >= 3:
        batch_id = "_".join(parts[:-2])
        try:
            retry_count = int(parts[-2])
        except ValueError:
            retry_count = 0
        return batch_id, retry_count, parts[-1]
    return base, 0, ""


def _build_filename(batch_id: str, retry_count: int = 0) -> str:
    ts = int(time.time() * 1_000_000)
    return f"{batch_id}_{retry_count}_{ts}{NDJSON_EXTENSION}"


class SpoolManager:
    """Manages the shared spool directory for reliable NDJSON event delivery.

    Files are stored as raw NDJSON (one JSON HEC event per line).
    Retry count is embedded in the filename, avoiding any need to
    deserialize file content for tracking.
    """

    def __init__(
        self,
        spool_dir: str = "/spool",
        max_spool_size_mb: int = 1024,
        dead_letter_dir: str | None = None,
    ) -> None:
        self.spool_dir = Path(spool_dir)
        self.max_spool_size_bytes = max_spool_size_mb * 1024 * 1024
        self.dead_letter_dir = (
            Path(dead_letter_dir) if dead_letter_dir else self.spool_dir / DEAD_LETTER_DIRNAME
        )
        ensure_dir(str(self.spool_dir))
        ensure_dir(str(self.dead_letter_dir))
        self._lock = threading.Lock()
        self.reclaim_stale_processing()

        logger.info(
            "spool_manager_initialized",
            spool_dir=str(self.spool_dir),
            max_size_mb=max_spool_size_mb,
            dead_letter_dir=str(self.dead_letter_dir),
            current_size_bytes=self._scan_spool_size(),
        )

    def write_ndjson(self, content: str, batch_id: str = "") -> str:
        """Atomically write raw NDJSON content to a spool file.

        Args:
            content: NDJSON string (one JSON event per line).
            batch_id: Optional batch identifier prefix.

        Returns:
            The filename written.

        Raises:
            SpoolFullError: If the spool exceeds max size.
            ValueError: If content is empty.
        """
        if not content.strip():
            raise ValueError("Cannot write empty NDJSON content")

        if self._get_spool_size() >= self.max_spool_size_bytes:
            raise SpoolFullError(
                f"Spool directory exceeds maximum size of "
                f"{self.max_spool_size_bytes / 1024 / 1024:.0f} MB — "
                f"back-pressure: refusing write"
            )

        bid = batch_id or str(uuid.uuid4())
        filename = _build_filename(bid)
        filepath = self.spool_dir / filename
        tmp_path = filepath.with_suffix(filepath.suffix + TEMP_EXTENSION)

        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.rename(str(tmp_path), str(filepath))
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise

        line_count = content.count("\n")
        logger.debug(
            "ndjson_written",
            filename=filename,
            batch_id=bid,
            lines=line_count,
        )
        return filename

    def list_pending(self) -> list[str]:
        """List all pending .ndjson files."""
        if not self.spool_dir.exists():
            return []
        return [
            e.name for e in self.spool_dir.iterdir() if e.is_file() and e.suffix == NDJSON_EXTENSION
        ]

    def read_ndjson_batch(self, max_files: int = 50) -> list[SpoolEntry]:
        """Read up to max_files pending files, renaming to .processing.

        Files are renamed to .processing to prevent re-reading.
        Thread-safe: lock protects the list-pending + claim gap.

        Args:
            max_files: Maximum number of files to read in one batch.

        Returns:
            List of SpoolEntry (filename, raw NDJSON content, retry_count).
        """
        with self._lock:
            pending = self.list_pending()[:max_files]
            if not pending:
                return []

            entries: list[SpoolEntry] = []
            for filename in pending:
                filepath = self.spool_dir / filename
                processing_path = filepath.with_suffix(PROCESSING_EXTENSION)

                try:
                    os.rename(str(filepath), str(processing_path))
                except FileNotFoundError:
                    logger.debug("file_already_claimed", filename=filename)
                    continue

                try:
                    content = processing_path.read_text(encoding="utf-8")
                except Exception as e:
                    logger.error("failed_to_read_ndjson", filename=filename, error=str(e))
                    self.move_to_dead_letter(str(processing_path))
                    continue

                _, retry, _ = _parse_filename(filename)
                entries.append(SpoolEntry(filename=filename, content=content, retry_count=retry))

        if entries:
            total_lines = sum(e.content.count("\n") for e in entries)
            logger.info("ndjson_batch_read", files=len(entries), lines=total_lines)
        return entries

    def ack_file(self, filename: str) -> None:
        """Acknowledge successful delivery by deleting the file."""
        for ext in (NDJSON_EXTENSION, PROCESSING_EXTENSION):
            fp = self.spool_dir / filename
            if fp.suffix != ext:
                fp = fp.with_suffix(ext)
            if fp.exists():
                fp.unlink()
                logger.debug("file_acknowledged", filename=str(fp))
                return
        logger.warning("ack_file_not_found", filename=filename)

    def nack_file(self, filename: str, error: str = "") -> None:
        """Handle failed delivery — retry or dead-letter.

        Increments the retry count embedded in the filename.
        Moves to dead_letter if max retries exceeded.
        """
        proc_path = self.spool_dir / filename
        if proc_path.suffix != PROCESSING_EXTENSION:
            proc_path = proc_path.with_suffix(PROCESSING_EXTENSION)

        if not proc_path.exists():
            logger.warning("nack_file_not_found", filename=filename)
            return

        batch_id, retry_count, _ = _parse_filename(filename)
        retry_count += 1

        if retry_count > MAX_RETRIES:
            logger.error(
                "max_retries_exceeded",
                filename=filename,
                retry_count=retry_count,
                max_retries=MAX_RETRIES,
                error=error,
            )
            self.move_to_dead_letter(str(proc_path))
            return

        new_name = _build_filename(batch_id, retry_count)
        new_path = self.spool_dir / new_name
        os.rename(str(proc_path), str(new_path))

        logger.warning(
            "file_nacked_for_retry",
            old_filename=filename,
            new_filename=new_name,
            retry_count=retry_count,
            error=error,
        )

    def move_to_dead_letter(self, filepath: str) -> None:
        """Move a file to the dead letter directory."""
        src = Path(filepath)
        if not src.exists():
            return
        dst = self.dead_letter_dir / src.name
        os.rename(str(src), str(dst))
        logger.info("moved_to_dead_letter", filename=src.name)

    def reclaim_stale_processing(self, stale_minutes: int = 10) -> int:
        """Reclaim stranded .processing files back to .ndjson pending state.

        A .processing file untouched for > stale_minutes indicates the sender
        crashed mid-send. Renaming back to .ndjson makes the batch retryable.

        Returns:
            Number of files reclaimed.
        """
        cutoff = time.time() - (stale_minutes * 60)
        reclaimed = 0
        for entry in self.spool_dir.glob(f"*{PROCESSING_EXTENSION}"):
            if not entry.is_file():
                continue
            try:
                if entry.stat().st_mtime >= cutoff:
                    continue
                new_path = entry.with_suffix(NDJSON_EXTENSION)
                if new_path.exists():
                    new_path.unlink()
                os.rename(str(entry), str(new_path))
                reclaimed += 1
            except (FileNotFoundError, OSError):
                pass
        if reclaimed:
            logger.info("stale_processing_reclaimed", count=reclaimed)
        return reclaimed

    def cleanup_old_files(self, max_age_hours: int = 24) -> int:
        """Remove files older than max_age_hours from spool.

        Never removes .ndjson (un-sent) or .processing (in-flight) files.
        """
        cutoff = time.time() - (max_age_hours * 3600)
        removed = 0
        for entry in self.spool_dir.iterdir():
            if not entry.is_file():
                continue
            if entry.suffix in (NDJSON_EXTENSION, PROCESSING_EXTENSION):
                continue
            try:
                if entry.stat().st_mtime < cutoff:
                    entry.unlink()
                    removed += 1
            except FileNotFoundError:
                pass
        if removed:
            logger.info("cleanup_completed", removed_count=removed)
        return removed

    def get_spool_stats(self) -> dict[str, float]:
        """Get spool directory statistics."""
        total_size = self._get_spool_size()
        pending = len(self.list_pending())

        dead_letter_count = 0
        if self.dead_letter_dir.exists():
            dead_letter_count = sum(1 for _ in self.dead_letter_dir.iterdir() if _.is_file())

        processing_count = 0
        if self.spool_dir.exists():
            processing_count = sum(
                1 for _ in self.spool_dir.glob(f"*{PROCESSING_EXTENSION}") if _.is_file()
            )

        return {
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "pending_count": pending,
            "dead_letter_count": dead_letter_count,
            "processing_count": processing_count,
        }

    def _get_spool_size(self) -> int:
        return self._scan_spool_size()

    def _scan_spool_size(self) -> int:
        if not self.spool_dir.exists():
            return 0
        total = 0
        for entry in self.spool_dir.iterdir():
            if entry.is_file():
                with contextlib.suppress(FileNotFoundError):
                    total += entry.stat().st_size
        return total
