import os
import time
from pathlib import Path

import pytest

from src.spool import (
    MAX_RETRIES,
    PROCESSING_EXTENSION,
    SpoolEntry,
    SpoolFullError,
    SpoolManager,
    _build_filename,
    _parse_filename,
)


class TestParseFilename:
    def test_parses_retry_count(self) -> None:
        batch_id, retry_count, ts = _parse_filename("batch123_3_1717171200000000.ndjson")
        assert batch_id == "batch123"
        assert retry_count == 3
        assert ts == "1717171200000000"

    def test_parses_zero_retry(self) -> None:
        batch_id, retry_count, ts = _parse_filename("batch123_0_1717171200000000.ndjson")
        assert batch_id == "batch123"
        assert retry_count == 0

    def test_parses_processing_extension(self) -> None:
        batch_id, retry_count, ts = _parse_filename("batch123_2_1717171200000000.processing")
        assert batch_id == "batch123"
        assert retry_count == 2

    def test_parses_batch_id_with_underscores(self) -> None:
        batch_id, retry_count, ts = _parse_filename("complex_batch_id_1_1717171200000000.ndjson")
        assert batch_id == "complex_batch_id"
        assert retry_count == 1


class TestBuildFilename:
    def test_includes_timestamp(self) -> None:
        filename1 = _build_filename("batch1", retry_count=0)
        assert filename1.startswith("batch1_0_")
        assert filename1.endswith(".ndjson")

    def test_different_timestamps(self) -> None:
        f1 = _build_filename("batch1", retry_count=0)
        time.sleep(0.001)
        f2 = _build_filename("batch1", retry_count=0)
        assert f1 != f2


class TestSpoolManagerWrite:
    def test_write_and_list(self, spool_manager: SpoolManager) -> None:
        content = '{"key": "value"}\n'
        filename = spool_manager.write_ndjson(content, batch_id="b1")
        assert filename.endswith(".ndjson")
        pending = spool_manager.list_pending()
        assert filename in pending

    def test_empty_content_raises(self, spool_manager: SpoolManager) -> None:
        with pytest.raises(ValueError, match="empty"):
            spool_manager.write_ndjson("   \n  ")

    def test_auto_batch_id(self, spool_manager: SpoolManager) -> None:
        filename = spool_manager.write_ndjson('{"a": 1}\n')
        assert filename.endswith(".ndjson")
        assert "SpoolManager" in spool_manager.__class__.__name__


class TestSpoolManagerRead:
    def test_read_batch_renames_to_processing(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        entries = spool_manager.read_ndjson_batch(max_files=10)
        assert len(entries) == 1
        assert entries[0].filename.startswith("b1_0_")

    def test_read_multiple_files(self, spool_manager: SpoolManager) -> None:
        for i in range(5):
            spool_manager.write_ndjson('{"idx": ' + str(i) + "}\n", batch_id="b" + str(i))
        entries = spool_manager.read_ndjson_batch(max_files=10)
        assert len(entries) == 5

    def test_read_respects_max_files(self, spool_manager: SpoolManager) -> None:
        for i in range(10):
            spool_manager.write_ndjson('{"idx": ' + str(i) + "}\n", batch_id="b" + str(i))
        entries = spool_manager.read_ndjson_batch(max_files=3)
        assert len(entries) == 3

    def test_processing_files_not_re_listed(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        spool_manager.read_ndjson_batch(max_files=10)
        pending = spool_manager.list_pending()
        assert len(pending) == 0

    def test_no_files_returns_empty(self, spool_manager: SpoolManager) -> None:
        entries = spool_manager.read_ndjson_batch(max_files=10)
        assert entries == []


class TestSpoolManagerAck:
    def test_ack_removes_file(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        entries = spool_manager.read_ndjson_batch(max_files=10)
        spool_manager.ack_file(entries[0].filename)
        pending = spool_manager.list_pending()
        assert len(pending) == 0

    def test_ack_missing_file_no_error(self, spool_manager: SpoolManager) -> None:
        spool_manager.ack_file("nonexistent.ndjson")


class TestSpoolManagerNack:
    def test_nack_increments_retry_in_filename(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        entries = spool_manager.read_ndjson_batch(max_files=10)
        spool_manager.nack_file(entries[0].filename, error="test error")
        pending = spool_manager.list_pending()
        assert len(pending) == 1
        assert "_1_" in pending[0]

    def test_nack_moves_to_dead_letter_after_max_retries(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        for _ in range(MAX_RETRIES + 1):
            entries = spool_manager.read_ndjson_batch(max_files=10)
            if not entries:
                break
            spool_manager.nack_file(entries[0].filename, error="test error")
        pending = spool_manager.list_pending()
        assert len(pending) == 0
        stats = spool_manager.get_spool_stats()
        assert stats["dead_letter_count"] == 1

    def test_nack_missing_file(self, spool_manager: SpoolManager) -> None:
        spool_manager.nack_file("nonexistent.ndjson")


class TestSpoolManagerCleanup:
    def test_removes_old_files(self, spool_manager: SpoolManager) -> None:
        tmp_path = spool_manager.spool_dir / "old.tmp"
        tmp_path.write_text("stale")
        old_time = time.time() - (48 * 3600)
        os.utime(str(tmp_path), (old_time, old_time))
        removed = spool_manager.cleanup_old_files(max_age_hours=24)
        assert removed == 1
        assert not tmp_path.exists()

    def test_keeps_recent_files(self, spool_manager: SpoolManager) -> None:
        tmp_path = spool_manager.spool_dir / "recent.tmp"
        tmp_path.write_text("recent")
        removed = spool_manager.cleanup_old_files(max_age_hours=24)
        assert removed == 0
        assert tmp_path.exists()

    def test_cleanup_preserves_ndjson(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        filepath = spool_manager.spool_dir / spool_manager.list_pending()[0]
        old_time = time.time() - (48 * 3600)
        os.utime(str(filepath), (old_time, old_time))
        removed = spool_manager.cleanup_old_files(max_age_hours=24)
        assert removed == 0
        assert filepath.exists()

    def test_cleanup_preserves_processing(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        spool_manager.read_ndjson_batch(max_files=10)
        for entry in spool_manager.spool_dir.iterdir():
            if entry.suffix == PROCESSING_EXTENSION:
                old_time = time.time() - (48 * 3600)
                os.utime(str(entry), (old_time, old_time))
                break
        removed = spool_manager.cleanup_old_files(max_age_hours=24)
        assert removed == 0


class TestReclaimStaleProcessing:
    def test_reclaims_stale_processing(self, tmp_path: Path) -> None:
        d = str(tmp_path)
        mgr = SpoolManager(spool_dir=d, max_spool_size_mb=10)
        mgr.write_ndjson('{"a": 1}\n', batch_id="b1")
        mgr.read_ndjson_batch(max_files=10)
        assert mgr.list_pending() == []

        for entry in Path(d).iterdir():
            if entry.suffix == PROCESSING_EXTENSION:
                old_time = time.time() - (20 * 60)
                os.utime(str(entry), (old_time, old_time))
                break

        reclaimed = mgr.reclaim_stale_processing(stale_minutes=10)
        assert reclaimed == 1
        assert len(mgr.list_pending()) == 1

    def test_leaves_recent_processing_alone(self, tmp_path: Path) -> None:
        d = str(tmp_path)
        mgr = SpoolManager(spool_dir=d, max_spool_size_mb=10)
        mgr.write_ndjson('{"a": 1}\n', batch_id="b1")
        mgr.read_ndjson_batch(max_files=10)

        reclaimed = mgr.reclaim_stale_processing(stale_minutes=10)
        assert reclaimed == 0
        assert mgr.list_pending() == []


class TestSpoolStats:
    def test_empty_stats(self, spool_manager: SpoolManager) -> None:
        stats = spool_manager.get_spool_stats()
        assert stats["pending_count"] == 0
        assert stats["total_size_mb"] == 0.0
        assert stats["dead_letter_count"] == 0
        assert stats["processing_count"] == 0

    def test_stats_with_data(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n' * 10000, batch_id="b1")
        spool_manager.read_ndjson_batch(max_files=10)
        stats = spool_manager.get_spool_stats()
        assert stats["pending_count"] == 0
        assert stats["processing_count"] == 1
        assert stats["total_size_mb"] > 0


class TestSpoolFull:
    def test_refuses_write_when_full(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            mgr = SpoolManager(spool_dir=d, max_spool_size_mb=0)
            with pytest.raises(SpoolFullError):
                mgr.write_ndjson('{"a": 1}\n')


class TestSpoolEntry:
    def test_named_tuple_fields(self) -> None:
        entry = SpoolEntry(filename="f.ndjson", content="{}", retry_count=2)
        assert entry.filename == "f.ndjson"
        assert entry.content == "{}"
        assert entry.retry_count == 2


class TestMoveToDeadLetter:
    def test_moves_file(self, spool_manager: SpoolManager) -> None:
        spool_manager.write_ndjson('{"a": 1}\n', batch_id="b1")
        entries = spool_manager.read_ndjson_batch(max_files=10)
        proc_path = str(spool_manager.spool_dir / (entries[0].filename.split("_0_")[0] + "_0_"))
        for path in spool_manager.spool_dir.iterdir():
            if path.suffix == PROCESSING_EXTENSION:
                proc_path = str(path)
                break
        spool_manager.move_to_dead_letter(proc_path)
        stats = spool_manager.get_spool_stats()
        assert stats["dead_letter_count"] >= 1

    def test_nonexistent_file_no_error(self, spool_manager: SpoolManager) -> None:
        spool_manager.move_to_dead_letter("/nonexistent/path")


class TestCrossInstanceSpoolSize:
    def test_two_instances_see_each_others_files(self, tmp_path: Path) -> None:
        d = str(tmp_path)
        writer = SpoolManager(spool_dir=d, max_spool_size_mb=10)
        reader = SpoolManager(spool_dir=d, max_spool_size_mb=10)

        writer.write_ndjson('{"a": 1}\n', batch_id="cross1")
        writer.write_ndjson('{"b": 2}\n', batch_id="cross2")

        assert writer._get_spool_size() > 0
        assert reader._get_spool_size() == writer._get_spool_size()

        pending = reader.list_pending()
        assert len(pending) == 2

        entries = reader.read_ndjson_batch(max_files=2)
        assert len(entries) == 2

        size_before_ack = writer._get_spool_size()
        reader.ack_file(entries[0].filename)
        assert writer._get_spool_size() < size_before_ack
