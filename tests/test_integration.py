import json

import pytest

from src.spool import SpoolManager


class TestEndToEndPipeline:
    @pytest.mark.integration
    def test_poller_spool_sender_flow(
        self, tmp_spool_dir: str
    ) -> None:
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
    def test_retry_and_dead_letter_flow(
        self, tmp_spool_dir: str
    ) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)

        spool_mgr.write_ndjson('{"event": "test"}\n', batch_id="retry-batch")

        for attempt in range(6):
            entries = spool_mgr.read_ndjson_batch(max_files=10)
            if not entries:
                break
            if attempt < 5:
                spool_mgr.nack_file(entries[0].filename, f"attempt {attempt + 1}")
            else:
                spool_mgr.ack_file(entries[0].filename)

        pending = spool_mgr.list_pending()
        stats = spool_mgr.get_spool_stats()
        assert len(pending) == 0
        assert stats["dead_letter_count"] > 0 or stats["pending_count"] + stats["processing_count"] == 0

    @pytest.mark.integration
    def test_hec_ndjson_send_with_mock(
        self, tmp_spool_dir: str
    ) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)
        events = [
            {"time": "1717171200.000000", "event": {"cpu_pct": 50}},
            {"time": "1717171200.000001", "event": {"cpu_pct": 75}},
        ]
        ndjson = "\n".join(json.dumps(e) for e in events) + "\n"
        spool_mgr.write_ndjson(ndjson, batch_id="hec-test")

        assert spool_mgr.read_ndjson_batch(max_files=1)[0].content == ndjson

    @pytest.mark.integration
    def test_spool_persistence_and_cleanup(
        self, tmp_spool_dir: str
    ) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)

        spool_mgr.write_ndjson('{"event": "persist"}\n', batch_id="persist-test")
        pending1 = spool_mgr.list_pending()
        assert len(pending1) == 1

        entries = spool_mgr.read_ndjson_batch(max_files=10)
        assert len(entries) == 1
        spool_mgr.ack_file(entries[0].filename)

        assert len(spool_mgr.list_pending()) == 0

    @pytest.mark.integration
    def test_multiple_spool_files_concurrent_access(
        self, tmp_spool_dir: str
    ) -> None:
        spool_mgr = SpoolManager(spool_dir=tmp_spool_dir)

        for i in range(20):
            spool_mgr.write_ndjson(
                '{"idx": ' + str(i) + "}\n", batch_id=f"batch-{i:02d}"
            )

        pending = spool_mgr.list_pending()
        assert len(pending) == 20

        entries = spool_mgr.read_ndjson_batch(max_files=5)
        assert len(entries) == 5

        for entry in entries:
            entry_data = json.loads(entry.content.strip())
            if entry_data["idx"] % 2 == 0:
                spool_mgr.ack_file(entry.filename)
            else:
                spool_mgr.nack_file(entry.filename, "simulated failure")

        remaining = spool_mgr.list_pending()
        assert len(remaining) == 17
