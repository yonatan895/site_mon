import os
from datetime import UTC, datetime

import pytest

from src.utils import (
    atomic_read,
    atomic_write,
    calculate_backoff,
    ensure_dir,
    format_timestamp,
    retry_with_backoff,
    setup_logging,
    slugify,
)


class TestSetupLogging:
    def test_returns_logger(self) -> None:
        logger = setup_logging("test_logger")
        assert logger is not None


class TestAtomicWrite:
    def test_write_and_read_dict(self, tmp_path: str) -> None:
        filepath = str(tmp_path) + "/test.json"
        data = {"key": "value", "nested": {"a": 1}}
        atomic_write(filepath, data)
        result = atomic_read(filepath)
        assert result == data

    def test_write_and_read_list(self, tmp_path: str) -> None:
        filepath = str(tmp_path) + "/test.json"
        data = [{"a": 1}, {"b": 2}]
        atomic_write(filepath, data)
        result = atomic_read(filepath)
        assert result == data

    def test_no_tmp_file_left(self, tmp_path: str) -> None:
        filepath = str(tmp_path) + "/test.json"
        atomic_write(filepath, {"k": "v"})
        assert not os.path.exists(filepath + ".tmp")

    def test_creates_parent_dirs(self, tmp_path: str) -> None:
        filepath = str(tmp_path) + "/subdir/nested/test.json"
        atomic_write(filepath, {"k": "v"})
        assert os.path.exists(filepath)
        result = atomic_read(filepath)
        assert result == {"k": "v"}

    def test_read_nonexistent_file(self, tmp_path: str) -> None:
        with pytest.raises(FileNotFoundError):
            atomic_read(str(tmp_path) + "/nonexistent.json")


class TestEnsureDir:
    def test_creates_dir(self, tmp_path: str) -> None:
        dirpath = str(tmp_path) + "/newdir"
        ensure_dir(dirpath)
        assert os.path.isdir(dirpath)

    def test_no_error_if_exists(self, tmp_path: str) -> None:
        dirpath = str(tmp_path) + "/newdir"
        ensure_dir(dirpath)
        ensure_dir(dirpath)
        assert os.path.isdir(dirpath)

    def test_creates_nested(self, tmp_path: str) -> None:
        dirpath = str(tmp_path) + "/a/b/c"
        ensure_dir(dirpath)
        assert os.path.isdir(dirpath)


class TestFormatTimestamp:
    def test_returns_iso_format(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = format_timestamp(dt)
        assert result == "2024-01-15T10:30:45.123456Z"

    def test_defaults_to_now_utc(self) -> None:
        result = format_timestamp()
        assert result.endswith("Z")
        assert "T" in result


class TestCalculateBackoff:
    def test_first_attempt(self) -> None:
        for _ in range(20):
            result = calculate_backoff(0, base=1, max_wait=64)
            assert 1.0 <= result <= 1.5

    def test_fourth_attempt_range(self) -> None:
        for _ in range(20):
            result = calculate_backoff(3, base=1, max_wait=64)
            assert 8.0 <= result <= 12.0

    def test_capped_at_max(self) -> None:
        for _ in range(20):
            result = calculate_backoff(10, base=1, max_wait=16)
            assert result <= 24.0


class TestRetryWithBackoff:
    def test_successful_call_no_retry(self) -> None:
        call_count = 0

        @retry_with_backoff(max_attempts=3, base_delay=0.01, max_delay=0.05)
        def succeed() -> bool:
            nonlocal call_count
            call_count += 1
            return True

        result = succeed()
        assert result is True
        assert call_count == 1

    def test_retries_on_failure(self) -> None:
        call_count = 0

        @retry_with_backoff(max_attempts=3, base_delay=0.01, max_delay=0.05)
        def fail_then_succeed() -> bool:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("fail")
            return True

        result = fail_then_succeed()
        assert result is True
        assert call_count == 3

    def test_eventual_failure(self) -> None:
        @retry_with_backoff(max_attempts=2, base_delay=0.01, max_delay=0.05)
        def always_fail() -> bool:
            raise RuntimeError("always")

        with pytest.raises(RuntimeError):
            always_fail()


class TestSlugify:
    def test_lowercase_and_replace_spaces(self) -> None:
        assert slugify("Hello World") == "hello-world"

    def test_strips_special_chars(self) -> None:
        assert slugify("test@#$%ing") == "testing"

    def test_multiple_hyphens_collapsed(self) -> None:
        assert slugify("a   b__c") == "a-b-c"

    def test_leading_trailing_hyphens_removed(self) -> None:
        assert slugify("-hello-world-") == "hello-world"
