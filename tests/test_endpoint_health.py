
import pytest
import requests_mock

from src.endpoint_health import EndpointHealthChecker
from src.models import HealthStatus, SourceEndpoint


@pytest.fixture
def test_endpoints() -> list[SourceEndpoint]:
    return [
        SourceEndpoint(
            name="ep1",
            url="https://ep1.example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        ),
        SourceEndpoint(
            name="ep2",
            url="https://ep2.example.com",
            platform="hmc",
            site="backup",
            auth_type="basic",
            creds_vault_path="secret/test",
        ),
    ]


@pytest.fixture
def checker(test_endpoints: list[SourceEndpoint]) -> EndpointHealthChecker:
    return EndpointHealthChecker(
        endpoints=test_endpoints,
        check_interval=1,
        health_path="/health",
    )


class TestInit:
    def test_initial_statuses_created(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(test_endpoints)
        assert len(checker._statuses) == 2
        assert checker._statuses["ep1"].is_healthy is True
        assert checker._statuses["ep2"].is_healthy is True

    def test_thread_not_started_by_default(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(test_endpoints)
        assert checker._thread is None


class TestStartStop:
    def test_start_creates_thread(
        self, checker: EndpointHealthChecker
    ) -> None:
        checker.start()
        assert checker._thread is not None
        assert checker._thread.is_alive()
        checker.stop()

    def test_double_start_no_error(
        self, checker: EndpointHealthChecker
    ) -> None:
        checker.start()
        checker.start()
        assert checker._thread is not None
        checker.stop()

    def test_stop_joins_thread(
        self, checker: EndpointHealthChecker
    ) -> None:
        checker.start()
        assert checker._thread is not None
        checker.stop()
        checker._thread.join(timeout=2)
        assert not checker._thread.is_alive()

    def test_stop_without_start(
        self, checker: EndpointHealthChecker
    ) -> None:
        checker.stop()


class TestHealthProbes:
    def test_successful_probe_marks_healthy(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(endpoints=test_endpoints, check_interval=1)

        with requests_mock.Mocker() as m:
            m.get("https://ep1.example.com/health", status_code=200)
            m.get("https://ep2.example.com/health", status_code=200)
            checker._run_checks()

        assert checker.is_healthy("ep1") is True
        assert checker.is_healthy("ep2") is True

    def test_failed_probe_marks_unhealthy(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(endpoints=test_endpoints, check_interval=1)

        with requests_mock.Mocker() as m:
            m.get("https://ep1.example.com/health", status_code=500)
            m.get("https://ep2.example.com/health", status_code=200)
            checker._run_checks()

        assert checker.is_healthy("ep1") is False
        assert checker.is_healthy("ep2") is True
        status = checker.get_status("ep1")
        assert status.consecutive_failures == 1

    def test_consecutive_failures_threshold(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(endpoints=test_endpoints[:1], check_interval=1)

        with requests_mock.Mocker() as m:
            m.get("https://ep1.example.com/health", status_code=500)
            for _ in range(4):
                checker._run_checks()

        status = checker.get_status("ep1")
        assert status.consecutive_failures == 4
        assert status.is_healthy is False
        assert status.degraded_since is not None

    def test_recovery_after_failures(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(endpoints=test_endpoints[:1], check_interval=1)

        with requests_mock.Mocker() as m:
            m.get("https://ep1.example.com/health", status_code=500)
            checker._run_checks()

        assert checker.is_healthy("ep1") is False

        with requests_mock.Mocker() as m:
            m.get("https://ep1.example.com/health", status_code=200)
            checker._run_checks()

        assert checker.is_healthy("ep1") is True
        status = checker.get_status("ep1")
        assert status.consecutive_failures == 0
        assert status.degraded_since is None

    def test_timeout_considered_failure(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(endpoints=test_endpoints[:1], check_interval=1)

        with requests_mock.Mocker() as m:
            m.get("https://ep1.example.com/health", exc=Exception("timeout"))
            checker._run_checks()

        assert checker.is_healthy("ep1") is False


class TestGetStatuses:
    def test_get_all_statuses(
        self, test_endpoints: list[SourceEndpoint]
    ) -> None:
        checker = EndpointHealthChecker(endpoints=test_endpoints, check_interval=1)

        all_statuses = checker.get_all_statuses()
        assert len(all_statuses) == 2
        assert "ep1" in all_statuses
        assert "ep2" in all_statuses

    def test_get_status_unknown_endpoint(
        self, checker: EndpointHealthChecker
    ) -> None:
        status = checker.get_status("nonexistent")
        assert isinstance(status, HealthStatus)
        assert status.is_healthy is False
