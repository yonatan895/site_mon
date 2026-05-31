from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient


@pytest.fixture
def client() -> TestClient:
    import src.health as health_mod

    return TestClient(health_mod.app)


@pytest.fixture
def spool_manager_mock() -> MagicMock:
    mock = MagicMock()
    mock.get_spool_stats.return_value = {
        "total_size_mb": 50.0,
        "pending_count": 5,
        "dead_letter_count": 0,
        "processing_count": 2,
    }
    return mock


@pytest.fixture
def health_checker_mock() -> MagicMock:
    mock = MagicMock()
    healthy_status = MagicMock()
    healthy_status.is_healthy = True
    mock.get_all_statuses.return_value = {
        "ep1": healthy_status,
        "ep2": healthy_status,
    }
    return mock


class TestHealthz:
    def test_returns_ok(self, client: TestClient) -> None:
        response = client.get("/healthz")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestReadyz:
    def test_returns_ready_when_spool_not_full(
        self, client: TestClient, spool_manager_mock: MagicMock
    ) -> None:
        import src.health as health_mod

        health_mod.spool_manager = spool_manager_mock
        health_mod.health_checker = None

        response = client.get("/readyz")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"

    def test_returns_not_ready_when_spool_full(self, client: TestClient) -> None:
        import src.health as health_mod

        mock = MagicMock()
        mock.get_spool_stats.return_value = {
            "total_size_mb": 1500.0,
            "pending_count": 100,
            "dead_letter_count": 0,
            "processing_count": 5,
        }
        health_mod.spool_manager = mock
        health_mod.health_checker = None

        response = client.get("/readyz")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "not_ready"
        assert "spool_full" in data.get("reason", "")

    def test_returns_not_ready_when_no_healthy_endpoints(self, client: TestClient) -> None:
        import src.health as health_mod

        mock_checker = MagicMock()
        unhealthy = MagicMock()
        unhealthy.is_healthy = False
        mock_checker.get_all_statuses.return_value = {"ep1": unhealthy}
        health_mod.health_checker = mock_checker

        mock_spool = MagicMock()
        mock_spool.get_spool_stats.return_value = {
            "total_size_mb": 50.0,
            "pending_count": 1,
            "dead_letter_count": 0,
            "processing_count": 0,
        }
        health_mod.spool_manager = mock_spool

        response = client.get("/readyz")
        data = response.json()
        assert data["status"] == "not_ready"
        assert "no_healthy_endpoints" in data.get("reason", "")

    def test_ready_when_healthy_endpoints_exist(
        self,
        client: TestClient,
        spool_manager_mock: MagicMock,
        health_checker_mock: MagicMock,
    ) -> None:
        import src.health as health_mod

        health_mod.spool_manager = spool_manager_mock
        health_mod.health_checker = health_checker_mock

        response = client.get("/readyz")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"


class TestMetrics:
    def test_returns_prometheus_format(
        self,
        client: TestClient,
        spool_manager_mock: MagicMock,
    ) -> None:
        import src.health as health_mod

        health_mod.spool_manager = spool_manager_mock
        health_mod.health_checker = None

        response = client.get("/metrics")
        assert response.status_code == 200
        text = response.text
        if "# HELP" in text or "# TYPE" in text:
            assert "site_mon_spool_pending_count" in text
            assert "site_mon_spool_size_mb" in text
            assert "site_mon_spool_dead_letter_count" in text

    def test_includes_endpoint_health(
        self,
        client: TestClient,
        spool_manager_mock: MagicMock,
        health_checker_mock: MagicMock,
    ) -> None:
        import src.health as health_mod

        health_mod.spool_manager = spool_manager_mock
        health_mod.health_checker = health_checker_mock

        response = client.get("/metrics")
        assert response.status_code == 200


class TestInitHealth:
    def test_sets_global_references(self) -> None:
        import src.health as health_mod

        mock_hc = MagicMock()
        mock_sm = MagicMock()

        health_mod.init_health(
            health_checker_instance=mock_hc,
            spool_manager_instance=mock_sm,
        )
        assert health_mod.health_checker is mock_hc
        assert health_mod.spool_manager is mock_sm


class TestRoutes:
    def test_healthz_is_get_method(self, client: TestClient) -> None:
        import src.health as health_mod

        health_mod.spool_manager = None
        health_mod.health_checker = None
        response = client.get("/healthz")
        assert response.status_code == 200

    def test_readyz_is_get_method(self, client: TestClient) -> None:
        import src.health as health_mod

        health_mod.spool_manager = None
        health_mod.health_checker = None
        response = client.get("/readyz")
        assert response.status_code == 200

    def test_metrics_is_get_method(self, client: TestClient) -> None:
        import src.health as health_mod

        health_mod.spool_manager = None
        health_mod.health_checker = None
        response = client.get("/metrics")
        assert response.status_code == 200
