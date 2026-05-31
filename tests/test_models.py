from datetime import UTC, datetime

import pydantic
import pytest

from src.models import (
    FieldExtraction,
    HealthStatus,
    PlatformRule,
    PollingEvent,
    SiteConfig,
    SourceEndpoint,
    ThresholdRule,
)


class TestSourceEndpoint:
    def test_valid_endpoint(self) -> None:
        ep = SourceEndpoint(
            name="test-ep",
            url="https://example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert ep.name == "test-ep"
        assert ep.rate_limit_rps == 5.0
        assert ep.timeout == 30

    def test_defaults(self) -> None:
        ep = SourceEndpoint(
            name="test-ep",
            url="https://example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        assert ep.rate_limit_rps == 5.0
        assert ep.timeout == 30

    def test_missing_required_fields(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            SourceEndpoint(name="test-ep")


class TestHealthStatus:
    def test_valid_status(self) -> None:
        hs = HealthStatus(endpoint_name="ep1")
        assert hs.endpoint_name == "ep1"
        assert hs.is_healthy is True
        assert hs.consecutive_failures == 0
        assert hs.max_consecutive_failures == 3
        assert hs.degraded_since is None

    def test_not_healthy_by_default(self) -> None:
        hs = HealthStatus(endpoint_name="ep1", is_healthy=False)
        assert hs.is_healthy is False


class TestFieldExtraction:
    def test_valid_extraction(self) -> None:
        fe = FieldExtraction(
            field_name="cpu_pct",
            json_path="$.cpu_usage_pct",
        )
        assert fe.field_name == "cpu_pct"
        assert fe.json_path == "$.cpu_usage_pct"
        assert fe.default is None
        assert fe.transform is None

    def test_with_default_and_transform(self) -> None:
        fe = FieldExtraction(
            field_name="count",
            json_path="$.count",
            default=0,
            transform="int",
        )
        assert fe.default == 0
        assert fe.transform == "int"

    def test_missing_required(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            FieldExtraction(field_name="x")


class TestThresholdRule:
    def test_valid_rule(self) -> None:
        tr = ThresholdRule(
            field="cpu_pct",
            operator="gt",
            value=90,
            severity="warning",
            message_template="CPU at {cpu_pct}%",
        )
        assert tr.operator == "gt"
        assert tr.value == 90
        assert tr.severity == "warning"

    def test_invalid_operator_still_accepted(self) -> None:
        tr = ThresholdRule(
            field="x",
            operator="unknown_op",
            value=0,
            severity="info",
            message_template="test",
        )
        assert tr.operator == "unknown_op"


class TestPlatformRule:
    def test_valid_rule(self) -> None:
        pr = PlatformRule(
            name="cpc-stats",
            data_type="cpc-stats",
            sourcetype="hmc:cpc",
            index="mainframe_metrics",
        )
        assert pr.name == "cpc-stats"
        assert pr.data_type == "cpc-stats"
        assert pr.interval_seconds == 300
        assert pr.extractions == []
        assert pr.thresholds == []
        assert pr.common_fields == {}

    def test_with_extractions_and_thresholds(self) -> None:
        extractions = [FieldExtraction(field_name="cpu", json_path="$.cpu")]
        thresholds = [
            ThresholdRule(
                field="cpu",
                operator="gt",
                value=90,
                severity="warning",
                message_template="high cpu",
            )
        ]
        pr = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test:test",
            index="test",
            extractions=extractions,
            thresholds=thresholds,
            common_fields={"platform": "hmc"},
        )
        assert len(pr.extractions) == 1
        assert len(pr.thresholds) == 1
        assert pr.common_fields == {"platform": "hmc"}


class TestSiteConfig:
    def test_valid_config(self) -> None:
        sc = SiteConfig(
            site_name="primary",
            platform="hmc",
        )
        assert sc.site_name == "primary"
        assert sc.platform == "hmc"
        assert sc.endpoints == []
        assert sc.data_types == []
        assert sc.is_primary is True

    def test_with_endpoints(self) -> None:
        ep = SourceEndpoint(
            name="ep1",
            url="https://example.com",
            platform="hmc",
            site="primary",
            auth_type="basic",
            creds_vault_path="secret/test",
        )
        sc = SiteConfig(
            site_name="primary",
            platform="hmc",
            endpoints=[ep],
            data_types=["cpc-stats"],
        )
        assert len(sc.endpoints) == 1
        assert sc.data_types == ["cpc-stats"]


class TestPollingEvent:
    def test_valid_event(self) -> None:
        pe = PollingEvent(
            platform="hmc",
            site="primary",
            data_type="cpc-stats",
            sourcetype="hmc:cpc",
            index="mainframe_metrics",
        )
        assert pe.platform == "hmc"
        assert pe.data_type == "cpc-stats"
        assert isinstance(pe.timestamp, datetime)
        assert pe.fields == {}
        assert pe.alerts == []
        assert pe.raw_response_metadata == {}

    def test_with_alerts(self) -> None:
        pe = PollingEvent(
            platform="hmc",
            site="primary",
            data_type="cpc-stats",
            sourcetype="hmc:cpc",
            index="mainframe_metrics",
            alerts=[
                {
                    "field": "status",
                    "severity": "critical",
                    "message": "CPC degraded",
                }
            ],
        )
        assert len(pe.alerts) == 1
        assert pe.alerts[0]["severity"] == "critical"

    def test_fields_populated(self) -> None:
        now = datetime.now(UTC)
        pe = PollingEvent(
            platform="hmc",
            site="primary",
            data_type="cpc-stats",
            sourcetype="hmc:cpc",
            index="mainframe_metrics",
            timestamp=now,
            fields={"cpc_name": "CPCA", "cpu_usage_pct": 45.2},
        )
        assert pe.fields == {"cpc_name": "CPCA", "cpu_usage_pct": 45.2}
        assert pe.timestamp == now
