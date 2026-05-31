from typing import Any

import pytest

from src.evaluator import Evaluator
from src.models import (
    FieldExtraction,
    PlatformRule,
    SiteConfig,
    ThresholdRule,
)


@pytest.fixture
def base_site_config() -> SiteConfig:
    return SiteConfig(site_name="test-site", platform="hmc")


@pytest.fixture
def evaluator(
    sample_platform_rule: PlatformRule,
    base_site_config: SiteConfig,
) -> Evaluator:
    return Evaluator(
        platform_rules={"cpc-stats": sample_platform_rule},
        site_config=base_site_config,
    )


class TestEvaluateSingleItem:
    def test_extracts_fields_from_raw_item(
        self,
        evaluator: Evaluator,
        sample_raw_response_item: dict[str, Any],
    ) -> None:
        result = evaluator.evaluate("cpc-stats", sample_raw_response_item, "test-endpoint")
        assert result.platform == "hmc"
        assert result.fields["cpc_name"] == "CPCA"
        assert result.fields["status"] == "operating"
        assert result.fields["cpu_usage_pct"] == 45.2
        assert result.fields["energy_watts"] == 4200

    def test_no_alerts_when_no_thresholds_breached(
        self,
        evaluator: Evaluator,
        sample_raw_response_item: dict[str, Any],
    ) -> None:
        result = evaluator.evaluate("cpc-stats", sample_raw_response_item, "test-endpoint")
        assert len(result.alerts) == 0

    def test_alerts_generated_for_breached_thresholds(
        self,
        evaluator: Evaluator,
        sample_raw_response_alerting_item: dict[str, Any],
    ) -> None:
        result = evaluator.evaluate("cpc-stats", sample_raw_response_alerting_item, "test-endpoint")
        assert len(result.alerts) == 3
        severities = [a["severity"] for a in result.alerts]
        assert "critical" in severities
        assert "warning" in severities
        assert "info" in severities


class TestEvaluateBatch:
    def test_batch_of_items(
        self,
        evaluator: Evaluator,
    ) -> None:
        items = [
            {"cpc_name": "CPA", "status": "operating", "cpu_usage_pct": 30.0},
            {"cpc_name": "CPB", "status": "operating", "cpu_usage_pct": 40.0},
        ]
        result = evaluator.evaluate("cpc-stats", items, "test-ep")
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].fields["cpc_name"] == "CPA"
        assert result[1].fields["cpc_name"] == "CPB"

    def test_empty_list(self, evaluator: Evaluator) -> None:
        result = evaluator.evaluate("cpc-stats", [], "test-ep")
        assert result == []

    def test_no_rule_for_data_type(
        self,
        base_site_config: SiteConfig,
    ) -> None:
        ev = Evaluator(
            platform_rules={
                "cpc-stats": PlatformRule(
                    name="cpc-stats",
                    data_type="cpc-stats",
                    sourcetype="test",
                    index="test",
                )
            },
            site_config=base_site_config,
        )
        result = ev.evaluate("unknown-type", {"x": 1}, "test-ep")
        assert result == []


class TestFieldExtraction:
    def test_default_value_used_when_jmespath_returns_none(
        self, base_site_config: SiteConfig
    ) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="missing_field",
                    json_path="nonexistent",
                    default="default_val",
                )
            ],
        )
        ev = Evaluator(
            platform_rules={"test": rule},
            site_config=base_site_config,
        )
        result = ev.evaluate("test", {"real": "data"}, "ep")
        assert result.fields["missing_field"] == "default_val"

    def test_passthrough_when_no_extractions(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
        )
        ev = Evaluator(
            platform_rules={"test": rule},
            site_config=base_site_config,
        )
        result = ev.evaluate("test", {"a": 1, "b": 2}, "ep")
        assert result.fields["a"] == 1
        assert result.fields["b"] == 2

    def test_common_fields_merged(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="custom_index",
            common_fields={"platform": "hmc", "custom": "val"},
        )
        ev = Evaluator(
            platform_rules={"test": rule},
            site_config=base_site_config,
        )
        result = ev.evaluate("test", {"a": 1}, "ep")
        assert result.fields["platform"] == "hmc"
        assert result.fields["custom"] == "val"
        assert result.index == "custom_index"
        assert result.sourcetype == "test"


class TestThresholdOperators:
    @pytest.fixture
    def threshold_evaluator(self, base_site_config: SiteConfig) -> Evaluator:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(field_name="val", json_path="val"),
            ],
            thresholds=[
                ThresholdRule(
                    field="val",
                    operator="eq",
                    value=10,
                    severity="warning",
                    message_template="{field}={value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="ne",
                    value=5,
                    severity="info",
                    message_template="{field}!={value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="gt",
                    value=8,
                    severity="warning",
                    message_template="{field}>{value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="lt",
                    value=3,
                    severity="info",
                    message_template="{field}<{value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="gte",
                    value=10,
                    severity="warning",
                    message_template="{field}>={value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="lte",
                    value=10,
                    severity="info",
                    message_template="{field}<={value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="contains",
                    value="ello",
                    severity="info",
                    message_template="{field} contains {value}",
                ),
                ThresholdRule(
                    field="val",
                    operator="regex",
                    value=r"^\d+$",
                    severity="info",
                    message_template="{field} matches {value}",
                ),
            ],
        )
        return Evaluator(
            platform_rules={"test": rule},
            site_config=base_site_config,
        )

    def test_eq_operator_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 10}, "ep")
        assert any(a["operator"] == "eq" for a in result.alerts)

    def test_eq_operator_not_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 11}, "ep")
        assert not any(a["operator"] == "eq" for a in result.alerts)

    def test_ne_operator_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 10}, "ep")
        assert any(a["operator"] == "ne" for a in result.alerts)

    def test_gt_operator_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 9}, "ep")
        assert any(a["operator"] == "gt" for a in result.alerts)

    def test_lt_operator_not_breached_when_equal(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 3}, "ep")
        assert not any(a["operator"] == "lt" for a in result.alerts)

    def test_gte_operator_breached_at_equal(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 10}, "ep")
        assert any(a["operator"] == "gte" for a in result.alerts)

    def test_lte_operator_breached_at_equal(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": 10}, "ep")
        assert any(a["operator"] == "lte" for a in result.alerts)

    def test_contains_operator_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": "hello world"}, "ep")
        assert any(a["operator"] == "contains" for a in result.alerts)

    def test_contains_operator_not_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": "goodbye"}, "ep")
        assert not any(a["operator"] == "contains" for a in result.alerts)

    def test_regex_operator_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": "12345"}, "ep")
        assert any(a["operator"] == "regex" for a in result.alerts)

    def test_regex_operator_not_breached(self, threshold_evaluator: Evaluator) -> None:
        result = threshold_evaluator.evaluate("test", {"val": "abc"}, "ep")
        assert not any(a["operator"] == "regex" for a in result.alerts)


class TestTransforms:
    def test_int_transform(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="count",
                    json_path="count",
                    transform="int",
                )
            ],
        )
        ev = Evaluator(platform_rules={"test": rule}, site_config=base_site_config)
        result = ev.evaluate("test", {"count": "42"}, "ep")
        assert result.fields["count"] == 42
        assert isinstance(result.fields["count"], int)

    def test_float_transform(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="pct",
                    json_path="pct",
                    transform="float",
                )
            ],
        )
        ev = Evaluator(platform_rules={"test": rule}, site_config=base_site_config)
        result = ev.evaluate("test", {"pct": "85.5"}, "ep")
        assert result.fields["pct"] == 85.5
        assert isinstance(result.fields["pct"], float)

    def test_str_transform(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="label",
                    json_path="label",
                    transform="str",
                )
            ],
        )
        ev = Evaluator(platform_rules={"test": rule}, site_config=base_site_config)
        result = ev.evaluate("test", {"label": 123}, "ep")
        assert result.fields["label"] == "123"
        assert isinstance(result.fields["label"], str)

    def test_bool_transform_true_string(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="active",
                    json_path="active",
                    transform="bool",
                )
            ],
        )
        ev = Evaluator(platform_rules={"test": rule}, site_config=base_site_config)
        result = ev.evaluate("test", {"active": "true"}, "ep")
        assert result.fields["active"] is True

    def test_bool_transform_false_string(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="active",
                    json_path="active",
                    transform="bool",
                )
            ],
        )
        ev = Evaluator(platform_rules={"test": rule}, site_config=base_site_config)
        result = ev.evaluate("test", {"active": "no"}, "ep")
        assert result.fields["active"] is False

    def test_bool_transform_bool_input(self, base_site_config: SiteConfig) -> None:
        rule = PlatformRule(
            name="test",
            data_type="test",
            sourcetype="test",
            index="test",
            extractions=[
                FieldExtraction(
                    field_name="active",
                    json_path="active",
                    transform="bool",
                )
            ],
        )
        ev = Evaluator(platform_rules={"test": rule}, site_config=base_site_config)
        result = ev.evaluate("test", {"active": False}, "ep")
        assert result.fields["active"] is False


class TestMetadata:
    def test_raw_response_metadata(self, evaluator: Evaluator) -> None:
        result = evaluator.evaluate("cpc-stats", {"cpc_name": "CPA", "status": "operating"}, "ep")
        assert result.raw_response_metadata["data_type"] == "cpc-stats"
        assert "rule_name" in result.raw_response_metadata
