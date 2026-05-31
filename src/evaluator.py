"""Rule evaluation engine for transforming and threshold-checking raw data."""

import operator
import re
from datetime import datetime, timezone
from typing import Any, Union

import jmespath
import structlog

from .models import (
    FieldExtraction,
    PlatformRule,
    PollingEvent,
    SiteConfig,
    ThresholdRule,
)
from .utils import setup_logging

logger = setup_logging(__name__)

OPERATOR_FUNCTIONS = {
    "eq": operator.eq,
    "ne": operator.ne,
    "gt": operator.gt,
    "lt": operator.lt,
    "gte": operator.ge,
    "lte": operator.le,
}


class Evaluator:
    """Applies field extractions and threshold rules to raw API response data.

    Transforms raw data into PollingEvent objects with alerts for breached thresholds.
    """

    def __init__(
        self,
        platform_rules: dict[str, PlatformRule],
        site_config: SiteConfig,
    ) -> None:
        """Initialize the evaluator.

        Args:
            platform_rules: Dictionary of data_type -> PlatformRule.
            site_config: Site configuration with platform and site metadata.
        """
        self.platform_rules = platform_rules
        self.site_config = site_config
        logger.info(
            "evaluator_initialized",
            data_types=list(platform_rules.keys()),
            site=site_config.site_name,
        )

    def evaluate(
        self,
        data_type: str,
        raw_data: Union[dict[str, Any], list[dict[str, Any]]],
        endpoint_name: str,
    ) -> Union[PollingEvent, list[PollingEvent]]:
        """Evaluate raw API response data against configured rules.

        Args:
            data_type: The data type being processed (maps to a PlatformRule).
            raw_data: Raw API response, either a single dict or list of dicts.
            endpoint_name: Name of the source endpoint.

        Returns:
            A single PollingEvent or list of PollingEvent objects with alerts.
        """
        rule = self.platform_rules.get(data_type)
        if rule is None:
            logger.warning("no_rule_for_data_type", data_type=data_type)
            return []

        if isinstance(raw_data, list):
            events: list[PollingEvent] = []
            for item in raw_data:
                event = self._evaluate_item(
                    data_type, item, rule, endpoint_name
                )
                if event is not None:
                    events.append(event)
            logger.debug(
                "batch_evaluated",
                data_type=data_type,
                item_count=len(events),
            )
            return events

        return self._evaluate_item(data_type, raw_data, rule, endpoint_name)

    def _evaluate_item(
        self,
        data_type: str,
        raw_item: dict[str, Any],
        rule: PlatformRule,
        endpoint_name: str,
    ) -> PollingEvent:
        """Evaluate a single raw data item against the platform rule.

        Args:
            data_type: Data type identifier.
            raw_item: Single raw response item.
            rule: PlatformRule to apply.
            endpoint_name: Source endpoint name.

        Returns:
            PollingEvent with extracted fields and any alerts.
        """
        fields = self._extract_fields(raw_item, rule.extractions)

        # Merge common fields (platform, site, timestamp, etc.)
        common = {
            "platform": self.site_config.platform,
            "site": self.site_config.site_name,
            "sourcetype": rule.sourcetype,
            "index": rule.index,
            "endpoint": endpoint_name,
            **rule.common_fields,
        }
        fields = {**common, **fields}

        alerts = self._check_thresholds(fields, rule.thresholds)

        metadata = {
            "data_type": data_type,
            "rule_name": rule.name,
            "extraction_count": len(rule.extractions),
            "threshold_count": len(rule.thresholds),
        }

        event = PollingEvent(
            platform=self.site_config.platform,
            site=self.site_config.site_name,
            data_type=data_type,
            sourcetype=rule.sourcetype,
            index=rule.index,
            timestamp=datetime.now(timezone.utc),
            fields=fields,
            alerts=alerts,
            raw_response_metadata=metadata,
        )

        if alerts:
            logger.info(
                "threshold_alerts_generated",
                data_type=data_type,
                alert_count=len(alerts),
                severities=[a["severity"] for a in alerts],
            )

        return event

    def _extract_fields(
        self, raw_item: dict[str, Any], extractions: list[FieldExtraction]
    ) -> dict[str, Any]:
        """Apply JMESPath extractions to a single raw data item.

        Args:
            raw_item: Raw response dictionary.
            extractions: List of FieldExtraction configurations.

        Returns:
            Dictionary of extracted field names to their values.
        """
        fields: dict[str, Any] = {}
        for extraction in extractions:
            try:
                value = jmespath.search(extraction.json_path, raw_item)
                if value is None and extraction.default is not None:
                    value = extraction.default
                if value is not None and extraction.transform:
                    value = self._apply_transform(value, extraction.transform)
                fields[extraction.field_name] = value
            except Exception as e:
                logger.warning(
                    "field_extraction_failed",
                    field=extraction.field_name,
                    json_path=extraction.json_path,
                    error=str(e),
                )
                fields[extraction.field_name] = extraction.default

        if not extractions:
            # Pass through all raw fields
            fields.update(raw_item)

        return fields

    def _check_thresholds(
        self, fields: dict[str, Any], thresholds: list[ThresholdRule]
    ) -> list[dict[str, Any]]:
        """Evaluate all threshold rules against extracted fields.

        Args:
            fields: Extracted field values.
            thresholds: List of ThresholdRule configurations.

        Returns:
            List of alert dictionaries for breached thresholds.
        """
        alerts: list[dict[str, Any]] = []
        for rule in thresholds:
            try:
                field_value = fields.get(rule.field)
                if field_value is None:
                    continue

                breached = self._evaluate_threshold(field_value, rule)
                if breached:
                    message = rule.message_template.format(
                        field=rule.field,
                        value=field_value,
                        threshold=rule.value,
                        **fields,
                    )
                    alerts.append({
                        "field": rule.field,
                        "severity": rule.severity,
                        "message": message,
                        "threshold": str(rule.value),
                        "actual": str(field_value),
                        "operator": rule.operator,
                    })
            except Exception as e:
                logger.warning(
                    "threshold_evaluation_failed",
                    field=rule.field,
                    operator=rule.operator,
                    error=str(e),
                )

        return alerts

    def _evaluate_threshold(self, field_value: Any, rule: ThresholdRule) -> bool:
        """Evaluate a single threshold rule against a field value.

        Args:
            field_value: The value extracted from the field.
            rule: The threshold rule to apply.

        Returns:
            True if the threshold is breached.
        """
        operator_name = rule.operator

        if operator_name in OPERATOR_FUNCTIONS:
            try:
                return OPERATOR_FUNCTIONS[operator_name](field_value, rule.value)
            except TypeError:
                return OPERATOR_FUNCTIONS[operator_name](
                    str(field_value), rule.value
                )

        if operator_name == "contains":
            return str(rule.value).lower() in str(field_value).lower()

        if operator_name == "regex":
            try:
                return bool(re.search(str(rule.value), str(field_value)))
            except re.error:
                logger.warning(
                    "invalid_regex_pattern",
                    pattern=str(rule.value),
                )
                return False

        logger.warning("unknown_operator", operator=operator_name)
        return False

    def _apply_transform(self, value: Any, transform_type: str) -> Any:
        """Convert a value based on the specified transform type.

        Args:
            value: The raw value to transform.
            transform_type: One of "int", "float", "str", "bool".

        Returns:
            Transformed value.
        """
        if transform_type == "int":
            return int(value)
        if transform_type == "float":
            return float(value)
        if transform_type == "str":
            return str(value)
        if transform_type == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)
        logger.warning("unknown_transform", transform=transform_type)
        return value
