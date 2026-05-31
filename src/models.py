"""Pydantic models for the mainframe infrastructure monitoring pipeline."""

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class SpoolRecord(BaseModel):
    """A batch of events spooled for delivery to Splunk HEC."""
    batch_id: str
    events: list[dict[str, Any]]
    platform: str
    site: str
    endpoint: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    retry_count: int = 0
    max_retries: int = 5


class SourceEndpoint(BaseModel):
    """Endpoint configuration for a data source."""
    name: str
    url: str
    platform: str
    site: str
    auth_type: str  # "basic" | "token" | "cert"
    creds_vault_path: str
    rate_limit_rps: float = 5.0
    timeout: int = 30


class HealthStatus(BaseModel):
    """Health status of a monitored endpoint."""
    endpoint_name: str
    is_healthy: bool = True
    last_check: datetime | None = None
    consecutive_failures: int = 0
    max_consecutive_failures: int = 3
    degraded_since: datetime | None = None
    response_time_ms: float | None = None


class FieldExtraction(BaseModel):
    """Defines how to extract a field from raw response data."""
    field_name: str
    json_path: str  # jmespath expression
    default: Any | None = None
    transform: str | None = None  # "int" | "float" | "str" | "bool"


class ThresholdRule(BaseModel):
    """Rule for evaluating a field value against a threshold."""
    field: str
    operator: str  # "eq" | "ne" | "gt" | "lt" | "gte" | "lte" | "contains" | "regex"
    value: Any
    severity: str  # "info" | "warning" | "critical"
    message_template: str


class PlatformRule(BaseModel):
    """Complete rule configuration for a data type on a platform."""
    name: str
    data_type: str
    sourcetype: str
    index: str
    interval_seconds: int = 300
    extractions: list[FieldExtraction] = Field(default_factory=list)
    thresholds: list[ThresholdRule] = Field(default_factory=list)
    common_fields: dict[str, Any] = Field(default_factory=dict)


class SiteConfig(BaseModel):
    """Configuration for a site within a platform."""
    site_name: str
    platform: str
    endpoints: list[SourceEndpoint] = Field(default_factory=list)
    data_types: list[str] = Field(default_factory=list)
    is_primary: bool = True


class PollingEvent(BaseModel):
    """Result of a single polling cycle for a data type."""
    platform: str
    site: str
    data_type: str
    sourcetype: str
    index: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    fields: dict[str, Any] = Field(default_factory=dict)
    alerts: list[dict[str, Any]] = Field(default_factory=list)
    raw_response_metadata: dict[str, Any] = Field(default_factory=dict)
