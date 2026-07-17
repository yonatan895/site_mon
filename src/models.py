"""Pydantic data models shared across the site_mon package."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SourceEndpoint(BaseModel):
    """Represents a single monitored API endpoint."""

    name: str
    url: str
    platform: str
    site: str
    timeout: int = 30
    health_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SiteConfig(BaseModel):
    """Per-site configuration block (one site = one physical location)."""

    site_name: str
    platform: str
    endpoints: list[SourceEndpoint] = Field(default_factory=list)
    data_types: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class FieldExtraction(BaseModel):
    """Describes how to pluck one field from a raw API response."""

    field_name: str
    json_path: str
    default: Any = None
    transform: str | None = None  # e.g. "int", "float", "str", "bool"


class ThresholdRule(BaseModel):
    """A single alerting threshold applied to an extracted field."""

    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, contains, regex
    value: Any
    severity: str = "warning"
    message_template: str = "Field {field} value {value} breached threshold {threshold}"


class PlatformRule(BaseModel):
    """Full rule set for one data_type on one platform."""

    name: str
    sourcetype: str
    index: str
    extractions: list[FieldExtraction] = Field(default_factory=list)
    thresholds: list[ThresholdRule] = Field(default_factory=list)
    common_fields: dict[str, Any] = Field(default_factory=dict)


class SelectionPolicy(BaseModel):
    """Controls how the source selector picks between redundant endpoints."""

    strategy: str = "round_robin"  # round_robin | primary_only | weighted
    weights: dict[str, float] = Field(default_factory=dict)
    sticky: bool = False


class PollingEvent(BaseModel):
    """A fully evaluated event ready for HEC delivery."""

    platform: str
    site: str
    data_type: str
    sourcetype: str
    index: str
    timestamp: datetime
    fields: dict[str, Any] = Field(default_factory=dict)
    alerts: list[dict[str, Any]] = Field(default_factory=list)
    raw_response_metadata: dict[str, Any] = Field(default_factory=dict)
