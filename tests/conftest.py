import json
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from src.models import (
    FieldExtraction,
    PlatformRule,
    SiteConfig,
    SourceEndpoint,
    ThresholdRule,
)
from src.rules_loader import RulesLoader
from src.spool import SpoolManager


@pytest.fixture
def tmp_spool_dir() -> str:
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def tmp_rules_dir() -> str:
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def sample_source_endpoint() -> SourceEndpoint:
    return SourceEndpoint(
        name="test-primary",
        url="https://test-hmc.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/mainframe/hmc/primary",
        rate_limit_rps=5.0,
        timeout=30,
        role="primary",
    )


@pytest.fixture
def sample_backup_endpoint() -> SourceEndpoint:
    return SourceEndpoint(
        name="test-backup",
        url="https://test-hmc-backup.example.com",
        platform="hmc",
        site="primary",
        auth_type="basic",
        creds_vault_path="secret/mainframe/hmc/backup",
        rate_limit_rps=5.0,
        timeout=30,
        role="backup",
    )


@pytest.fixture
def sample_site_config(sample_source_endpoint: SourceEndpoint) -> SiteConfig:
    return SiteConfig(
        site_name="test-primary",
        platform="hmc",
        endpoints=[sample_source_endpoint],
        data_types=["cpc-stats", "lpars"],
        is_primary=True,
    )


@pytest.fixture
def sample_site_config_with_backup(
    sample_source_endpoint: SourceEndpoint,
    sample_backup_endpoint: SourceEndpoint,
) -> SiteConfig:
    return SiteConfig(
        site_name="test-primary",
        platform="hmc",
        endpoints=[sample_source_endpoint, sample_backup_endpoint],
        data_types=["cpc-stats", "lpars"],
        is_primary=True,
    )


@pytest.fixture
def sample_extractions() -> list[FieldExtraction]:
    return [
        FieldExtraction(field_name="cpc_name", json_path="cpc_name"),
        FieldExtraction(field_name="status", json_path="status"),
        FieldExtraction(field_name="cpu_usage_pct", json_path="cpu_usage_pct"),
        FieldExtraction(
            field_name="memory_usage_pct",
            json_path="memory_usage_pct",
            default=0.0,
        ),
        FieldExtraction(
            field_name="energy_watts",
            json_path="energy_consumption_watts",
            transform="int",
        ),
    ]


@pytest.fixture
def sample_thresholds() -> list[ThresholdRule]:
    return [
        ThresholdRule(
            field="status",
            operator="ne",
            value="operating",
            severity="critical",
            message_template="CPC {cpc_name} is not operating (status: {status})",
        ),
        ThresholdRule(
            field="cpu_usage_pct",
            operator="gt",
            value=90,
            severity="warning",
            message_template="CPC {cpc_name} CPU usage is {cpu_usage_pct}%",
        ),
        ThresholdRule(
            field="energy_watts",
            operator="gte",
            value=5000,
            severity="info",
            message_template="CPC {cpc_name} energy at {energy_watts}W",
        ),
    ]


@pytest.fixture
def sample_platform_rule(
    sample_extractions: list[FieldExtraction],
    sample_thresholds: list[ThresholdRule],
) -> PlatformRule:
    return PlatformRule(
        name="cpc-stats",
        data_type="cpc-stats",
        sourcetype="hmc:cpc_stats",
        index="mainframe_metrics",
        interval_seconds=300,
        extractions=sample_extractions,
        thresholds=sample_thresholds,
        common_fields={"platform": "hmc"},
    )


@pytest.fixture
def sample_raw_response_item() -> dict[str, Any]:
    return {
        "cpc_name": "CPCA",
        "status": "operating",
        "se_version": "2.16.0",
        "hmc_version": "2.16.0",
        "cpu_usage_pct": 45.2,
        "memory_usage_pct": 62.3,
        "energy_consumption_watts": "4200",
    }


@pytest.fixture
def sample_raw_response_alerting_item() -> dict[str, Any]:
    return {
        "cpc_name": "CPCB",
        "status": "degraded",
        "se_version": "2.15.0",
        "hmc_version": "2.15.0",
        "cpu_usage_pct": 95.1,
        "memory_usage_pct": 88.0,
        "energy_consumption_watts": "6200",
    }


@pytest.fixture
def spool_manager(tmp_spool_dir: str) -> SpoolManager:
    return SpoolManager(spool_dir=tmp_spool_dir, max_spool_size_mb=10)


@pytest.fixture
def sample_ndjson_content() -> str:
    return (
        '{"time": "1717171200.000000", "host": "test-hmc", '
        '"source": "hmc:cpc-stats", "sourcetype": "hmc:cpc_stats", '
        '"index": "mainframe_metrics", "event": {"cpc_name": "CPCA"}}\n'
        '{"time": "1717171200.000000", "host": "test-hmc", '
        '"source": "hmc:cpu", "sourcetype": "hmc:cpu_stats", '
        '"index": "mainframe_metrics", "event": {"cpu_index": "0"}}\n'
    )


@pytest.fixture
def sample_hec_events() -> list[dict[str, Any]]:
    return [
        {
            "time": "1717171200.000000",
            "host": "test-hmc",
            "source": "hmc:cpc-stats",
            "sourcetype": "hmc:cpc_stats",
            "index": "mainframe_metrics",
            "event": {"cpc_name": "CPCA", "status": "operating", "cpu_usage_pct": 45.2},
        },
        {
            "time": "1717171200.000000",
            "host": "test-hmc",
            "source": "hmc:cpu",
            "sourcetype": "hmc:cpu_stats",
            "index": "mainframe_metrics",
            "event": {"cpu_index": "0", "cpu_usage_pct": 20.0},
        },
    ]


@pytest.fixture
def hec_ndjson(sample_hec_events: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(e) for e in sample_hec_events) + "\n"


@pytest.fixture
def rules_loader(tmp_rules_dir: str) -> RulesLoader:
    return RulesLoader(rules_dir=tmp_rules_dir)


@pytest.fixture(autouse=True)
def prometheus_cleanup() -> Any:
    try:
        from prometheus_client import REGISTRY

        list(REGISTRY._collector_to_names.keys())  # noqa: F841
    except ImportError:
        yield
        return

    yield

    try:
        from prometheus_client import REGISTRY

        current = set(REGISTRY._collector_to_names.keys())
        for c in current:
            REGISTRY.unregister(c)
    except (ImportError, KeyError):
        pass


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


def _write_yaml_file(dir_path: str, filename: str, content: str) -> str:
    path = os.path.join(dir_path, filename)
    with open(path, "w") as f:
        f.write(content)
    return path
