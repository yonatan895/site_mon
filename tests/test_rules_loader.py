import shutil
from pathlib import Path

import pytest

from src.models import PlatformRule
from src.rules_loader import RulesLoader


@pytest.fixture
def rules_dir_with_full_setup(tmp_rules_dir: str, fixtures_dir: Path) -> str:
    shutil.copytree(
        str(fixtures_dir / "rules"),
        tmp_rules_dir,
        dirs_exist_ok=True,
    )
    return tmp_rules_dir


class TestLoadPlatformRules:
    def test_loads_rules_for_hmc(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules = loader.load_platform_rules("hmc")
        assert "cpc-stats" in rules
        assert "lpars" in rules
        assert isinstance(rules["cpc-stats"], PlatformRule)

    def test_common_fields_merged(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules = loader.load_platform_rules("hmc")
        rule = rules["cpc-stats"]
        assert rule.common_fields.get("platform") == "hmc"

    def test_extractions_parsed(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules = loader.load_platform_rules("hmc")
        rule = rules["cpc-stats"]
        assert len(rule.extractions) == 3
        assert rule.extractions[0].field_name == "cpc_name"

    def test_thresholds_parsed(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules = loader.load_platform_rules("hmc")
        rule = rules["cpc-stats"]
        assert len(rule.thresholds) == 2
        assert rule.thresholds[0].field == "cpu_usage_pct"

    def test_skips_source_policy(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules = loader.load_platform_rules("hmc")
        assert "source-policy" not in rules

    def test_skips_common(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules = loader.load_platform_rules("hmc")
        assert "common" not in rules

    def test_missing_directory_returns_empty(self, tmp_rules_dir: str) -> None:
        loader = RulesLoader(rules_dir=tmp_rules_dir)
        rules = loader.load_platform_rules("nonexistent")
        assert rules == {}


class TestLoadSiteConfigs:
    def test_loads_sites_for_hmc(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        configs = loader.load_site_configs("hmc")
        assert "primary" in configs
        assert "backup" in configs

    def test_endpoints_parsed(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        configs = loader.load_site_configs("hmc")
        primary = configs["primary"]
        assert len(primary.endpoints) == 1
        assert primary.endpoints[0].name == "hmc-primary"

    def test_is_primary_flag(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        configs = loader.load_site_configs("hmc")
        assert configs["primary"].is_primary is True
        assert configs["backup"].is_primary is False

    def test_data_types(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        configs = loader.load_site_configs("hmc")
        primary = configs["primary"]
        assert "cpc-stats" in primary.data_types
        assert "lpars" in primary.data_types

    def test_missing_directory_returns_empty(self, tmp_rules_dir: str) -> None:
        loader = RulesLoader(rules_dir=tmp_rules_dir)
        configs = loader.load_site_configs("nonexistent")
        assert configs == {}


class TestLoadSourcePolicy:
    def test_loads_policy(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        policy = loader.load_source_policy("hmc")
        assert policy["failover_mode"] == "primary_backup"
        assert policy["health_check"]["path"] == "/api/console/version"

    def test_missing_policy_returns_empty(self, tmp_rules_dir: str) -> None:
        loader = RulesLoader(rules_dir=tmp_rules_dir)
        policy = loader.load_source_policy("hmc")
        assert policy == {}


class TestLoadFullConfig:
    def test_loads_all_three(self, rules_dir_with_full_setup: str) -> None:
        loader = RulesLoader(rules_dir=rules_dir_with_full_setup)
        rules, sites, policy = loader.load_full_config("hmc")
        assert isinstance(rules, dict)
        assert isinstance(sites, dict)
        assert isinstance(policy, dict)
        assert len(rules) >= 2
        assert len(sites) == 2


class TestMissingFile:
    def test_returns_empty_dict(self, tmp_rules_dir: str) -> None:
        loader = RulesLoader(rules_dir=tmp_rules_dir)
        result = loader._load_yaml(Path(tmp_rules_dir) / "nonexistent.yaml")
        assert result == {}
