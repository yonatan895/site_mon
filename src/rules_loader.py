"""Rules and configuration loader from YAML files."""

from pathlib import Path
from typing import Any

import yaml

from .models import (
    FieldExtraction,
    PlatformRule,
    SiteConfig,
    SourceEndpoint,
    ThresholdRule,
)
from .utils import setup_logging

logger = setup_logging(__name__)


class RulesLoader:
    """Loads platform rules, site configs, and source policies from YAML files."""

    def __init__(self, rules_dir: str = "/rules") -> None:
        """Initialize the rules loader.

        Args:
            rules_dir: Base directory containing rules and configuration YAML files.
        """
        self.rules_dir = Path(rules_dir)
        logger.info("rules_loader_initialized", rules_dir=str(self.rules_dir))

    def _load_yaml(self, filepath: Path) -> dict[str, Any]:
        """Load and parse a single YAML file.

        Args:
            filepath: Path to the YAML file.

        Returns:
            Parsed YAML content as a dictionary. Returns empty dict if file missing.
        """
        if not filepath.exists():
            logger.warning("yaml_file_not_found", path=str(filepath))
            return {}
        with open(filepath, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        logger.debug("yaml_loaded", path=str(filepath))
        return data

    def load_platform_rules(self, platform: str) -> dict[str, PlatformRule]:
        """Load all data type rule YAML files for a given platform.

        Loads every YAML file from /{rules_dir}/platforms/{platform}/
        except source-policy.yaml. Merges common.yaml fields into each
        individual data type rule.

        Args:
            platform: Platform identifier (e.g. "hmc", "ds8k", "csm", "ts7700").

        Returns:
            Dictionary mapping data_type -> PlatformRule.
        """
        rules_path = self.rules_dir / "platforms" / platform
        if not rules_path.exists():
            logger.warning("platform_rules_dir_not_found", path=str(rules_path))
            return {}

        # Load common fields
        common_yaml = rules_path / "common.yaml"
        common_data = self._load_yaml(common_yaml)
        common_fields = common_data.get("common_fields", {})

        rules: dict[str, PlatformRule] = {}
        for yaml_file in sorted(rules_path.glob("*.yaml")):
            if yaml_file.name in ("source-policy.yaml", "common.yaml"):
                continue

            rule_data = self._load_yaml(yaml_file)
            if not rule_data:
                continue

            # Merge common fields into rule (rule-specific fields take precedence)
            merged_common = {**common_fields, **rule_data.get("common_fields", {})}

            # Parse extractions
            extractions = [FieldExtraction(**ex) for ex in rule_data.get("extractions", [])]

            # Parse thresholds
            thresholds = [ThresholdRule(**th) for th in rule_data.get("thresholds", [])]

            rule = PlatformRule(
                name=rule_data.get("name", yaml_file.stem),
                data_type=rule_data.get("data_type", yaml_file.stem),
                sourcetype=rule_data.get("sourcetype", f"{platform}:{yaml_file.stem}"),
                index=rule_data.get("index", "mainframe_mon"),
                interval_seconds=rule_data.get("interval_seconds", 300),
                extractions=extractions,
                thresholds=thresholds,
                common_fields=merged_common,
            )
            rules[rule.data_type] = rule
            logger.debug("platform_rule_loaded", data_type=rule.data_type)

        logger.info("platform_rules_loaded", platform=platform, count=len(rules))
        return rules

    def load_site_configs(self, platform: str) -> dict[str, SiteConfig]:
        """Load all site configuration YAML files for a given platform.

        Args:
            platform: Platform identifier.

        Returns:
            Dictionary mapping site_name -> SiteConfig.
        """
        sites_path = self.rules_dir / "sites" / platform
        if not sites_path.exists():
            logger.warning("sites_dir_not_found", path=str(sites_path))
            return {}

        site_configs: dict[str, SiteConfig] = {}
        for yaml_file in sorted(sites_path.glob("*.yaml")):
            data = self._load_yaml(yaml_file)
            if not data:
                continue

            endpoints = [SourceEndpoint(**ep) for ep in data.get("endpoints", [])]

            config = SiteConfig(
                site_name=data.get("site_name", yaml_file.stem),
                platform=data.get("platform", platform),
                endpoints=endpoints,
                data_types=data.get("data_types", []),
                is_primary=data.get("is_primary", True),
            )
            site_configs[config.site_name] = config
            logger.debug("site_config_loaded", site=config.site_name)

        logger.info("site_configs_loaded", platform=platform, count=len(site_configs))
        return site_configs

    def load_source_policy(self, platform: str) -> dict[str, Any]:
        """Load the source selection policy for a platform.

        Args:
            platform: Platform identifier.

        Returns:
            Policy dictionary with source selection rules.
        """
        policy_path = self.rules_dir / "platforms" / platform / "source-policy.yaml"
        policy = self._load_yaml(policy_path)
        logger.info("source_policy_loaded", platform=platform)
        return policy

    def load_full_config(
        self, platform: str
    ) -> tuple[dict[str, PlatformRule], dict[str, SiteConfig], dict[str, Any]]:
        """Load all configuration for a platform.

        Args:
            platform: Platform identifier.

        Returns:
            Tuple of (platform_rules dict, site_configs dict, policy dict).
        """
        platform_rules = self.load_platform_rules(platform)
        site_configs = self.load_site_configs(platform)
        policy = self.load_source_policy(platform)
        logger.info("full_config_loaded", platform=platform)
        return platform_rules, site_configs, policy
