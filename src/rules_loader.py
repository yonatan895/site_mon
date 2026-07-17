"""Loads and validates YAML rule files from the rules directory."""

import os
from pathlib import Path
from typing import Any

import structlog
import yaml

from .models import (
    FieldExtraction,
    PlatformRule,
    SelectionPolicy,
    SiteConfig,
    SourceEndpoint,
    ThresholdRule,
)
from .utils import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


class RulesLoader:
    """Discovers and parses YAML rule files under a given directory.

    Expected layout::

        rules/
          platform_rules.yaml   # PlatformRule definitions
          sites/
            site_a.yaml         # SiteConfig + SourceEndpoint definitions
            site_b.yaml
          policy.yaml           # SelectionPolicy (optional)
    """

    def __init__(self, rules_dir: str) -> None:
        self.rules_dir = Path(rules_dir)
        logger.info("rules_loader_initialized", rules_dir=str(self.rules_dir))

    def load_full_config(
        self, platform: str
    ) -> tuple[dict[str, PlatformRule], dict[str, SiteConfig], SelectionPolicy]:
        """Load and return the full configuration for *platform*.

        Returns:
            (platform_rules, site_configs, policy)
        """
        platform_rules = self.load_platform_rules(platform)
        site_configs = self.load_site_configs(platform)
        policy = self.load_policy()
        return platform_rules, site_configs, policy

    # ------------------------------------------------------------------
    # Platform rules
    # ------------------------------------------------------------------

    def load_platform_rules(
        self, platform: str
    ) -> dict[str, PlatformRule]:
        """Load PlatformRule objects for *platform* from platform_rules.yaml."""
        rules_file = self.rules_dir / "platform_rules.yaml"
        if not rules_file.exists():
            logger.warning("platform_rules_file_not_found", path=str(rules_file))
            return {}

        raw = self._load_yaml(rules_file)
        platform_data = raw.get(platform, {})
        if not platform_data:
            logger.warning(
                "no_rules_for_platform",
                platform=platform,
                available=list(raw.keys()),
            )
            return {}

        rules: dict[str, PlatformRule] = {}
        for data_type, rule_data in platform_data.items():
            try:
                rules[data_type] = self._parse_platform_rule(data_type, rule_data)
            except Exception as e:
                logger.error(
                    "rule_parse_error",
                    platform=platform,
                    data_type=data_type,
                    error=str(e),
                )

        logger.info(
            "platform_rules_loaded",
            platform=platform,
            data_types=list(rules.keys()),
        )
        return rules

    def _parse_platform_rule(
        self, data_type: str, raw: dict[str, Any]
    ) -> PlatformRule:
        extractions = [
            FieldExtraction(
                field_name=e["field_name"],
                json_path=e["json_path"],
                default=e.get("default"),
                transform=e.get("transform"),
            )
            for e in raw.get("extractions", [])
        ]
        thresholds = [
            ThresholdRule(
                field=t["field"],
                operator=t["operator"],
                value=t["value"],
                severity=t.get("severity", "warning"),
                message_template=t.get(
                    "message_template",
                    "Field {field} value {value} breached threshold {threshold}",
                ),
            )
            for t in raw.get("thresholds", [])
        ]
        return PlatformRule(
            name=raw.get("name", data_type),
            sourcetype=raw.get("sourcetype", f"{data_type}:generic"),
            index=raw.get("index", "mainframe_metrics"),
            extractions=extractions,
            thresholds=thresholds,
            common_fields=raw.get("common_fields", {}),
        )

    # ------------------------------------------------------------------
    # Site configs
    # ------------------------------------------------------------------

    def load_site_configs(self, platform: str) -> dict[str, SiteConfig]:
        """Load SiteConfig objects from the sites/ sub-directory."""
        sites_dir = self.rules_dir / "sites"
        if not sites_dir.exists():
            logger.warning("sites_dir_not_found", path=str(sites_dir))
            return {}

        site_configs: dict[str, SiteConfig] = {}
        for yaml_file in sites_dir.glob("*.yaml"):
            try:
                raw = self._load_yaml(yaml_file)
                if raw.get("platform", "").lower() != platform.lower():
                    continue
                sc = self._parse_site_config(raw)
                site_configs[sc.site_name] = sc
            except Exception as e:
                logger.error(
                    "site_config_parse_error", file=str(yaml_file), error=str(e)
                )

        logger.info(
            "site_configs_loaded",
            platform=platform,
            sites=list(site_configs.keys()),
        )
        return site_configs

    def _parse_site_config(self, raw: dict[str, Any]) -> SiteConfig:
        endpoints = [
            SourceEndpoint(
                name=ep["name"],
                url=ep["url"],
                platform=raw["platform"],
                site=raw["site_name"],
                timeout=ep.get("timeout", 30),
                health_url=ep.get("health_url"),
                metadata=ep.get("metadata", {}),
            )
            for ep in raw.get("endpoints", [])
        ]
        return SiteConfig(
            site_name=raw["site_name"],
            platform=raw["platform"],
            endpoints=endpoints,
            data_types=raw.get("data_types", []),
            metadata=raw.get("metadata", {}),
        )

    # ------------------------------------------------------------------
    # Selection policy
    # ------------------------------------------------------------------

    def load_policy(self) -> SelectionPolicy:
        """Load the optional SelectionPolicy from policy.yaml."""
        policy_file = self.rules_dir / "policy.yaml"
        if not policy_file.exists():
            return SelectionPolicy()
        try:
            raw = self._load_yaml(policy_file)
            policy = SelectionPolicy(**raw.get("policy", {}))
            logger.info("selection_policy_loaded", strategy=policy.strategy)
            return policy
        except Exception as e:
            logger.warning("policy_load_failed", error=str(e))
            return SelectionPolicy()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        env_vars_expanded = os.path.expandvars(path.read_text(encoding="utf-8"))
        data = yaml.safe_load(env_vars_expanded)
        return data if isinstance(data, dict) else {}
