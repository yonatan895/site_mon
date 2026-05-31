"""Main poller: gathers data from APIs, evaluates rules, writes NDJSON to spool."""

import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional

import structlog
import uvicorn

from .endpoint_health import EndpointHealthChecker
from .evaluator import Evaluator
from .models import PollingEvent, PlatformRule, SourceEndpoint
from .rules_loader import RulesLoader
from .source_selector import SourceSelector
from .spool import SpoolManager
from .utils import ensure_dir, setup_logging

logger = setup_logging(__name__)


class Poller:
    """Orchestrates the polling cycle: source selection, data gathering,
    rule evaluation, and spool writing.
    """

    def __init__(
        self,
        platform: str,
        rules_dir: str = "/rules",
        spool_dir: str = "/spool",
    ) -> None:
        """Initialize the Poller.

        Args:
            platform: Platform identifier (hmc, ds8k, csm, ts7700).
            rules_dir: Base directory for rules configuration.
            spool_dir: Shared PVC spool directory.
        """
        self.platform = platform

        ensure_dir(rules_dir)
        ensure_dir(spool_dir)

        # Load configuration
        loader = RulesLoader(rules_dir)
        self.platform_rules, self.site_configs, self.policy = loader.load_full_config(
            platform
        )

        # Initialize health checker with all endpoints
        all_endpoints: list[SourceEndpoint] = []
        for site_config in self.site_configs.values():
            all_endpoints.extend(site_config.endpoints)

        self.health_checker = EndpointHealthChecker(all_endpoints)
        self.source_selector = SourceSelector(
            platform=platform,
            site_configs=self.site_configs,
            health_checker=self.health_checker,
            policy=self.policy,
        )
        self.spool_manager = SpoolManager(spool_dir)

        logger.info(
            "poller_initialized",
            platform=platform,
            sites=list(self.site_configs.keys()),
            data_types=list(self.platform_rules.keys()),
        )

    def run_once(self) -> int:
        """Execute a single polling cycle.

        Retrieves active endpoints, queries all configured data types in parallel,
        evaluates results, serializes to HEC NDJSON format, and writes to spool.

        Returns:
            Number of HEC event lines written.
        """
        cycle_start = time.monotonic()
        batch_id = str(uuid.uuid4())

        active_endpoints = self.source_selector.get_active_endpoints()
        if not active_endpoints:
            logger.warning("no_active_endpoints", platform=self.platform)
            return 0

        ndjson_lines: list[str] = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}

            for endpoint in active_endpoints:
                site_config = self.site_configs.get(endpoint.site)
                if not site_config:
                    logger.warning("no_site_config", endpoint=endpoint.name, site=endpoint.site)
                    continue

                data_types = site_config.data_types or list(self.platform_rules.keys())

                for data_type in data_types:
                    platform_rule = self.platform_rules.get(data_type)
                    if not platform_rule:
                        logger.debug(
                            "skipping_data_type",
                            data_type=data_type,
                            endpoint=endpoint.name,
                        )
                        continue

                    future = executor.submit(
                        self._query_and_evaluate,
                        endpoint,
                        data_type,
                        platform_rule,
                    )
                    futures[future] = (endpoint, data_type)

            for future in as_completed(futures):
                endpoint, data_type = futures[future]
                try:
                    result = future.result(timeout=endpoint.timeout + 30)
                    if result:
                        lines = self._events_to_hec_lines(
                            result, endpoint
                        )
                        ndjson_lines.extend(lines)
                except Exception:
                    logger.exception(
                        "query_failed",
                        endpoint=endpoint.name,
                        data_type=data_type,
                    )

        event_count = len(ndjson_lines)
        if ndjson_lines:
            ndjson_content = "\n".join(ndjson_lines) + "\n"
            try:
                self.spool_manager.write_ndjson(
                    ndjson_content, batch_id=batch_id
                )
                logger.info(
                    "cycle_complete",
                    batch_id=batch_id,
                    events_written=event_count,
                    duration_ms=(time.monotonic() - cycle_start) * 1000,
                )
            except Exception:
                logger.exception("spool_write_failed", batch_id=batch_id)

        elapsed_ms = (time.monotonic() - cycle_start) * 1000
        logger.info(
            "polling_cycle_finished",
            platform=self.platform,
            duration_ms=round(elapsed_ms, 2),
            events_written=event_count,
        )
        return event_count

    def _query_and_evaluate(
        self,
        endpoint: SourceEndpoint,
        data_type: str,
        platform_rule: PlatformRule,
    ) -> Optional[Any]:
        """Query an endpoint for a specific data type and evaluate results.

        Args:
            endpoint: The source endpoint to query.
            data_type: The data type to retrieve.
            platform_rule: The PlatformRule for this data type.

        Returns:
            PollingEvent or list of PollingEvent, or None on failure.
        """
        try:
            raw_data = self._query_endpoint(endpoint, data_type, platform_rule)
        except Exception:
            logger.exception(
                "endpoint_query_failed",
                endpoint=endpoint.name,
                data_type=data_type,
            )
            return None

        if raw_data is None:
            logger.warning(
                "empty_response",
                endpoint=endpoint.name,
                data_type=data_type,
            )
            return None

        try:
            site_config = self.site_configs.get(endpoint.site)
            if not site_config:
                logger.warning("no_site_config_for_evaluation", site=endpoint.site)
                return None

            evaluator = Evaluator(
                platform_rules={data_type: platform_rule},
                site_config=site_config,
            )
            result = evaluator.evaluate(data_type, raw_data, endpoint.name)
            return result
        except Exception:
            logger.exception(
                "evaluation_failed",
                endpoint=endpoint.name,
                data_type=data_type,
            )
            return None

    def _events_to_hec_lines(
        self,
        events: Any,
        endpoint: SourceEndpoint,
    ) -> list[str]:
        """Convert polling events to HEC NDJSON lines.

        Each line is a JSON object in Splunk HEC event format:
        {"time": ..., "host": ..., "source": ..., "sourcetype": ..., "index": ..., "event": {...}}

        Args:
            events: Single PollingEvent or list of PollingEvent.
            endpoint: Source endpoint for host/source metadata.

        Returns:
            List of HEC JSON strings (one per event).
        """
        if not events:
            return []

        if not isinstance(events, list):
            events = [events]

        lines: list[str] = []
        for event in events:
            if isinstance(event, PollingEvent):
                hec = self._polling_event_to_hec(event, endpoint)
            elif isinstance(event, dict):
                hec = self._dict_to_hec_line(event, endpoint)
            else:
                continue
            lines.append(json.dumps(hec, default=str, ensure_ascii=False))

        return lines

    def _polling_event_to_hec(
        self,
        event: PollingEvent,
        endpoint: SourceEndpoint,
    ) -> dict[str, Any]:
        """Convert a PollingEvent to a Splunk HEC event dict."""
        return {
            "time": event.timestamp.strftime("%s.%f"),
            "host": endpoint.name,
            "source": f"{event.platform}:{event.data_type}",
            "sourcetype": event.sourcetype,
            "index": event.index,
            "event": event.model_dump(mode="json"),
        }

    @staticmethod
    def _dict_to_hec_line(
        event_dict: dict[str, Any],
        endpoint: SourceEndpoint,
    ) -> dict[str, Any]:
        """Convert a raw dict to a Splunk HEC event dict."""
        return {
            "time": datetime.now(timezone.utc).strftime("%s.%f"),
            "host": endpoint.name,
            "source": f"{endpoint.platform}:data",
            "sourcetype": event_dict.get(
                "sourcetype", f"{endpoint.platform}:data"
            ),
            "index": event_dict.get("index", "mainframe_metrics"),
            "event": event_dict,
        }

    def _query_endpoint(
        self,
        endpoint: SourceEndpoint,
        data_type: str,
        platform_rule: PlatformRule,
    ) -> Any:
        """Query an API endpoint for the specified data type.

        Args:
            endpoint: SourceEndpoint configuration.
            data_type: Data type to query.
            platform_rule: PlatformRule for this data type.

        Returns:
            Raw response data as dict or list of dicts.
        """
        client = self._create_client(endpoint)
        return client.query(data_type, platform_rule)

    def _create_client(self, endpoint: SourceEndpoint) -> Any:
        """Factory method to create the appropriate API client for the platform.

        Args:
            endpoint: SourceEndpoint configuration.

        Returns:
            Platform-specific API client instance.

        Raises:
            ValueError: If the platform is unsupported.
        """
        platform_lower = endpoint.platform.lower()

        if platform_lower == "hmc":
            return HMCClient(endpoint)

        if platform_lower in ("ds", "ds8k", "ds8000"):
            return DS8000Client(endpoint)

        if platform_lower == "csm":
            return CSMClient(endpoint)

        if platform_lower == "ts7700":
            return TS7700Client(endpoint)

        raise ValueError(f"Unsupported platform: {endpoint.platform}")

    def run_forever(self, interval_seconds: int = 300) -> None:
        """Run polling continuously at the specified interval.

        Args:
            interval_seconds: Seconds between polling cycles.
        """
        logger.info(
            "poller_loop_started",
            platform=self.platform,
            interval_seconds=interval_seconds,
        )
        self.health_checker.start()

        try:
            while True:
                cycle_start = time.monotonic()
                try:
                    self.run_once()
                except Exception:
                    logger.exception(
                        "polling_cycle_error",
                        platform=self.platform,
                    )

                elapsed = time.monotonic() - cycle_start
                sleep_time = max(0, interval_seconds - elapsed)
                logger.debug("polling_sleep", seconds=sleep_time)
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("poller_interrupted")
        finally:
            self.health_checker.stop()


class BaseAPIClient:
    """Base class for platform-specific API clients."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        self.endpoint = endpoint
        self.logger = structlog.get_logger(__name__)

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query the API for a specific data type.

        Args:
            data_type: The data type to query.
            platform_rule: The PlatformRule configuration.

        Returns:
            Raw response data.

        Raises:
            NotImplementedError: Subclasses must implement this.
        """
        raise NotImplementedError


class HMCClient(BaseAPIClient):
    """Client for IBM Z HMC (Hardware Management Console) via zhmcclient."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._client = None

    def _connect(self) -> Any:
        """Establish connection to the HMC.

        Returns:
            zhmcclient session object.
        """
        if self._client is not None:
            return self._client

        import zhmcclient

        creds = self._load_creds()
        session = zhmcclient.Session(
            self.endpoint.url,
            creds["username"],
            creds["password"],
            verify_cert=False,
            session_id="site_mon_hmc",
        )
        self._client = session
        self.logger.info("hmc_connected", url=self.endpoint.url)
        return self._client

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query HMC for CPC stats, CPUs, LPARs, CHPIDs, networking, or channels.

        Args:
            data_type: One of "cpc-stats", "cpus", "lpars", "chpid", "chpids",
                       "networking", "channels".
            platform_rule: PlatformRule configuration.

        Returns:
            Structured dict or list of dicts with HMC data.
        """
        import zhmcclient

        session = self._connect()
        client = zhmcclient.Client(session)

        if data_type in ("cpc-stats", "cpus"):
            return self._query_cpcs(client)
        elif data_type == "lpars":
            return self._query_lpars(client)
        elif data_type in ("chpid", "chpids", "networking", "channels"):
            return self._query_chpids(client)
        else:
            self.logger.warning("unknown_hmc_data_type", data_type=data_type)
            return []

    def _query_cpcs(self, client: Any) -> list[dict[str, Any]]:
        """Query all CPCs from the HMC.

        Args:
            client: zhmcclient Client instance.

        Returns:
            List of CPC property dictionaries.
        """
        cpcs = client.cpcs.list()
        results = []
        for cpc in cpcs:
            cpc.pull_full_properties()
            results.append(dict(cpc.properties))
        self.logger.info("hmc_cpcs_queried", count=len(results))
        return results

    def _query_lpars(self, client: Any) -> list[dict[str, Any]]:
        """Query all LPARs across all CPCs.

        Args:
            client: zhmcclient Client instance.

        Returns:
            List of LPAR property dictionaries.
        """
        results = []
        for cpc in client.cpcs.list():
            try:
                lpars = cpc.lpars.list()
                for lpar in lpars:
                    lpar.pull_full_properties()
                    lpar_data = dict(lpar.properties)
                    lpar_data["cpc_name"] = cpc.properties.get("name", "")
                    results.append(lpar_data)
            except Exception as e:
                self.logger.warning(
                    "lpar_query_failed", cpc=cpc.properties.get("name", ""), error=str(e)
                )
        self.logger.info("hmc_lpars_queried", count=len(results))
        return results

    def _query_chpids(self, client: Any) -> list[dict[str, Any]]:
        """Query all CHPIDs across all CPCs.

        Args:
            client: zhmcclient Client instance.

        Returns:
            List of CHPID information dictionaries.
        """
        results = []
        for cpc in client.cpcs.list():
            try:
                adapters = cpc.adapters.list()
                for adapter in adapters:
                    adapter.pull_full_properties()
                    adapter_data = dict(adapter.properties)
                    adapter_data["cpc_name"] = cpc.properties.get("name", "")
                    results.append(adapter_data)
            except Exception as e:
                self.logger.warning(
                    "chpid_query_failed", cpc=cpc.properties.get("name", ""), error=str(e)
                )
        self.logger.info("hmc_chpids_queried", count=len(results))
        return results

    def _load_creds(self) -> dict[str, str]:
        """Load credentials from vault or config.

        Returns:
            Dictionary with username and password.
        """
        platform = self.endpoint.platform.upper()
        site = self.endpoint.site.upper()
        username_key = f"{platform}_{site}_USERNAME"
        password_key = f"{platform}_{site}_PASSWORD"

        return {
            "username": os.environ.get(username_key, "admin"),
            "password": os.environ.get(password_key, ""),
        }


class DS8000Client(BaseAPIClient):
    """Client for IBM DS8000 storage via pyds8k."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._connection = None

    def _connect(self) -> Any:
        """Establish connection to the DS8000.

        Returns:
            pyds8k connection object.
        """
        if self._connection is not None:
            return self._connection

        from pyds8k.client import DS8KClient

        creds = self._load_creds()
        conn = DS8KClient(
            self.endpoint.url,
            creds["username"],
            creds["password"],
        )
        self._connection = conn
        self.logger.info("ds8k_connected", url=self.endpoint.url)
        return self._connection

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query DS8000 for arrays, ports, ranks, or replication.

        Args:
            data_type: One of "arrays", "ports", "ranks", "replication".
            platform_rule: PlatformRule configuration.

        Returns:
            Structured dict or list of dicts.
        """
        conn = self._connect()

        if data_type == "arrays":
            return self._query_arrays(conn)
        elif data_type == "ports":
            return self._query_ports(conn)
        elif data_type == "ranks":
            return self._query_ranks(conn)
        elif data_type == "replication":
            return self._query_replication(conn)
        else:
            self.logger.warning("unknown_ds8k_data_type", data_type=data_type)
            return []

    def _query_arrays(self, conn: Any) -> list[dict[str, Any]]:
        """Query all storage arrays.

        Args:
            conn: pyds8k connection.

        Returns:
            List of array property dictionaries.
        """
        arrays = conn.get_systems()
        results = []
        for arr in arrays:
            results.append({
                "id": getattr(arr, "id", ""),
                "name": getattr(arr, "name", ""),
                "state": getattr(arr, "state", ""),
                "capacity": getattr(arr, "capacity", ""),
                "firmware_version": getattr(arr, "bundle_version", ""),
            })
        self.logger.info("ds8k_arrays_queried", count=len(results))
        return results

    def _query_ports(self, conn: Any) -> list[dict[str, Any]]:
        """Query all I/O ports.

        Args:
            conn: pyds8k connection.

        Returns:
            List of port information dictionaries.
        """
        results = []
        for system in conn.get_systems():
            try:
                ports = conn.get_ioports(system.id)
                for port in ports:
                    results.append({
                        "system_id": system.id,
                        "port_id": getattr(port, "id", ""),
                        "wwpn": getattr(port, "wwpn", ""),
                        "state": getattr(port, "state", ""),
                        "speed": getattr(port, "speed", ""),
                        "type": getattr(port, "type", ""),
                    })
            except Exception as e:
                self.logger.warning(
                    "ports_query_failed", system=getattr(system, "id", ""), error=str(e)
                )
        self.logger.info("ds8k_ports_queried", count=len(results))
        return results

    def _query_ranks(self, conn: Any) -> list[dict[str, Any]]:
        """Query all storage ranks.

        Args:
            conn: pyds8k connection.

        Returns:
            List of rank information dictionaries.
        """
        results = []
        for system in conn.get_systems():
            try:
                ranks = conn.get_ranks(system.id)
                for rank in ranks:
                    results.append({
                        "system_id": system.id,
                        "rank_id": getattr(rank, "id", ""),
                        "state": getattr(rank, "state", ""),
                        "capacity": getattr(rank, "capacity", ""),
                        "raid_type": getattr(rank, "raid_type", ""),
                    })
            except Exception as e:
                self.logger.warning(
                    "ranks_query_failed", system=getattr(system, "id", ""), error=str(e)
                )
        self.logger.info("ds8k_ranks_queried", count=len(results))
        return results

    def _query_replication(self, conn: Any) -> list[dict[str, Any]]:
        """Query replication status.

        Args:
            conn: pyds8k connection.

        Returns:
            List of replication status dictionaries.
        """
        results = []
        for system in conn.get_systems():
            try:
                pairs = conn.get_copy_services(system.id)
                for pair in pairs:
                    results.append({
                        "system_id": system.id,
                        "pair_id": getattr(pair, "id", ""),
                        "source_volume": getattr(pair, "source_volume", ""),
                        "target_volume": getattr(pair, "target_volume", ""),
                        "state": getattr(pair, "state", ""),
                        "type": getattr(pair, "type", ""),
                    })
            except Exception as e:
                self.logger.warning(
                    "replication_query_failed", system=getattr(system, "id", ""), error=str(e)
                )
        self.logger.info("ds8k_replication_queried", count=len(results))
        return results

    def _load_creds(self) -> dict[str, str]:
        """Load credentials from vault or config."""
        platform = self.endpoint.platform.upper()
        site = self.endpoint.site.upper()
        username_key = f"{platform}_{site}_USERNAME"
        password_key = f"{platform}_{site}_PASSWORD"
        return {
            "username": os.environ.get(username_key, "admin"),
            "password": os.environ.get(password_key, ""),
        }


class CSMClient(BaseAPIClient):
    """Client for IBM Copy Services Manager via pycsm."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._connection = None

    def _connect(self) -> Any:
        """Establish connection to CSM.

        Returns:
            pycsm session object.
        """
        if self._connection is not None:
            return self._connection

        from pycsm import CSMClient as _CSMClient

        creds = self._load_creds()
        conn = _CSMClient(
            self.endpoint.url,
            creds["username"],
            creds["password"],
        )
        self._connection = conn
        self.logger.info("csm_connected", url=self.endpoint.url)
        return self._connection

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query CSM for sessions, policies, or replication.

        Args:
            data_type: One of "sessions", "policies", "replication".
            platform_rule: PlatformRule configuration.

        Returns:
            Structured dict or list of dicts.
        """
        conn = self._connect()

        if data_type == "sessions":
            return self._query_sessions(conn)
        elif data_type == "policies":
            return self._query_policies(conn)
        elif data_type == "replication":
            return self._query_replication(conn)
        else:
            self.logger.warning("unknown_csm_data_type", data_type=data_type)
            return []

    def _query_sessions(self, conn: Any) -> list[dict[str, Any]]:
        """Query all CSM sessions.

        Args:
            conn: pycsm client.

        Returns:
            List of session dictionaries.
        """
        try:
            sessions = conn.get_sessions()
            results = [
                {
                    "session_id": getattr(s, "id", ""),
                    "name": getattr(s, "name", ""),
                    "state": getattr(s, "state", ""),
                    "role": getattr(s, "role", ""),
                    "type": getattr(s, "type", ""),
                }
                for s in sessions
            ]
            self.logger.info("csm_sessions_queried", count=len(results))
            return results
        except Exception as e:
            self.logger.error("csm_sessions_query_failed", error=str(e))
            return []

    def _query_policies(self, conn: Any) -> list[dict[str, Any]]:
        """Query all CSM policies.

        Args:
            conn: pycsm client.

        Returns:
            List of policy dictionaries.
        """
        try:
            policies = conn.get_policies()
            results = [
                {
                    "policy_id": getattr(p, "id", ""),
                    "name": getattr(p, "name", ""),
                    "type": getattr(p, "type", ""),
                    "is_active": getattr(p, "is_active", False),
                }
                for p in policies
            ]
            self.logger.info("csm_policies_queried", count=len(results))
            return results
        except Exception as e:
            self.logger.error("csm_policies_query_failed", error=str(e))
            return []

    def _query_replication(self, conn: Any) -> list[dict[str, Any]]:
        """Query CSM replication status.

        Args:
            conn: pycsm client.

        Returns:
            List of replication status dictionaries.
        """
        try:
            replication = conn.get_replication_status()
            self.logger.info("csm_replication_queried")
            return replication if isinstance(replication, list) else [replication]
        except Exception as e:
            self.logger.error("csm_replication_query_failed", error=str(e))
            return []

    def _load_creds(self) -> dict[str, str]:
        """Load credentials from vault or config."""
        platform = self.endpoint.platform.upper()
        site = self.endpoint.site.upper()
        username_key = f"{platform}_{site}_USERNAME"
        password_key = f"{platform}_{site}_PASSWORD"
        return {
            "username": os.environ.get(username_key, "admin"),
            "password": os.environ.get(password_key, ""),
        }


class TS7700Client(BaseAPIClient):
    """Client for IBM TS7700 tape virtualization via REST API."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._session = None

    def _get_session(self) -> Any:
        """Get or create a requests session with auth.

        Returns:
            requests.Session with auth configured.
        """
        import requests

        if self._session is not None:
            return self._session

        creds = self._load_creds()
        session = requests.Session()
        session.auth = (creds["username"], creds["password"])
        session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        session.verify = False
        self._session = session
        self.logger.info("ts7700_session_created", url=self.endpoint.url)
        return self._session

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query TS7700 for cluster info, cache, drives, or replication.

        Args:
            data_type: One of "cluster", "cache", "drives", "replication".
            platform_rule: PlatformRule configuration.

        Returns:
            Structured dict or list of dicts.
        """
        import requests

        session = self._get_session()
        timeout = self.endpoint.timeout

        endpoints_map = {
            "cluster": "/api/v1/cluster",
            "cache": "/api/v1/cache",
            "drives": "/api/v1/drives",
            "replication": "/api/v1/replication",
        }

        path = endpoints_map.get(data_type)
        if not path:
            self.logger.warning("unknown_ts7700_data_type", data_type=data_type)
            return []

        url = f"{self.endpoint.url.rstrip('/')}{path}"

        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            self.logger.info(
                "ts7700_queried", data_type=data_type, url=url
            )
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(
                "ts7700_query_failed", data_type=data_type, url=url, error=str(e)
            )
            return []

    def _load_creds(self) -> dict[str, str]:
        """Load credentials from vault or config."""
        platform = self.endpoint.platform.upper()
        site = self.endpoint.site.upper()
        username_key = f"{platform}_{site}_USERNAME"
        password_key = f"{platform}_{site}_PASSWORD"
        return {
            "username": os.environ.get(username_key, "admin"),
            "password": os.environ.get(password_key, ""),
        }


def main() -> None:
    """Entry point for the poller container."""

    platform = os.environ.get("PLATFORM", "hmc")
    rules_dir = os.environ.get("RULES_DIR", "/rules")
    spool_dir = os.environ.get("SPOOL_DIR", "/spool")
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))

    poller = Poller(platform=platform, rules_dir=rules_dir, spool_dir=spool_dir)

    from .health import app as health_app, init_health
    init_health(
        health_checker_instance=poller.health_checker,
        spool_manager_instance=poller.spool_manager,
    )

    port = int(os.environ.get("HEALTH_PORT", "8080"))
    server_thread = threading.Thread(
        target=uvicorn.run,
        args=(health_app,),
        kwargs={"host": "0.0.0.0", "port": port, "log_level": "warning"},
        daemon=True,
    )
    server_thread.start()

    poller.run_forever(interval_seconds=interval)


if __name__ == "__main__":
    main()
