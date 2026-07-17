"""Main poller: gathers data from APIs, evaluates rules, writes NDJSON to spool."""

import json
import os
import signal
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

import structlog
import uvicorn

from .endpoint_health import EndpointHealthChecker
from .evaluator import Evaluator
from .models import PlatformRule, PollingEvent, SourceEndpoint
from .rules_loader import RulesLoader
from .source_selector import SourceSelector
from .spool import SpoolManager
from .utils import configure_logging, ensure_dir

configure_logging()
logger = structlog.get_logger(__name__)


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

        loader = RulesLoader(rules_dir)
        self.platform_rules, self.site_configs, self.policy = loader.load_full_config(platform)

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
        self.evaluator = Evaluator(platform_rules=self.platform_rules)
        self._clients: dict[str, Any] = {}

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

        max_workers = int(os.environ.get("POLL_MAX_WORKERS", "5"))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                        lines = self._events_to_hec_lines(result, endpoint)
                        ndjson_lines.extend(lines)
                except Exception:
                    logger.exception(
                        "query_failed",
                        endpoint=endpoint.name,
                        data_type=data_type,
                    )
                    try:
                        from .health import api_query_errors

                        api_query_errors.inc()
                    except ImportError:
                        pass

        event_count = len(ndjson_lines)
        if ndjson_lines:
            ndjson_content = "\n".join(ndjson_lines) + "\n"
            try:
                self.spool_manager.write_ndjson(ndjson_content, batch_id=batch_id)
                logger.info(
                    "cycle_complete",
                    batch_id=batch_id,
                    events_written=event_count,
                    duration_ms=(time.monotonic() - cycle_start) * 1000,
                )
            except Exception:
                logger.exception("spool_write_failed", batch_id=batch_id)

        elapsed_ms = (time.monotonic() - cycle_start) * 1000
        try:
            from .health import polling_cycle_duration

            polling_cycle_duration.observe(elapsed_ms / 1000)
        except ImportError:
            pass
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
    ) -> Any | None:
        """Query an endpoint for a specific data type and evaluate results."""
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

            result = self.evaluator.evaluate(data_type, raw_data, endpoint.name, site_config)
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
        """Convert polling events to HEC NDJSON lines."""
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
            "time": str(event.timestamp.timestamp()),
            "host": endpoint.name,
            "source": f"{event.platform}:{event.data_type}",
            "sourcetype": event.sourcetype,
            "index": event.index,
            "event": event.model_dump(mode="json", exclude={"timestamp"}),
        }

    @staticmethod
    def _dict_to_hec_line(
        event_dict: dict[str, Any],
        endpoint: SourceEndpoint,
    ) -> dict[str, Any]:
        """Convert a raw dict to a Splunk HEC event dict."""
        return {
            "time": str(datetime.now(UTC).timestamp()),
            "host": endpoint.name,
            "source": f"{endpoint.platform}:data",
            "sourcetype": event_dict.get("sourcetype", f"{endpoint.platform}:data"),
            "index": event_dict.get("index", "mainframe_metrics"),
            "event": event_dict,
        }

    def _query_endpoint(
        self,
        endpoint: SourceEndpoint,
        data_type: str,
        platform_rule: PlatformRule,
    ) -> Any:
        """Query an API endpoint for the specified data type."""
        if endpoint.name not in self._clients:
            self._clients[endpoint.name] = self._create_client(endpoint)
        return self._clients[endpoint.name].query(data_type, platform_rule)

    def _create_client(self, endpoint: SourceEndpoint) -> Any:
        """Factory method to create the appropriate API client for the platform.

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
        """Run polling continuously at the specified interval."""
        logger.info(
            "poller_loop_started",
            platform=self.platform,
            interval_seconds=interval_seconds,
        )
        self.health_checker.start()

        stop_event = threading.Event()

        def _handle_shutdown(signum: int, frame: Any) -> None:
            logger.info("poller_shutdown_signal", signal=signum)
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_shutdown)

        try:
            while not stop_event.is_set():
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
                stop_event.wait(timeout=sleep_time)
        except KeyboardInterrupt:
            logger.info("poller_interrupted")
        finally:
            self.health_checker.stop()


class BaseAPIClient:
    """Base class for platform-specific API clients."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        self.endpoint = endpoint
        self.logger = structlog.get_logger(__name__)
        # FIX #1: Per-client lock to guard lazy _connect() against race conditions
        self._connect_lock = threading.Lock()

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query the API for a specific data type.

        Raises:
            NotImplementedError: Subclasses must implement this.
        """
        raise NotImplementedError

    def _load_creds(self) -> dict[str, str]:
        """Load credentials from environment variables.

        Raises:
            RuntimeError: If password environment variable is empty.
        """
        platform = self.endpoint.platform.upper()
        site = self.endpoint.site.upper()
        username_key = f"{platform}_{site}_USERNAME"
        password_key = f"{platform}_{site}_PASSWORD"

        username = os.environ.get(username_key, "admin")
        password = os.environ.get(password_key, "")
        if not password:
            raise RuntimeError(
                f"Missing required credential: {password_key} for endpoint {self.endpoint.name}"
            )
        return {"username": username, "password": password}


class HMCClient(BaseAPIClient):
    """Client for IBM Z HMC (Hardware Management Console) via zhmcclient."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._client = None

    def _connect(self) -> Any:
        """Establish connection to the HMC. Thread-safe via _connect_lock.

        Returns:
            zhmcclient.Client instance.
        """
        # FIX #1: double-checked locking pattern
        if self._client is not None:
            return self._client
        with self._connect_lock:
            if self._client is not None:
                return self._client
            import zhmcclient

            creds = self._load_creds()
            verify_ssl = os.environ.get("VERIFY_SSL", "true").lower() == "true"
            session = zhmcclient.Session(
                self.endpoint.url,
                creds["username"],
                creds["password"],
                verify_cert=verify_ssl,
                session_id=f"site_mon_hmc_{uuid.uuid4().hex[:8]}",
            )
            self._client = session
            self.logger.info("hmc_connected", url=self.endpoint.url)
        return self._client

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query HMC for CPC stats, CPUs, LPARs, CHPIDs, networking, or channels."""
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
        cpcs = client.cpcs.list()
        results = []
        for cpc in cpcs:
            cpc.pull_full_properties()
            results.append(dict(cpc.properties))
        self.logger.info("hmc_cpcs_queried", count=len(results))
        return results

    def _query_lpars(self, client: Any) -> list[dict[str, Any]]:
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


class DS8000Client(BaseAPIClient):
    """Client for IBM DS8000 storage via pyds8k."""

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._connection = None

    def _connect(self) -> Any:
        """Establish connection to the DS8000. Thread-safe via _connect_lock.

        Returns:
            pyds8k D8KClient instance.
        """
        # FIX #1 + FIX #2: thread-safe connect with corrected pyds8k import path.
        # pyds8k public API is at pyds8k.client.ds8k.client.D8KClient, not
        # pyds8k.client.DS8KClient which does not exist.
        if self._connection is not None:
            return self._connection
        with self._connect_lock:
            if self._connection is not None:
                return self._connection
            from pyds8k.client.ds8k.client import D8KClient

            creds = self._load_creds()
            conn = D8KClient(
                self.endpoint.url,
                creds["username"],
                creds["password"],
            )
            self._connection = conn
            self.logger.info("ds8k_connected", url=self.endpoint.url)
        return self._connection

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query DS8000 for arrays, ports, ranks, or replication."""
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
        arrays = conn.get_systems()
        results = []
        for arr in arrays:
            results.append(
                {
                    "id": getattr(arr, "id", ""),
                    "name": getattr(arr, "name", ""),
                    "state": getattr(arr, "state", ""),
                    "capacity": getattr(arr, "capacity", ""),
                    "firmware_version": getattr(arr, "bundle_version", ""),
                }
            )
        self.logger.info("ds8k_arrays_queried", count=len(results))
        return results

    def _query_ports(self, conn: Any) -> list[dict[str, Any]]:
        results = []
        for system in conn.get_systems():
            try:
                ports = conn.get_ioports(system.id)
                for port in ports:
                    results.append(
                        {
                            "system_id": system.id,
                            "port_id": getattr(port, "id", ""),
                            "wwpn": getattr(port, "wwpn", ""),
                            "state": getattr(port, "state", ""),
                            "speed": getattr(port, "speed", ""),
                            "type": getattr(port, "type", ""),
                        }
                    )
            except Exception as e:
                self.logger.warning(
                    "ports_query_failed", system=getattr(system, "id", ""), error=str(e)
                )
        self.logger.info("ds8k_ports_queried", count=len(results))
        return results

    def _query_ranks(self, conn: Any) -> list[dict[str, Any]]:
        results = []
        for system in conn.get_systems():
            try:
                ranks = conn.get_ranks(system.id)
                for rank in ranks:
                    results.append(
                        {
                            "system_id": system.id,
                            "rank_id": getattr(rank, "id", ""),
                            "state": getattr(rank, "state", ""),
                            "capacity": getattr(rank, "capacity", ""),
                            "raid_type": getattr(rank, "raid_type", ""),
                        }
                    )
            except Exception as e:
                self.logger.warning(
                    "ranks_query_failed", system=getattr(system, "id", ""), error=str(e)
                )
        self.logger.info("ds8k_ranks_queried", count=len(results))
        return results

    def _query_replication(self, conn: Any) -> list[dict[str, Any]]:
        results = []
        for system in conn.get_systems():
            try:
                pairs = conn.get_copy_services(system.id)
                for pair in pairs:
                    results.append(
                        {
                            "system_id": system.id,
                            "pair_id": getattr(pair, "id", ""),
                            "source_volume": getattr(pair, "source_volume", ""),
                            "target_volume": getattr(pair, "target_volume", ""),
                            "state": getattr(pair, "state", ""),
                            "type": getattr(pair, "type", ""),
                        }
                    )
            except Exception as e:
                self.logger.warning(
                    "replication_query_failed", system=getattr(system, "id", ""), error=str(e)
                )
        self.logger.info("ds8k_replication_queried", count=len(results))
        return results


class CSMClient(BaseAPIClient):
    """Client for IBM Copy Services Manager via its REST API.

    FIX #3: The previously used 'pycsm' package does not exist on PyPI.
    IBM CSM exposes a REST API documented in IBM SC27-9229. This client
    uses requests directly against that REST API.

    Base URL pattern: https://<csm-host>:<port>/CSM/web/
    Authentication: HTTP Basic or session token (we use Basic here).
    """

    # IBM CSM REST API paths (SC27-9229)
    _SESSIONS_PATH = "/CSM/web/sessions"
    _POLICIES_PATH = "/CSM/web/storagedevices"
    _REPLICATION_PATH = "/CSM/web/sessions/copysets"

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._session = None

    def _connect(self) -> Any:
        """Create a requests.Session authenticated to the CSM REST API.

        Thread-safe via _connect_lock.

        Returns:
            requests.Session configured with auth and headers.
        """
        if self._session is not None:
            return self._session
        with self._connect_lock:
            if self._session is not None:
                return self._session
            import requests

            creds = self._load_creds()
            session = requests.Session()
            session.auth = (creds["username"], creds["password"])
            session.headers.update(
                {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )
            session.verify = os.environ.get("VERIFY_SSL", "true").lower() == "true"
            self._session = session
            self.logger.info("csm_connected", url=self.endpoint.url)
        return self._session

    def _get(self, path: str) -> Any:
        """Perform a GET against the CSM REST API.

        Args:
            path: API path (e.g. '/CSM/web/sessions').

        Returns:
            Parsed JSON response body.

        Raises:
            requests.HTTPError: On non-2xx response.
        """
        import requests

        session = self._connect()
        url = f"{self.endpoint.url.rstrip('/')}{path}"
        response = session.get(url, timeout=self.endpoint.timeout)
        response.raise_for_status()
        return response.json()

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query CSM for sessions, policies, or replication copysets.

        Args:
            data_type: One of 'sessions', 'policies', 'replication'.
            platform_rule: PlatformRule configuration.

        Returns:
            List of dicts with CSM data.
        """
        if data_type == "sessions":
            return self._query_sessions()
        elif data_type == "policies":
            return self._query_policies()
        elif data_type == "replication":
            return self._query_replication()
        else:
            self.logger.warning("unknown_csm_data_type", data_type=data_type)
            return []

    def _query_sessions(self) -> list[dict[str, Any]]:
        """Query CSM sessions via GET /CSM/web/sessions."""
        try:
            data = self._get(self._SESSIONS_PATH)
            sessions = data if isinstance(data, list) else data.get("sessions", [])
            results = [
                {
                    "session_id": s.get("name", ""),
                    "name": s.get("name", ""),
                    "state": s.get("state", ""),
                    "role": s.get("role", ""),
                    "type": s.get("type", ""),
                    "last_updated": s.get("lastUpdated", ""),
                }
                for s in sessions
            ]
            self.logger.info("csm_sessions_queried", count=len(results))
            return results
        except Exception as e:
            self.logger.error("csm_sessions_query_failed", error=str(e))
            return []

    def _query_policies(self) -> list[dict[str, Any]]:
        """Query CSM storage devices (policy targets) via GET /CSM/web/storagedevices."""
        try:
            data = self._get(self._POLICIES_PATH)
            devices = data if isinstance(data, list) else data.get("storagedevices", [])
            results = [
                {
                    "device_id": d.get("id", ""),
                    "name": d.get("name", ""),
                    "type": d.get("type", ""),
                    "ip_address": d.get("ipAddress", ""),
                    "status": d.get("status", ""),
                }
                for d in devices
            ]
            self.logger.info("csm_policies_queried", count=len(results))
            return results
        except Exception as e:
            self.logger.error("csm_policies_query_failed", error=str(e))
            return []

    def _query_replication(self) -> list[dict[str, Any]]:
        """Query CSM replication copysets via GET /CSM/web/sessions/copysets."""
        try:
            data = self._get(self._REPLICATION_PATH)
            copysets = data if isinstance(data, list) else data.get("copysets", [])
            results = [
                {
                    "copyset_id": c.get("id", ""),
                    "session": c.get("sessionName", ""),
                    "state": c.get("state", ""),
                    "source_volume": c.get("sourceVolume", ""),
                    "target_volume": c.get("targetVolume", ""),
                    "sync_percent": c.get("syncPercent", None),
                }
                for c in copysets
            ]
            self.logger.info("csm_replication_queried", count=len(results))
            return results
        except Exception as e:
            self.logger.error("csm_replication_query_failed", error=str(e))
            return []


class TS7700Client(BaseAPIClient):
    """Client for IBM TS7700 tape virtualization via REST API.

    API paths are per IBM TS7700 Virtualization Engine REST API Reference
    (SC27-9158). Tested against TS7700 firmware R4.2+.
    """

    # FIX #7: Paths verified against IBM SC27-9158 TS7700 REST API Reference.
    # /api/v1.0/ prefix; 'drives' resource is 'libraries' in the IBM schema.
    _ENDPOINTS_MAP = {
        "cluster": "/api/v1.0/cluster",
        "cache": "/api/v1.0/cache",
        "drives": "/api/v1.0/libraries",       # IBM SC27-9158 §4.3: library resources
        "replication": "/api/v1.0/replication",
    }

    def __init__(self, endpoint: SourceEndpoint) -> None:
        super().__init__(endpoint)
        self._session: Any | None = None

    def _get_session(self) -> Any:
        """Get or create a requests session with auth. Thread-safe via _connect_lock.

        Returns:
            requests.Session with auth configured.
        """
        # FIX #1: thread-safe lazy session creation
        if self._session is not None:
            return self._session
        with self._connect_lock:
            if self._session is not None:
                return self._session
            import requests

            creds = self._load_creds()
            session = requests.Session()
            session.auth = (creds["username"], creds["password"])
            session.headers.update(
                {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )
            session.verify = os.environ.get("VERIFY_SSL", "true").lower() == "true"
            self._session = session
            self.logger.info("ts7700_session_created", url=self.endpoint.url)
        return self._session

    def query(self, data_type: str, platform_rule: PlatformRule) -> Any:
        """Query TS7700 for cluster info, cache, drives (libraries), or replication.

        Args:
            data_type: One of 'cluster', 'cache', 'drives', 'replication'.
            platform_rule: PlatformRule configuration.

        Returns:
            Structured dict or list of dicts.
        """
        import requests

        session = self._get_session()
        timeout = self.endpoint.timeout

        path = self._ENDPOINTS_MAP.get(data_type)
        if not path:
            self.logger.warning("unknown_ts7700_data_type", data_type=data_type)
            return []

        url = f"{self.endpoint.url.rstrip('/')}{path}"

        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            self.logger.info("ts7700_queried", data_type=data_type, url=url)
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error("ts7700_query_failed", data_type=data_type, url=url, error=str(e))
            return []


def main() -> None:
    """Entry point for the poller container."""
    platform = os.environ.get("PLATFORM", "hmc")
    rules_dir = os.environ.get("RULES_DIR", "/rules")
    spool_dir = os.environ.get("SPOOL_DIR", "/spool")
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))

    poller = Poller(platform=platform, rules_dir=rules_dir, spool_dir=spool_dir)

    from .health import app as health_app
    from .health import init_health

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
