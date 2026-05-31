# ARCHITECTURE.md — Component reference and runtime behavior

## Component inventory

| Component | File | Responsibility | Key methods |
|-----------|------|---------------|-------------|
| Poller | `src/poller.py` | Orchestrates the polling cycle: source selection, API queries, rule evaluation, spool writing | `run_once()`, `run_forever()`, `_query_and_evaluate()`, `_events_to_hec_lines()` |
| Sender | `src/sender.py` | Watches spool directory, sends NDJSON to Splunk HEC, handles ack/nack/cleanup | `run_once()`, `run_forever()`, `cleanup()` |
| SpoolManager | `src/spool.py` | Manages NDJSON file lifecycle on the shared PVC | `write_ndjson()`, `read_ndjson_batch()`, `ack_file()`, `nack_file()`, `get_spool_stats()` |
| Evaluator | `src/evaluator.py` | Applies JMESPath field extractions and threshold rule checks to raw API responses | `evaluate()`, `_extract_fields()`, `_check_thresholds()` |
| SplunkHECClient | `src/splunk_hec.py` | HTTP client for Splunk HEC with gzip compression, ACK channel, and tenacity retry | `send_ndjson()`, `_create_ack_channel()`, `_do_post()` |
| SourceSelector | `src/source_selector.py` | Selects active endpoints based on health status and platform-specific routing policy | `get_active_endpoints()`, `_select_failover()`, `_select_all_healthy()` |
| EndpointHealthChecker | `src/endpoint_health.py` | Background thread that probes endpoints via HTTP GET and tracks health state | `start()`, `stop()`, `is_healthy()`, `get_status()` |
| RulesLoader | `src/rules_loader.py` | Loads YAML rule files and site configs into Pydantic models | `load_platform_rules()`, `load_site_configs()`, `load_full_config()` |
| Health app | `src/health.py` | Starlette ASGI app exposing liveness, readiness, and metrics | `/healthz`, `/readyz`, `/metrics` |

## Spool file lifecycle

```
write_ndjson()
   │
   ▼
  .tmp ──→ os.rename() ──→ .ndjson          (Poller)
                               │
                               ▼
                          list_pending()      (Sender)
                               │
                               ▼
                     os.rename() → .processing
                               │
                          read content
                               │
                               ▼
                    ┌── HEC POST success? ──┐
                    │                       │
                   yes                      no
                    │                       │
                    ▼                       ▼
              ack_file()            retry < 5?
               (delete)              │       │
                                   yes      no
                                    │       │
                                    ▼       ▼
                               nack_file() move_to_dead_letter()
                           {name}_{N+1}_{ts}.ndjson
```

File naming convention: `{batch_id}_{retry_count}_{microsecond_timestamp}.ndjson`,
e.g., `a1b2c3d4_2_1717171200123456.ndjson` — batch `a1b2c3d4`, 2nd retry.

## Polling cycle (`Poller.run_once()`)

1. **Source selection.** `SourceSelector.get_active_endpoints()` returns healthy endpoints based on the platform's routing policy (failover for HMC/TS7700, all-healthy for DS/CSM).

2. **Parallel queries.** A `ThreadPoolExecutor` (max 5 workers) dispatches `_query_and_evaluate()` for each `(endpoint, data_type)` pair.

3. **API query.** The platform-specific client (HMCClient, DS8000Client, CSMClient, or TS7700Client) calls the mainframe API and returns raw JSON.

4. **Evaluation.** `Evaluator.evaluate()` applies the JMESPath extractions and threshold checks defined in the platform's YAML rules, producing `PollingEvent` objects with any generated alerts.

5. **Serialization.** Each `PollingEvent` is converted to Splunk HEC NDJSON format:
   ```json
   {"time":"1717171200.000000","host":"hmc-primary","source":"hmc:cpc-stats","sourcetype":"hmc:cpc_stats","index":"mainframe_metrics","event":{...}}
   ```

6. **Spool write.** `SpoolManager.write_ndjson()` atomically writes the NDJSON content to a `.tmp` file and renames it to `.ndjson`.

7. Returns the number of HEC events written.

## Delivery cycle (`Sender.run_once()`)

1. **Read batch.** `SpoolManager.read_ndjson_batch(max_files=50)` renames pending `.ndjson` files to `.processing` and reads their content.

2. **Send.** For each file, `SplunkHECClient.send_ndjson()` POSTs the raw NDJSON content to `/services/collector/event` with gzip compression and the optional `X-Splunk-Request-Channel` header.

3. **Acknowledge.** On HTTP 200: `SpoolManager.ack_file()` deletes the file.

4. **Retry.** On failure: `SpoolManager.nack_file()` increments the retry count in the filename and renames back to `.ndjson`. On HTTP 429/503, the client sleeps for the `Retry-After` duration. The `tenacity` decorator on `_do_post()` retries up to 5 times with exponential jitter (1s → 64s).

5. **Dead-letter.** After 5 retries: `SpoolManager.move_to_dead_letter()` moves the file to `dead_letter/` for manual inspection.

6. **Cleanup.** Every hour, `SpoolManager.cleanup_old_files(24)` removes files older than 24 hours.

## Health and observability

| Endpoint | Port | Container | Purpose |
|----------|------|-----------|---------|
| `/healthz` | 8080 | Poller | Liveness: always returns 200 (process check) |
| `/readyz` | 8080 | Poller | Readiness: spool not full AND at least one endpoint healthy |
| `/metrics` | 8080 | Poller | Prometheus metrics |
| `/healthz` | 8081 | Sender | Liveness: always returns 200 (process check) |
| `/readyz` | 8081 | Sender | Readiness: spool not full |
| `/metrics` | 8081 | Sender | Prometheus metrics |

Prometheus metrics:

| Metric | Type | Labels |
|--------|------|--------|
| `site_mon_spool_pending_count` | Gauge | — |
| `site_mon_spool_size_mb` | Gauge | — |
| `site_mon_spool_dead_letter_count` | Gauge | — |
| `site_mon_batch_send_duration_seconds` | Histogram | — |
| `site_mon_batch_send_errors_total` | Counter | — |
| `site_mon_endpoint_health` | Gauge | `endpoint` |
| `site_mon_polling_cycle_duration_seconds` | Histogram | — |
| `site_mon_api_query_errors_total` | Counter | — |

## Kubernetes topology

```
Pod (mainframe-monitoring namespace)
│
├── securityContext: runAsNonRoot: true, runAsUser: 1001
├── serviceAccountName: api-to-splunk-sa (Vault auth)
│
├── Container: poller
│   ├── command: python -m src.poller
│   ├── port: 8080 (health/metrics)
│   ├── probes: liveness /healthz, readiness /readyz
│   ├── envFrom: ConfigMap (app config) + Secret (platform credentials)
│   ├── volumeMounts: /spool (PVC), /rules (ConfigMap)
│   └── resources: requests {100m, 256Mi}, limits {500m, 512Mi}
│
├── Container: sender
│   ├── command: python -m src.sender
│   ├── port: 8081 (health/metrics)
│   ├── probes: liveness /healthz, readiness /readyz
│   ├── envFrom: ConfigMap (app config) + Secret (platform credentials)
│   ├── volumeMounts: /spool (PVC)
│   └── resources: requests {50m, 128Mi}, limits {200m, 256Mi}
│
├── Volumes
│   ├── spool: PersistentVolumeClaim (1Gi / 5Gi prod)
│   └── rules: ConfigMap (from files/rules/**/*.yaml)
│
├── NetworkPolicy
│   ├── Egress: UDP 53 (DNS)
│   ├── Egress: TCP 8088 (Splunk HEC)
│   └── Egress: TCP 443 (API endpoints per platform)
│
└── PodDisruptionBudget: minAvailable 1
```

## Configuration layer

```
charts/api-to-splunk/files/rules/
├── platforms/
│   ├── hmc/
│   │   ├── common.yaml          # shared fields inherited by all HMC rules
│   │   ├── source-policy.yaml   # failover vs multi-endpoint policy
│   │   ├── cpc-stats.yaml        # data type rule
│   │   ├── lpars.yaml
│   │   └── ...
│   ├── ds/
│   ├── csm/
│   └── ts7700/
└── sites/
    ├── hmc/
    │   ├── primary.yaml          # endpoint URLs, auth config, data types
    │   └── backup.yaml
    ├── ds/
    ├── csm/
    └── ts7700/
```

Each platform gets at least one `site` (two for failover platforms) with endpoint connection configs. Each data type gets a YAML rule file with JMESPath extractions and threshold checks. `common.yaml` provides shared fields (like `platform: hmc`) that are merged into every rule. `source-policy.yaml` controls the endpoint selection strategy.

The entire `files/rules/` directory is mounted as a Kubernetes ConfigMap at `/rules` inside the Poller container.

## External dependencies

| Service | Protocol | Purpose | Failure behavior |
|---------|----------|---------|------------------|
| HMC API | HTTPS | Query CPC stats, LPARs, CHPIDs | Endpoint marked unhealthy, failover to backup |
| DS8000 API | HTTPS | Query arrays, ports, ranks, replication | Endpoint excluded from active set |
| CSM API | HTTPS | Query sessions, policies, replication | Endpoint excluded from active set |
| TS7700 API | HTTPS | Query cluster, cache, drives, replication | Endpoint marked unhealthy, failover to backup |
| Splunk HEC | HTTPS | Deliver NDJSON metrics | Exponential retry with jitter (5 attempts). File dead-lettered after exhaustion. |
| HashiCorp Vault | HTTPS | Provide API credentials at startup | External Secret Operator handles refresh. Stale credentials cause auth failures on next API call. |
| DNS | UDP 53 | Resolve API and HEC hostnames | NetworkPolicy allows. Host-level DNS failure causes connection errors. |

## CI/CD

| Workflow | Trigger | Steps |
|----------|---------|-------|
| `ci.yml` | PR → main | Build (Docker + INSTALL_DEV=true), lint (ruff), typecheck (mypy), tests (pytest 85%+ cov), pip-audit, trivy scan, helm lint + unittest |
| `release.yml` | Tag `v*` | Full CI gates → build & push Docker image to GHCR → package Helm chart → push to GHCR OCI → update `values-prod.yaml` tag → commit |
| `schedule.yml` | Daily 2 AM | Full test suite + pip-audit + trivy full scan (all severities) |

CI runs everything inside a Docker container built from the production Dockerfile with `INSTALL_DEV=true`, matching the production Python 3.11 / UBI9 environment. See `DESIGN.md` for rationale.
