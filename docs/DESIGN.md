# DESIGN.md — Why we built it this way

## Purpose

`site_mon` monitors IBM mainframe infrastructure (HMC, DS8000, CSM, TS7700) and ships operational metrics to Splunk. It runs as a Kubernetes Pod with two sidecar containers — a Poller and a Sender — sharing a PVC-backed spool directory.

## Architecture at a glance

```
              ┌──────────────────────────────────┐
              │              Pod                  │
              │                                  │
HMC ◄────────│────► Poller ────► spool/ ────►  │────► Splunk HEC
DS  ◄────────│                  (PVC)           │
CSM ◄────────│────► Sender ────────────────────►│
TS  ◄────────│                                  │
              │   ▲                              │
              │   │ envFrom: Secret              │
              └───│──────────────────────────────┘
                  │
              Vault (External Secrets Operator)
```

Poller and Sender run as independent containers, each with its own health endpoint, sharing exactly one resource: the spool directory.

## Key design decisions

### Sidecar over monolith

**We chose**: two containers in one Pod — Poller and Sender.
**We rejected**: a single process that both queries APIs and sends to Splunk.

Why:
- **Crash isolation.** If the HMC API returns garbage and the Poller crashes, the Sender keeps delivering already-spooled data.
- **Independent scaling profiles.** Poller needs CPU for JMESPath evaluation and up to 5 parallel HTTP calls. Sender is mostly I/O-bound waiting on HEC responses. They have separate resource requests/limits.
- **Separate health endpoints** (port 8080 for Poller, 8081 for Sender) enable independent readiness gating and Prometheus scraping.

### Filesystem spool over message broker

**We chose**: NDJSON files on a shared PVC.
**We rejected**: RabbitMQ, Kafka, Redis streams, or an embedded queue.

Why:
- **Zero infrastructure.** The Pod already needs a PVC for persistence. No broker cluster to deploy, upgrade, or monitor.
- **Crash-safe by construction.** Atomic writes (`tmp` file → `os.rename`) prevent partial reads. The Linux filesystem is the durability layer.
- **Data durability.** Spool files survive container restarts and Pod rescheduling as long as the PVC exists.
- **Splunk HEC natively accepts NDJSON.** Each file is a valid HEC payload — no transformation needed.

### Retry count in filename

**We chose**: encode retry count as `{batch_id}_{count}_{timestamp}.ndjson`.
**We rejected**: storing retry count in a database, in file metadata, or in the file content itself.

Why:
- The Sender is **stateless across restarts**. It reads filenames, not a database.
- No deserialization needed to check retry count — parse the filename, not the content.
- The timestamp suffix makes every filename unique and sortable by creation time.

### Atomic write protocol

Files transition through three states:
1. **`.tmp`** — Poller writes NDJSON content while being composed
2. **`.ndjson`** — atomically renamed; visible to Sender's `list_pending()`
3. **`.processing`** — Sender claims the file by renaming it before reading; prevents double-delivery

On success: file is deleted. On failure: Sender renames back to `.ndjson` with incremented retry count. After 5 failures: moved to `dead_letter/` for manual inspection.

### YAML-driven rules over hardcoded thresholds

**We chose**: JMESPath extraction expressions + threshold rules defined in YAML files under `files/rules/platforms/`.
**We rejected**: hardcoded thresholds in Python or a proprietary DSL.

Why:
- Operators can **add monitoring for new data types without shipping code** — just add a YAML file.
- `common.yaml` provides shared field defaults; per-data-type files override them.
- The 8 operators (`eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `contains`, `regex`) and 4 transforms (`int`, `float`, `str`, `bool`) cover all mainframe monitoring scenarios without custom code.

### Failover vs multi-endpoint source selection

Mainframe platforms fall into two routing categories:

| Platforms | Mode | Behavior |
|-----------|------|----------|
| HMC, TS7700 | **Primary/backup failover** | Use primary unless unhealthy, then switch to backup |
| DS8000, CSM | **All healthy endpoints** | Query every endpoint whose health probe passes |

The distinction exists because HMC and TS7700 run active/passive with redundant hardware, while DS8000 and CSM are dual-active with independent data on each side.

### Inline SDK imports

Platform-specific SDKs (`zhmcclient`, `pyds8k`, `pycsm`) are imported **inside** class methods, not at the module level.

Why:
- If a platform's SDK is not installed (e.g., the container only needs to talk to HMC), the Poller still starts and fails gracefully when that platform's code path is hit.
- Tests can mock entire SDKs by injecting into `sys.modules` before the inline import runs.

## Data model

| Model | Purpose |
|-------|---------|
| `SourceEndpoint` | Connection config for one mainframe API (URL, auth type, rate limit) |
| `HealthStatus` | Health state for one endpoint (healthy, degraded, failure count) |
| `FieldExtraction` | One JMESPath field extraction rule |
| `ThresholdRule` | One threshold check (field, operator, value, severity) |
| `PlatformRule` | All extractions + thresholds for one data type on one platform |
| `SiteConfig` | One site's endpoints and data type list |
| `PollingEvent` | Result of one polling cycle: extracted fields + generated alerts |
| `SpoolRecord` | Batch of HEC events staged for delivery |

## Rule engine walkthrough

Given this YAML fragment:
```yaml
extractions:
  - field_name: cpu_usage_pct
    json_path: $.cpu_usage_pct
thresholds:
  - field: cpu_usage_pct
    operator: gt
    value: 90
    severity: warning
    message_template: "CPC {cpc_name} CPU is {cpu_usage_pct}%"
```

And a raw API response:
```json
{"cpc_name": "CPCA", "cpu_usage_pct": 95.1}
```

The Evaluator:
1. Runs `jmespath.search("$.cpu_usage_pct", response)` → `95.1`
2. Checks `95.1 > 90` → `True`
3. Formats `"CPC CPCA CPU is 95.1%"` as the alert message
4. Emits a `PollingEvent` with `alerts: [{"severity": "warning", "message": "..."}]`

## Security model

- **Credentials via Vault.** The External Secrets Operator maps Vault paths to Kubernetes Secrets injected as environment variables. No credentials in code, ConfigMaps, or container images.
- **Per-platform secrets.** Each of the four platforms has its own ExternalSecret and Kubernetes Secret (`api-to-splunk-hmc-secrets`, etc.). A Poller container for HMC cannot read CSM credentials.
- **Non-root execution.** Container runs as UID 1001. `USER root` exists only transiently in the Dockerfile to create `/spool` and `/rules`.
- **NetworkPolicy.** Egress is restricted to DNS (UDP 53), Splunk HEC (TCP 8088), and the platform-specific API endpoints (TCP 443).
