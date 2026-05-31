# AGENTS.md — Mainframe Infrastructure Monitoring Pipeline

## Agent workflow

Every change goes through a PR. Never push directly to `main`.

1. `git fetch origin && git checkout main && git pull origin main`
2. `git checkout -b <prefix>/<description>` — use `feat/`, `fix/`, `audit/`, or `refactor/`
3. Implement the change with comprehensive tests
4. `make all` — must pass (lint → typecheck → test-cov → helm-lint → helm-test)
5. `git add` relevant files, commit, `git push -u origin <branch>`
6. `gh pr create --base main` — open PR for review

## Dev commands

```
make all              # lint → typecheck → test-cov → helm-lint → helm-test
make lint             # ruff check + ruff format --check
make lint-fix         # auto-fix lint + format
make typecheck        # mypy src/
make test             # pytest -v
make test-cov         # pytest --cov-fail-under=85
make helm-lint        # helm lint (default, prod, airgap values)
make helm-test        # helm unittest (requires helm-unittest plugin)
make build            # Docker build (production)
make build-dev        # Docker build with INSTALL_DEV=true
```

Run `make all` before committing. CI runs the same checks inside the container.

`helm-test` requires the helm-unittest plugin: `helm plugin install https://github.com/helm-unittest/helm-unittest`

## Architecture

- **poller** (`src/poller.py`): Queries mainframe APIs (HMC, DS8000, CSM, TS7700), evaluates rules, writes NDJSON to shared spool
- **sender** (`src/sender.py`): Watches spool directory, delivers NDJSON to Splunk HEC with gzip + retry
- **spool** (`src/spool.py`): NDJSON file staging with atomic writes (`.tmp` rename), `.processing` claim, retry-in-filename tracking, dead-letter after 5 failures
- **evaluator** (`src/evaluator.py`): JMESPath field extraction + threshold rule engine (8 operators, 4 transforms)
- **health** (`src/health.py`): Starlette app at `/healthz`, `/readyz`, `/metrics`
- Poller and Sender run as two containers in one Kubernetes Pod, sharing a PVC-backed `/spool`

## Container / CI

- UBI9 Python 3.11 base image. Runs as **UID 1001** by default.
- Must `USER root` before `mkdir` any `/` paths in Dockerfile, then `USER 1001`.
- CI builds the Docker image with `INSTALL_DEV=true`, then runs tools via `docker run -v $PWD:/app -w /app`.
- No `ENTRYPOINT` — Kubernetes `command:` in deployment.yaml provides it.
- **`httpx2` is a dev-only dependency** (only needed by Starlette TestClient). Production image does not include it.

## Testing quirks

### Platform SDK mocking
`zhmcclient`, `pyds8k`, `pycsm`, and `requests` are imported **inline** inside platform client classes (not at module level). Do NOT use `patch("src.poller.zhmcclient")` — it will raise `AttributeError`. Use `patch.dict(sys.modules, {"zhmcclient": mock_module})` instead.

### Prometheus registry cleanup
`health.py` registers Prometheus metrics at module import. `conftest.py` provides an `autouse` fixture that clears `REGISTRY` between tests. If you add new Prometheus metric registrations in tests, they must be re-registered after cleanup.

### Starlette TestClient
Requires `httpx2` (in `requirements-dev.txt`). Import `from starlette.testclient import TestClient` in test files — this needs the package to be installed locally.

### tenacity `before_sleep_log`
Accepts a `logging.Logger` (stdlib), not a `structlog.BoundLogger`. Pass `logging.getLogger(__name__)` to tenacity wrappers, not `setup_logging()`.

### Platform-specific dependency versions
- `pyds8k>=1.6.0` (1.7.0 does NOT exist on public PyPI — was a typo in the original requirements)
- `httpx2>=2.0` (all stable releases start at 2.x, not 0.x)

## Secrets

Four per-platform ExternalSecrets (HMC/DS/CSM/TS7700), each containing the platform's credentials + `SPLUNK_HEC_TOKEN`. The Helm deployment template uses `vault.secretName` to select the right one per platform. ArgoCD Application manifests set this per platform via inline `values: |`.

## ArgoCD

Four Application manifests in `argocd/`. Each references `values.yaml` + `values-prod.yaml`. Production overrides are in `values-prod.yaml` (GHCR image, 2 replicas, increased resources, log level). The `release.yml` workflow auto-updates the image tag in `values-prod.yaml` on tag push.
