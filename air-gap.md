# Air-Gap Dependencies Manifest

## Python Packages

### Direct Dependencies
| Package | Version Range |
|---------|---------------|
| zhmcclient | >=1.14.0,<2.0.0 |
| pyds8k | >=1.7.0,<2.0.0 |
| pycsm | >=1.0.0,<2.0.0 |
| requests | >=2.31.0,<3.0.0 |
| urllib3 | >=2.0.0,<3.0.0 |
| pyyaml | >=6.0,<7.0.0 |
| pydantic | >=2.0.0,<3.0.0 |
| tenacity | >=8.0.0,<9.0.0 |
| watchdog | >=4.0.0,<5.0.0 |
| structlog | >=24.0.0,<25.0.0 |
| uvicorn | >=0.27.0,<1.0.0 |
| starlette | >=0.36.0,<1.0.0 |
| prometheus-client | >=0.19.0,<1.0.0 |
| jmespath | >=1.0.0,<2.0.0 |

### Transitive Dependencies
Many of the packages above pull in additional transitive dependencies.
Run `pip download -r requirements.txt -d ./wheels/` from an internet-connected
machine to capture all required wheels including transitive dependencies.

## Container Base Image
- `registry.access.redhat.com/ubi9/python-311:latest`
- Maintains Red Hat UBI 9 compatibility for OpenShift deployments
- Must be mirrored to internal registry: `registry.internal.example.com/ubi9/python-311`

## Helm Binary
- If running `helm template` in CI pipelines, ensure the Helm binary is available
- Download from: https://github.com/helm/helm/releases
- Minimum version: 3.12.0

## External Secrets Operator
- CRDs required for SecretStore and ExternalSecret resources
- Helm chart: `external-secrets/external-secrets` (v0.9.0+)
- Must be pre-installed in the cluster or mirrored to internal chart repo
- Installation: `helm install external-secrets external-secrets/external-secrets -n external-secrets --create-namespace`

## Custom CA Certificates
- Internal endpoints (Vault, Splunk HEC, mainframe APIs) likely use internal CAs
- Obtain CA certificate bundle from your PKI team
- Mount as a ConfigMap and configure Python to trust via `REQUESTS_CA_BUNDLE` or `SSL_CERT_FILE`
- Alternatively, inject via OpenShift trusted CA bundle injection

## Mirroring to Internal Registry

1. **Container Images:**
   ```bash
   podman pull registry.access.redhat.com/ubi9/python-311:latest
   podman tag registry.access.redhat.com/ubi9/python-311:latest registry.internal.example.com/ubi9/python-311:1.0.0-airgap
   podman push registry.internal.example.com/ubi9/python-311:1.0.0-airgap
   ```

2. **Python Wheels:**
   ```bash
   pip download -r requirements.txt -d ./wheels/
   # Transfer ./wheels/ to air-gapped environment
   pip install --no-index --find-links ./wheels/ -r requirements.txt
   ```

3. **Helm Charts:**
   ```bash
   helm repo add external-secrets https://charts.external-secrets.io
   helm pull external-secrets/external-secrets --version 0.9.0 -d ./charts/
   helm push ./charts/external-secrets-0.9.0.tgz oci://registry.internal.example.com/helm
   ```

4. **Application Helm Chart:**
   Package and push the api-to-splunk chart to internal OCI registry if ArgoCD
   cannot reach the git repository from the air-gapped environment.
