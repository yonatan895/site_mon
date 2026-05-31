#!/bin/bash
set -euo pipefail

echo "=== Downloading Python wheels ==="
mkdir -p ./wheels/
pip download -r requirements.txt -d ./wheels/

echo "=== Pulling and saving container base image ==="
podman pull registry.access.redhat.com/ubi9/python-311:latest
podman save registry.access.redhat.com/ubi9/python-311:latest -o ubi9-python311.tar

echo "=== Downloading External Secrets Helm chart ==="
mkdir -p ./charts/
helm repo add external-secrets https://charts.external-secrets.io
helm repo update
helm pull external-secrets/external-secrets --version 0.9.0 -d ./charts/

echo "=== Downloading Helm binary ==="
HELM_VERSION="3.14.0"
curl -LO "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz"
tar -xzf "helm-v${HELM_VERSION}-linux-amd64.tar.gz"
mv linux-amd64/helm ./helm-binary
rm -rf linux-amd64 "helm-v${HELM_VERSION}-linux-amd64.tar.gz"

echo "=== Done. Transfer the following to the air-gapped environment: ==="
echo "  - ./wheels/"
echo "  - ./ubi9-python311.tar"
echo "  - ./charts/"
echo "  - ./helm-binary"
