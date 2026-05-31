.PHONY: lint lint-fix typecheck test test-cov helm-lint helm-test build build-dev clean

VENV ?= .venv
PYTHON ?= python3
HELM ?= helm
CHART_DIR = charts/api-to-splunk

lint:
	$(PYTHON) -m ruff check src/ tests/
	$(PYTHON) -m ruff format --check src/ tests/

lint-fix:
	$(PYTHON) -m ruff check --fix src/ tests/
	$(PYTHON) -m ruff format src/ tests/

typecheck:
	$(PYTHON) -m mypy src/

test:
	$(PYTHON) -m pytest tests/ -v

test-cov:
	$(PYTHON) -m pytest tests/ -v \
		--cov=src \
		--cov-report=term-missing \
		--cov-fail-under=85

helm-lint:
	$(HELM) lint $(CHART_DIR)
	$(HELM) lint $(CHART_DIR) -f $(CHART_DIR)/values-prod.yaml
	$(HELM) lint $(CHART_DIR) -f $(CHART_DIR)/values-airgap.yaml

helm-test:
	$(HELM) unittest $(CHART_DIR)

build:
	docker build -t site-mon:latest .

build-dev:
	docker build --build-arg INSTALL_DEV=true -t site-mon:dev .

clean:
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

all: lint typecheck test-cov helm-lint helm-test
