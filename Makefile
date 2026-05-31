.PHONY: lint lint-fix typecheck test test-cov build clean

VENV ?= .venv
PYTHON ?= python3

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

build:
	docker build -t site-mon:latest .

build-dev:
	docker build --build-arg INSTALL_DEV=true -t site-mon:dev .

clean:
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

all: lint typecheck test-cov
