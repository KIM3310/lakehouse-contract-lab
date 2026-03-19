# =============================================================================
# Lakehouse Contract Lab - Makefile
# =============================================================================

.PHONY: install test lint format build docker-build docker-run docker-down pipeline clean help

PYTHON ?= python3
VENV   := .venv
PIP    := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF   := $(VENV)/bin/ruff
UVICORN := $(VENV)/bin/uvicorn

APP_PORT ?= 8096
IMAGE_NAME ?= lakehouse-contract-lab
IMAGE_TAG  ?= latest

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Create venv and install all dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

test: ## Run the pytest suite
	$(PYTEST) -v --tb=short

test-cov: ## Run tests with coverage reporting
	$(PYTEST) -v --tb=short --cov=app --cov=scripts --cov-report=term-missing --cov-report=html

lint: ## Run ruff linter
	$(RUFF) check .

format: ## Run ruff formatter
	$(RUFF) format .

format-check: ## Check formatting without modifying files
	$(RUFF) format --check .

build: ## Run the medallion pipeline and generate artifacts
	$(PYTHON) scripts/build_lakehouse_artifacts.py

serve: ## Start the FastAPI development server
	$(UVICORN) app.main:app --host 127.0.0.1 --port $(APP_PORT) --reload

docker-build: ## Build the Docker image
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-run: ## Run the app via Docker Compose
	docker compose up --build -d

docker-down: ## Stop Docker Compose services
	docker compose down

docker-logs: ## Follow Docker Compose logs
	docker compose logs -f app

pipeline: lint test build ## Full pipeline: lint, test, then build artifacts

clean: ## Remove generated files and caches
	rm -rf $(VENV) __pycache__ .pytest_cache .ruff_cache *.egg-info
	rm -rf artifacts/runtime_delta
	rm -rf htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
