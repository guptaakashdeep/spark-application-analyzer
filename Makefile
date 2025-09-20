.PHONY: help install install-dev clean test lint format type-check build dist clean-build

help: ## Show this help message
	@echo "Spark Application Analyzer - Development Commands"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the package in development mode
	pip3 install -e .

install-dev: ## Install the package with development dependencies
	pip3 install -e ".[dev]"

clean: ## Clean up build artifacts and cache
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

test: ## Run tests with coverage
	pytest --cov=spark_application_analyzer --cov-report=html --cov-report=term-missing

test-fast: ## Run tests without coverage (faster)
	pytest

lint: ## Run linting with ruff
	ruff check .

format: ## Format code with ruff
	ruff format .

type-check: ## Run type checking with mypy
	mypy spark_application_analyzer/

check-all: ## Run all code quality checks
	@echo "Running all code quality checks..."
	@make lint
	@make type-check
	@make test

build: ## Build the package
	python -m build

dist: ## Create distribution packages
	python -m build --wheel --sdist

clean-build: clean build ## Clean and rebuild

install-local: ## Install the package locally
	pip3 install dist/*.whl

uninstall: ## Uninstall the package
	pip3 uninstall spark-application-analyzer -y

setup-dev: ## Set up development environment
	@echo "Setting up development environment..."
	python -m venv .venv
	@echo "Virtual environment created. Activate it with:"
	@echo "  source .venv/bin/activate  # On Unix/macOS"
	@echo "  .venv\\Scripts\\activate     # On Windows"
	@echo ""
	@echo "Then run: make install-dev"

run-example: ## Run example analysis (requires config.yaml)
	@echo "Running example analysis..."
	@if [ ! -f config.yaml ]; then \
		echo "Error: config.yaml not found. Copy config.yaml.example to config.yaml first."; \
		exit 1; \
	fi
	spark-analyzer analyze --history-server-url http://localhost:18080 --config-file config.yaml

create-config: ## Create config.yaml from example
	@if [ -f config.yaml ]; then \
		echo "config.yaml already exists. Skipping..."; \
	else \
		cp config.yaml.example config.yaml; \
		echo "config.yaml created from config.yaml.example"; \
		echo "Please customize the configuration for your environment."; \
	fi
