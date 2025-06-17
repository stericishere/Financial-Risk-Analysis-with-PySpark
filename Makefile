.PHONY: help install test lint format type-check clean run-etl build
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	poetry install

install-dev: ## Install development dependencies
	poetry install --with dev
	poetry run pre-commit install

test: ## Run tests
	poetry run pytest tests/ -v

test-coverage: ## Run tests with coverage
	poetry run pytest tests/ --cov=src --cov-report=html --cov-report=term

lint: ## Run linting (flake8)
	poetry run flake8 src/ jobs/ tests/

format: ## Format code (black)
	poetry run black src/ jobs/ tests/

format-check: ## Check code formatting
	poetry run black --check src/ jobs/ tests/

type-check: ## Run type checking (mypy)
	poetry run mypy src/ jobs/

quality-check: format-check lint type-check ## Run all quality checks

clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf __pycache__/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

run-etl: ## Run the main ETL job locally
	poetry run python jobs/financial_risk_etl.py

run-etl-spark: ## Run ETL job with spark-submit
	poetry run spark-submit \
		--master local[*] \
		--py-files packages.zip \
		--files configs/etl_config.json \
		jobs/financial_risk_etl.py

build-package: ## Build dependency package for spark-submit
	poetry export --without-hashes --format=requirements.txt > requirements.txt
	pip install -r requirements.txt --target packages/
	cd packages && zip -r ../packages.zip .
	rm -rf packages/ requirements.txt

jupyter: ## Start Jupyter notebook
	poetry run jupyter notebook

shell: ## Start poetry shell
	poetry shell

setup: install-dev ## Complete setup for development
	@echo "Setup complete! Run 'make help' for available commands." 