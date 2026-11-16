.DEFAULT_GOAL := help
PYTHON ?= python
PIPELINE := $(PYTHON) -m vet_lakehouse.pipeline
IMAGE ?= vet-lakehouse

.PHONY: help install lint format test run run-incremental generate bronze silver gold dq \
        optimize summary clean docker-build docker-run docker-test tf-validate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install: ## Install the package and dev tools (needs Python 3.10+ and Java 17)
	$(PYTHON) -m pip install -e ".[dev]"

lint: ## Check code style with ruff
	ruff check .
	ruff format --check .

format: ## Auto-format the code with ruff
	ruff check --fix .
	ruff format .

test: ## Run the unit and end-to-end tests
	$(PYTHON) -m pytest

run: ## Fresh start: generate data, then bronze -> silver -> gold -> dq
	$(PIPELINE) run --reset

run-incremental: ## Add two more weeks of files and load only the new ones
	$(PIPELINE) run --days 73

generate: ## Only generate the source files
	$(PIPELINE) generate

bronze: ## Only run the Bronze layer
	$(PIPELINE) bronze

silver: ## Only run the Silver layer
	$(PIPELINE) silver

gold: ## Only run the Gold layer
	$(PIPELINE) gold

dq: ## Only run the data quality checks
	$(PIPELINE) dq

optimize: ## Compact files and Z-ORDER the main tables
	$(PIPELINE) optimize

summary: ## Print row counts and a few report queries
	$(PIPELINE) summary

clean: ## Delete the local lakehouse and caches
	rm -rf .lakehouse .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log

docker-build: ## Build the Docker image (Python + Java + Spark + Delta)
	docker build -t $(IMAGE) .

docker-run: docker-build ## Run the full pipeline in Docker (works on any OS)
	docker run --rm $(IMAGE)

docker-test: docker-build ## Run lint and tests in Docker
	docker run --rm $(IMAGE) sh -c "make lint && make test"

tf-validate: ## Check the example Terraform code (no Azure login needed)
	cd infra && terraform fmt -check -recursive && terraform init -backend=false -input=false && terraform validate
