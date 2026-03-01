# ================================
# Variáveis
# ================================
PROJECT_NAME=gov_terceirizados
IMAGE_NAME=$(PROJECT_NAME):latest
CONTAINER_NAME=$(PROJECT_NAME)_container
DBT_PROJECT_DIR=./dw
DBT_PROFILES_DIR=./dw
WORKER_NAME=$(PROJECT_NAME)_worker

PYTHON_SRC=.
SQL_PATH=models

# ================================
# Help
# ================================
.PHONY: help
help:
	@echo "Comandos disponíveis:"
	@echo ""
	@echo "Docker:"
	@echo "  make build"
	@echo "  make up"
	@echo "  make down"
	@echo ""
	@echo "DBT:"
	@echo "  make dbt-debug"
	@echo "  make dbt-run"
	@echo "  make dbt-test"
	@echo ""
	@echo "Lint:"
	@echo "  make lint"
	@echo "  make lint-python"
	@echo "  make lint-sql"
	@echo "  make format"
	@echo ""
	@echo "Pipeline:"
	@echo "  make prefect"
	@echo "Data:"
	@echo "  make local-data"

# ================================
# Docker
# ================================
.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: up
up:
	docker run -d --name $(CONTAINER_NAME) $(IMAGE_NAME)

.PHONY: down
down:
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true

# ================================
# DBT
# ================================
.PHONY: dbt-debug
dbt-debug:
	dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

.PHONY: dbt-run
dbt-run:
	dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

.PHONY: dbt-test
dbt-test:
	dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)


# ================================
# LINT
# ================================

.PHONY: lint
lint: lint-python lint-sql

# Python lint (rápido)
.PHONY: lint-python
lint-python:
	ruff check $(PYTHON_SRC)
	black --check $(PYTHON_SRC)
	mypy $(PYTHON_SRC)

# SQL/dbt lint
.PHONY: lint-sql
lint-sql:
	sqlfluff lint $(SQL_PATH)

# Auto formatação
.PHONY: format
format:
	black $(TARGET)
	ruff check --fix $(TARGET)
	sqlfluff fix $(TARGET)

clean:
	@echo "Limpando artefatos..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	-rm -f /app/dw/dev.duckdb

# ================================
# Prefect
# ================================
.PHONY: prefect
prefect:
	python flows/main_flow.py

.PHONY: prefect-worker
prefect-worker:
	@echo "Verificando worker: $(WORKER_NAME)"

	@if prefect worker inspect "$(WORKER_NAME)" >/dev/null 2>&1; then \
		echo "Worker $(WORKER_NAME) já existe. Iniciando..."; \
	else \
		echo "Worker $(WORKER_NAME) não existe. Criando..."; \
		prefect work-pool create "$(WORKER_NAME)" --type docker; \
	fi

	@echo "Iniciando worker $(WORKER_NAME)..."
	@prefect worker start -p "$(WORKER_NAME)"


prefect-deployment:
	prefect deploy $(DEPLOY_FLOW_DIR)   --name $(DEPLOY_NAME)   --pool $(WORKER_NAME)  -jv image=$(IMAGE_NAME)   -jv image_pull_policy=Never


# ================================
# Data
# ================================
.PHONY: local-data fetch-data
local-data:
	python scripts/sync_gcs --layer all
fetch-data:
	python scripts/fetch_terceirizados_data.py --periodo $(PERIODO)

