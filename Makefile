.PHONY: data_nessie set_nessie_env

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-fixture-rest-catalog: install_requirements
	./scripts/start-fixture-rest-catalog.sh

install_requirements:
	python3 -m pip install -r scripts/requirements.txt


# Custom makefile targets
data: data_clean start-fixture-rest-catalog
	python3 -m scripts.data_generators.generate_data spark-rest local

data_large: data data_clean
	python3 -m scripts.data_generators.generate_data spark-rest local

data_clean:
	rm -rf data/generated

# ========================================
# ================ NESSIE ================
# ========================================

NESSIE_ENV_FILE ?= scripts/nessie.env

clone_nessie:
	@if [ ! -d "nessie" ]; then \
		echo "Cloning Nessie repository..."; \
		git clone https://github.com/projectnessie/nessie.git nessie; \
	else \
		echo "Nessie repository exists."; \
	fi

set_nessie_env:
	@if [ -f "$(NESSIE_ENV_FILE)" ]; then \
		echo "Loading env from $(NESSIE_ENV_FILE)"; \
		set -a; . ./$(NESSIE_ENV_FILE); set +a; \
	fi; \

nessie_start: clone_nessie set_nessie_env
	@echo "Starting Nessie catalog..."
	(cd nessie/docker/catalog-auth-s3 && docker ps -q | xargs -r docker stop; docker compose down -v && docker compose up -d)

nessie_data:
	@echo "Activating venv-spark4"
	python3 -m venv .venv-spark4
	source .venv-spark4/bin/activate
	python3 -m pip install -r scripts/requirements.txt

	@echo "Generating data..."
	@if [ -f "$(NESSIE_ENV_FILE)" ]; then set -a; . ./$(NESSIE_ENV_FILE); set +a; fi; \
	python3 -m scripts.data_generators.generate_data nessie

nessie: nessie_start nessie_data