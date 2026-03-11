.PHONY: fixture lakekeeper polaris nessie

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

install_requirements:
	python3 -m pip install -r scripts/requirements.txt


# Custom makefile targets
data: data_clean fixture_start
	python3 -m scripts.data_generators.generate_data spark-rest local

data_large: data data_clean
	python3 -m scripts.data_generators.generate_data spark-rest local

data_clean:
	rm -rf data/generated

# ========================================
# ================ NESSIE ================
# ========================================

NESSIE_ENV_FILE ?= scripts/envs/nessie.env

nessie_clone:
	@if [ ! -d "nessie" ]; then \
		echo "Cloning Nessie repository..."; \
		git clone https://github.com/projectnessie/nessie.git nessie; \
	else \
		echo "Nessie repository exists."; \
	fi

nessie_start: nessie_clone
	@echo "Starting Nessie catalog..."
	(cd nessie/docker/catalog-auth-s3 && docker ps -q | xargs -r docker stop; docker compose down -v && docker compose up -d)

nessie_data:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(NESSIE_ENV_FILE)" ]; then echo "Loading env from $(NESSIE_ENV_FILE)"; set -a; . ./$(NESSIE_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data nessie

nessie: nessie_start nessie_data

# ============================================
# ================ LAKEKEEPER ================
# ============================================

LAKEKEEPER_ENV_FILE ?= scripts/envs/lakekeeper.env

lakekeeper_clone:
	@if [ ! -d "lakekeeper" ]; then \
		echo "Cloning Lakekeeper repository..."; \
		git clone https://github.com/lakekeeper/lakekeeper.git lakekeeper; \
		cd lakekeeper && git checkout f6aaa4570cf62d54943581e7a00b9f882d9c992d && git apply ../.github/patches/lakekeeper_docker_compose.patch; \
	else \
		echo "Lakekeeper repository exists."; \
	fi

lakekeeper_start: lakekeeper_clone
	@echo "Starting Lakekeeper catalog..."
	@grep -q '127.0.0.1 minio' /etc/hosts || (echo "Adding minio host entry..." && echo "127.0.0.1 minio" | sudo tee -a /etc/hosts)
	(cd lakekeeper/examples/access-control-simple && docker ps -q | xargs -r docker stop; docker compose down -v && docker compose up -d)
	@echo "Bootstrapping Lakekeeper..."
	cd lakekeeper/examples/access-control-simple && \
	docker compose exec jupyter start.sh bash -lc "\
		jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/01-Bootstrap.ipynb && \
		jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/02-Create-Warehouse.ipynb && \
		jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/03-01-Spark.ipynb"

lakekeeper_data:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	python3 -m scripts.data_generators.generate_data lakekeeper

lakekeeper: lakekeeper_start lakekeeper_data

# ==========================================
# ================ POLARIS =================
# ==========================================

POLARIS_ENV_FILE ?= scripts/envs/polaris.env

polaris_clone:
	@if [ ! -d "polaris_catalog" ]; then \
		echo "Cloning Polaris repository..."; \
		git clone https://github.com/apache/polaris.git polaris_catalog; \
		cd polaris_catalog && git checkout c940ded0f6be44d58ac6f0b6606c41cb08b53304; \
	else \
		echo "Polaris repository exists."; \
	fi

polaris_build:
	@if [ -f "polaris_catalog/runtime/server/build/quarkus-app/quarkus-run.jar" ]; then \
		echo "Polaris already built, skipping. Run 'make polaris_rebuild' to force."; \
	else \
		echo "Building Polaris..."; \
		cd polaris_catalog && ./gradlew :polaris-server:assemble -Dquarkus.container-image.build=true && ./gradlew --stop; \
	fi

polaris_rebuild:
	@echo "Rebuilding Polaris (clean)..."
	cd polaris_catalog && ./gradlew clean :polaris-server:assemble -Dquarkus.container-image.build=true --no-build-cache && ./gradlew --stop

polaris_start: polaris_clone polaris_build
	@echo "Starting Polaris server..."
	@lsof -ti:8182 | xargs -r kill -9 || true
	cd polaris_catalog && nohup ./gradlew :polaris-server:run > polaris-server.log 2> polaris-error.log &
	@echo "Waiting for Polaris to initialize..."
	@max_attempts=50; attempt=1; \
	while ! (curl -sf http://localhost:8182/healthcheck || curl -sf http://localhost:8182/q/health); do \
		if [ $$attempt -gt $$max_attempts ]; then \
			echo "Polaris failed to initialize after $$max_attempts attempts"; \
			exit 1; \
		fi; \
		echo "Waiting for Polaris to initialize (attempt $$attempt/$$max_attempts)..."; \
		sleep 5; \
		attempt=$$((attempt + 1)); \
	done
	@echo "Polaris is healthy"
	@echo "Quick-starting Polaris catalog and extracting credentials..."
	cd polaris_catalog && python3 -m venv . && . bin/activate && make client-regenerate && cd client/python && python3 -m pip install . && cd ../../../ && \
	cd polaris_catalog && ../scripts/polaris/quickstart_polaris_catalog.sh > user_credentials.json && cd .. && \
	python3 scripts/polaris/get_polaris_client_creds.py && \
	perl -i -pe "s/%PLACEHOLDER_POLARIS_CLIENT_ID%/$$(cat polaris_client_id.txt)/g; s/%PLACEHOLDER_POLARIS_CLIENT_SECRET%/$$(cat polaris_client_secret.txt)/g" test/configs/polaris.json

polaris_data:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(POLARIS_ENV_FILE)" ]; then echo "Loading env from $(POLARIS_ENV_FILE)"; set -a; . ./$(POLARIS_ENV_FILE); set +a; fi && \
	export POLARIS_CLIENT_ID=$$(cat polaris_client_id.txt) && \
	export POLARIS_CLIENT_SECRET=$$(cat polaris_client_secret.txt) && \
	python3 -m scripts.data_generators.generate_data polaris

polaris: polaris_start polaris_data

# ==========================================
# ================ FIXTURE =================
# ==========================================

FIXTURE_ENV_FILE ?= scripts/envs/fixture.env

fixture_start:
	@echo "Starting apache/iceberg-rest-fixture catalog..."
	(cd scripts && docker ps -q | xargs -r docker stop; docker compose down -v)
	sudo rm -rf data/generated
	mkdir -p data/generated/iceberg/spark-rest
	mkdir -p data/generated/intermediates
	(cd scripts && docker compose up -d)

fixture_data:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data spark-rest

fixture: fixture_start fixture_data

fixture_data_local:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data local


fixture_local: fixture_start fixture_data_local


