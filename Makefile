COMPOSE := docker compose
SERVICE ?=

# Load .env so variables like MINIO_BUCKET are visible to make targets
ifneq (,$(wildcard .env))
include .env
export
endif

# ANSI color codes
BOLD   := \033[1m
RESET  := \033[0m
GREEN  := \033[0;32m
CYAN   := \033[0;36m
YELLOW := \033[0;33m
RED    := \033[0;31m
BLUE   := \033[0;34m
DIM    := \033[2m

.PHONY: up setup run down clean clean-all logs status ps help \
        duck duck-shell duck-ls duck-query duck-show duck-count duck-schema

DUCK_EXEC := $(COMPOSE) exec -T duckdb duckdb -init /workspace/init.sql
DUCK_BUCKET := $(or $(MINIO_BUCKET),etl-data)

## Build images and start MySQL, MinIO, NiFi in detached mode
up:
	$(COMPOSE) up -d --build
	@echo ""
	@printf "$(CYAN)$(BOLD)Services starting. Estimated ready times:$(RESET)\n"
	@printf "  $(GREEN)MySQL$(RESET)  : ~30s\n"
	@printf "  $(GREEN)MinIO$(RESET)  : ~15s\n"
	@printf "  $(GREEN)NiFi$(RESET)   : ~90s  $(DIM)(JVM startup)$(RESET)\n"
	@echo ""
	@printf "$(YELLOW)Run 'make setup' once NiFi is healthy (check with 'make status').$(RESET)\n"

## Deploy the NiFi ETL flow (waits for NiFi healthy, then provisions via REST API)
setup:
	@printf "$(CYAN)$(BOLD)Deploying NiFi ETL flow...$(RESET)\n"
	$(COMPOSE) --profile setup run --rm nifi-setup

## Alias for setup
run: setup

## Stop and remove containers (volumes + images preserved)
down:
	@printf "$(YELLOW)Stopping containers (volumes + images preserved)...$(RESET)\n"
	$(COMPOSE) down

## Stop and remove containers (volumes preserved, images preserved)
clean:
	@printf "$(YELLOW)$(BOLD)Removing containers (volumes + images preserved)...$(RESET)\n"
	$(COMPOSE) down
	@printf "$(YELLOW)Done. Containers removed. Volumes and images untouched.$(RESET)\n"

## Remove containers + volumes (images preserved)
clean-all:
	@printf "$(RED)$(BOLD)Removing containers and volumes (images preserved)...$(RESET)\n"
	$(COMPOSE) down -v
	@printf "$(RED)Done. Containers and volumes removed. Images untouched.$(RESET)\n"

## Tail logs. Filter by service: make logs SERVICE=nifi
logs:
	@printf "$(DIM)Tailing logs$(RESET)$(if $(SERVICE), for $(CYAN)$(SERVICE)$(RESET))...\n"
ifdef SERVICE
	$(COMPOSE) logs -f $(SERVICE)
else
	$(COMPOSE) logs -f
endif

## Show container status
status:
	@printf "$(CYAN)$(BOLD)Container status:$(RESET)\n"
	$(COMPOSE) ps

## Alias for status
ps: status

## Open an interactive DuckDB shell with MinIO/S3 preconfigured
duck duck-shell:
	@printf "$(CYAN)$(BOLD)Opening DuckDB shell (.quit to exit)...$(RESET)\n"
	$(COMPOSE) exec duckdb duckdb -init /workspace/init.sql

## List parquet files in s3://$(MINIO_BUCKET)
duck-ls:
	@printf "$(CYAN)Listing parquet files in s3://$(DUCK_BUCKET) ...$(RESET)\n"
	@$(DUCK_EXEC) -c "SELECT DISTINCT filename FROM read_parquet('s3://$(DUCK_BUCKET)/**/*.parquet', filename=true) ORDER BY filename;"

## Run an ad-hoc SQL query. Usage: make duck-query Q="SELECT 1"
duck-query:
	@if [ -z "$(Q)" ]; then printf "$(RED)Usage: make duck-query Q=\"SELECT ...\"$(RESET)\n"; exit 1; fi
	@echo "$(Q)" | $(DUCK_EXEC)

## Preview a parquet file. Usage: make duck-show FILE=path/in/bucket.parquet [LIMIT=20]
duck-show:
	@if [ -z "$(FILE)" ]; then printf "$(RED)Usage: make duck-show FILE=path/in/bucket.parquet [LIMIT=20]$(RESET)\n"; exit 1; fi
	@$(DUCK_EXEC) -c "SELECT * FROM read_parquet('s3://$(DUCK_BUCKET)/$(FILE)') LIMIT $(or $(LIMIT),20);"

## Count rows in a parquet file. Usage: make duck-count FILE=path/in/bucket.parquet
duck-count:
	@if [ -z "$(FILE)" ]; then printf "$(RED)Usage: make duck-count FILE=path/in/bucket.parquet$(RESET)\n"; exit 1; fi
	@$(DUCK_EXEC) -c "SELECT COUNT(*) AS rows FROM read_parquet('s3://$(DUCK_BUCKET)/$(FILE)');"

## Describe schema of a parquet file. Usage: make duck-schema FILE=path/in/bucket.parquet
duck-schema:
	@if [ -z "$(FILE)" ]; then printf "$(RED)Usage: make duck-schema FILE=path/in/bucket.parquet$(RESET)\n"; exit 1; fi
	@$(DUCK_EXEC) -c "DESCRIBE SELECT * FROM read_parquet('s3://$(DUCK_BUCKET)/$(FILE)');"

## Show this help message
help:
	@echo ""
	@printf "$(BOLD)$(BLUE)ETL Project — MySQL → NiFi → Parquet → MinIO$(RESET)\n"
	@echo ""
	@printf "$(BOLD)Usage:$(RESET) make $(CYAN)<target>$(RESET)\n"
	@echo ""
	@printf "$(BOLD)Targets:$(RESET)\n"
	@printf "  $(CYAN)up$(RESET)       Build images and start all services (MySQL, MinIO, NiFi)\n"
	@printf "  $(CYAN)setup$(RESET)    Deploy the NiFi ETL flow via REST API $(DIM)(run after 'make up')$(RESET)\n"
	@printf "  $(CYAN)run$(RESET)      Alias for setup\n"
	@printf "  $(CYAN)down$(RESET)     Stop and remove containers $(DIM)(volumes + images preserved)$(RESET)\n"
	@printf "  $(YELLOW)clean$(RESET)      Remove containers $(DIM)(volumes + images preserved)$(RESET)\n"
	@printf "  $(RED)clean-all$(RESET)  Remove containers + volumes $(DIM)(images preserved)$(RESET)\n"
	@printf "  $(CYAN)logs$(RESET)     Tail all logs  $(DIM)|  make logs SERVICE=nifi  to filter$(RESET)\n"
	@printf "  $(CYAN)status$(RESET)   Show container health and status\n"
	@printf "  $(CYAN)ps$(RESET)       Alias for status\n"
	@printf "  $(CYAN)help$(RESET)     Show this help message\n"
	@echo ""
	@printf "$(BOLD)DuckDB:$(RESET)\n"
	@printf "  $(CYAN)duck$(RESET) / $(CYAN)duck-shell$(RESET)  Open interactive DuckDB shell (S3/MinIO preconfigured)\n"
	@printf "  $(CYAN)duck-ls$(RESET)             List parquet files in s3://$(DUCK_BUCKET)\n"
	@printf "  $(CYAN)duck-query$(RESET)          Run SQL  $(DIM)|  make duck-query Q=\"SELECT 1\"$(RESET)\n"
	@printf "  $(CYAN)duck-show$(RESET)           Preview rows  $(DIM)|  make duck-show FILE=path.parquet [LIMIT=20]$(RESET)\n"
	@printf "  $(CYAN)duck-count$(RESET)          Count rows  $(DIM)|  make duck-count FILE=path.parquet$(RESET)\n"
	@printf "  $(CYAN)duck-schema$(RESET)         Describe schema  $(DIM)|  make duck-schema FILE=path.parquet$(RESET)\n"
	@echo ""
	@printf "$(BOLD)Endpoints:$(RESET)\n"
	@printf "  $(GREEN)NiFi Canvas$(RESET)   http://localhost:8080/nifi  $(DIM)(no credentials)$(RESET)\n"
	@printf "  $(GREEN)MinIO Console$(RESET) http://localhost:9001        $(DIM)(minioadmin / minioadmin)$(RESET)\n"
	@printf "  $(GREEN)MinIO S3 API$(RESET)  http://localhost:9000\n"
	@printf "  $(GREEN)MySQL$(RESET)         localhost:3306               $(DIM)(etl_user / etl_pass  db: etl_db)$(RESET)\n"
	@echo ""
