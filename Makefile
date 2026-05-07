ENV_FILE := $(if $(wildcard .env.local),.env.local,.env)
COMPOSE  := docker compose --env-file $(ENV_FILE)
SERVICE  ?=

-include .env
-include .env.local
export

# ANSI color codes
BOLD   := \033[1m
RESET  := \033[0m
GREEN  := \033[0;32m
CYAN   := \033[0;36m
YELLOW := \033[0;33m
RED    := \033[0;31m
BLUE   := \033[0;34m
DIM    := \033[2m

.PHONY: up setup run down clean clean-all logs status ps help env init-user \
        duck duck-shell duck-ls duck-query duck-show duck-count duck-schema

DUCK_EXEC   := $(COMPOSE) exec -T duckdb duckdb -init /workspace/init.sql
DUCK_BUCKET := $(or $(MINIO_BUCKET),etl-data)

## Bootstrap .env from .env.example if missing
env:
	@if [ ! -f .env ]; then \
	  cp .env.example .env; \
	  printf "$(GREEN).env created from .env.example — edit before running make up$(RESET)\n"; \
	else \
	  printf "$(DIM).env already exists$(RESET)\n"; \
	fi

## Interactive per-user setup: scan for free ports and write .env.local
init-user:
	@if [ -f .env.local ]; then \
	  printf "$(YELLOW).env.local already exists.$(RESET)\n"; \
	  printf "$(DIM)To regenerate: rm .env.local && make init-user$(RESET)\n"; \
	  exit 0; \
	fi; \
	\
	printf "$(CYAN)$(BOLD)ETL Project — per-user setup$(RESET)\n\n"; \
	\
	printf "Project name [$$USER]: "; read project; \
	[ -z "$$project" ] && project=$$USER; \
	\
	printf "MySQL root password [root]: "; read mysql_root_pass; \
	[ -z "$$mysql_root_pass" ] && mysql_root_pass=root; \
	\
	printf "MySQL user password [etl_pass]: "; read mysql_pass; \
	[ -z "$$mysql_pass" ] && mysql_pass=etl_pass; \
	\
	printf "MinIO root password [minioadmin]: "; read minio_pass; \
	[ -z "$$minio_pass" ] && minio_pass=minioadmin; \
	\
	printf "MinIO bucket [etl-data]: "; read minio_bucket; \
	[ -z "$$minio_bucket" ] && minio_bucket=etl-data; \
	\
	printf "\n$(DIM)Scanning for available ports from 20000...$(RESET)\n"; \
	port_in_use() { lsof -nP -i TCP:$$1 2>/dev/null | grep -q LISTEN; }; \
	next_port() { p=$$1; while port_in_use $$p; do p=$$((p + 1)); done; echo $$p; }; \
	\
	mysql_port=$$(next_port 20000); \
	nifi_port=$$(next_port $$((mysql_port + 1))); \
	minio_port=$$(next_port $$((nifi_port + 1))); \
	minio_console_port=$$(next_port $$((minio_port + 1))); \
	\
	printf "COMPOSE_PROJECT_NAME=$${project}_etl\n"         > .env.local; \
	printf "\n# Ports (host-side, auto-assigned from 20000)\n" >> .env.local; \
	printf "MYSQL_PORT=$$mysql_port\n"                      >> .env.local; \
	printf "NIFI_PORT=$$nifi_port\n"                        >> .env.local; \
	printf "MINIO_PORT=$$minio_port\n"                      >> .env.local; \
	printf "MINIO_CONSOLE_PORT=$$minio_console_port\n"      >> .env.local; \
	printf "\n# MySQL\n"                                    >> .env.local; \
	printf "MYSQL_ROOT_PASSWORD=$$mysql_root_pass\n"        >> .env.local; \
	printf "MYSQL_DATABASE=etl_db\n"                        >> .env.local; \
	printf "MYSQL_USER=etl_user\n"                          >> .env.local; \
	printf "MYSQL_PASSWORD=$$mysql_pass\n"                  >> .env.local; \
	printf "\n# MinIO\n"                                    >> .env.local; \
	printf "MINIO_ROOT_USER=minioadmin\n"                   >> .env.local; \
	printf "MINIO_ROOT_PASSWORD=$$minio_pass\n"             >> .env.local; \
	printf "MINIO_BUCKET=$$minio_bucket\n"                  >> .env.local; \
	\
	printf "\n$(GREEN)$(BOLD).env.local written:$(RESET)\n"; \
	printf "  $(CYAN)%-24s$(RESET) %s\n" "COMPOSE_PROJECT_NAME" "$${project}_etl"; \
	printf "  $(CYAN)%-24s$(RESET) %s\n" "MYSQL_PORT"           "$$mysql_port"; \
	printf "  $(CYAN)%-24s$(RESET) %s\n" "NIFI_PORT"            "$$nifi_port"; \
	printf "  $(CYAN)%-24s$(RESET) %s\n" "MINIO_PORT"           "$$minio_port"; \
	printf "  $(CYAN)%-24s$(RESET) %s\n" "MINIO_CONSOLE_PORT"   "$$minio_console_port"; \
	printf "  $(CYAN)%-24s$(RESET) %s\n" "MINIO_BUCKET"         "$$minio_bucket"; \
	printf "\n$(DIM)Run 'make up' to start the stack.$(RESET)\n"

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
	@printf "$(BOLD)Setup:$(RESET)\n"
	@printf "  $(CYAN)init-user$(RESET)  Interactive setup: scan free ports, write .env.local\n"
	@printf "  $(CYAN)env$(RESET)        Bootstrap .env from .env.example if missing\n"
	@echo ""
	@printf "$(BOLD)Targets:$(RESET)\n"
	@printf "  $(CYAN)up$(RESET)         Build images and start all services (MySQL, MinIO, NiFi)\n"
	@printf "  $(CYAN)setup$(RESET)      Deploy the NiFi ETL flow via REST API $(DIM)(run after 'make up')$(RESET)\n"
	@printf "  $(CYAN)run$(RESET)        Alias for setup\n"
	@printf "  $(CYAN)down$(RESET)       Stop and remove containers $(DIM)(volumes + images preserved)$(RESET)\n"
	@printf "  $(YELLOW)clean$(RESET)      Remove containers $(DIM)(volumes + images preserved)$(RESET)\n"
	@printf "  $(RED)clean-all$(RESET)  Remove containers + volumes $(DIM)(images preserved)$(RESET)\n"
	@printf "  $(CYAN)logs$(RESET)       Tail all logs  $(DIM)|  make logs SERVICE=nifi  to filter$(RESET)\n"
	@printf "  $(CYAN)status$(RESET)     Show container health and status\n"
	@printf "  $(CYAN)ps$(RESET)         Alias for status\n"
	@printf "  $(CYAN)help$(RESET)       Show this help message\n"
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
	@printf "  $(GREEN)NiFi Canvas$(RESET)   http://localhost:$(or $(NIFI_PORT),8080)/nifi  $(DIM)(no credentials)$(RESET)\n"
	@printf "  $(GREEN)MinIO Console$(RESET) http://localhost:$(or $(MINIO_CONSOLE_PORT),9001)        $(DIM)(minioadmin / minioadmin)$(RESET)\n"
	@printf "  $(GREEN)MinIO S3 API$(RESET)  http://localhost:$(or $(MINIO_PORT),9000)\n"
	@printf "  $(GREEN)MySQL$(RESET)         localhost:$(or $(MYSQL_PORT),3306)               $(DIM)(etl_user / etl_pass  db: etl_db)$(RESET)\n"
	@echo ""
