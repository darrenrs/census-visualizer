SHELL := /bin/bash

ifneq (,$(wildcard .env))
include .env
export
endif

SQL_BASE_SCRIPTS := $(sort $(wildcard pipeline/sql/0[0-5]_*.sql))
PYTHON_SCRIPTS := $(sort $(wildcard pipeline/python/[0-9][0-9]_*.py))

.PHONY: help check-env doctor vre sql_base python sql_contract geo tiles dev dev-server dev-client pipeline all

help:
	@echo "Available targets:"
	@echo "  make doctor        - Check env vars, commands, and DB connectivity"
	@echo "  make vre           - Download VRE tables"
	@echo "  make sql_base      - Run SQL base pipeline (00-05)"
	@echo "  make python        - Run Python derived pipeline (06-09)"
	@echo "  make sql_contract  - Run API contract SQL (10)"
	@echo "  make geo           - Build GeoJSON assets"
	@echo "  make tiles         - Build PMTiles assets"
	@echo "  make dev-server    - Run API server in dev mode"
	@echo "  make dev-client    - Run frontend in dev mode"
	@echo "  make dev           - Run API server + frontend in dev mode"
	@echo "  make pipeline      - Run full data pipeline"
	@echo "  make all           - Run full pipeline + geo + tiles"

check-env:
	@test -n "$(DATABASE_URL)" || (echo "DATABASE_URL is required (set it in .env or shell)." && exit 1)
	@test -n "$(VINTAGE)" || (echo "VINTAGE is required (set it in .env or shell)." && exit 1)

doctor: check-env
	@echo "==> Environment"
	@echo "DATABASE_URL=$$(printf '%s' "$(DATABASE_URL)" | sed 's#://.*@#://***@#')"
	@echo "VINTAGE=$(VINTAGE)"
	@echo
	@echo "==> Core commands"
	@for cmd in psql python3 node npm curl unzip ogrinfo ogr2ogr; do \
		if command -v "$$cmd" >/dev/null 2>&1; then \
			echo "[ok] $$cmd -> $$(command -v "$$cmd")"; \
		else \
			echo "[missing] $$cmd"; \
			exit 1; \
		fi; \
	done
	@echo
	@echo "==> Optional commands"
	@for cmd in tippecanoe pmtiles; do \
		if command -v "$$cmd" >/dev/null 2>&1; then \
			echo "[ok] $$cmd -> $$(command -v "$$cmd")"; \
		else \
			echo "[warn] $$cmd not found (only needed for tile build)"; \
		fi; \
	done
	@echo
	@echo "==> Tool versions"
	@echo "python3: $$(python3 --version 2>/dev/null)"
	@echo "node: $$(node --version 2>/dev/null)"
	@echo "npm: $$(npm --version 2>/dev/null)"
	@echo
	@echo "==> Database connectivity"
	@psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -c "SELECT 1;" >/dev/null
	@echo "[ok] Connected to database"
	@psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -tA -c "SELECT 1 FROM pg_namespace WHERE nspname = '$(VINTAGE)';" | grep -q '^1$$'
	@echo "[ok] Found source schema: $(VINTAGE)"
	@psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -tA -c "SELECT 1 FROM pg_namespace WHERE nspname = 'viz';" | grep -q '^1$$' \
		&& echo "[ok] Found schema: viz" \
		|| echo "[info] Schema 'viz' not present yet (it will be created by the pipeline)"
	@psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -tA -c "SELECT 1 FROM pg_namespace WHERE nspname = 'api';" | grep -q '^1$$' \
		&& echo "[ok] Found schema: api" \
		|| echo "[info] Schema 'api' not present yet (it will be created by the pipeline)"

vre: check-env
	@echo "==> Downloading VRE tables"
	python3 pipeline/vre/download_vre_tables.py

sql_base: check-env
	@echo "==> Running SQL base scripts"
	@for f in $(SQL_BASE_SCRIPTS); do \
		echo " -> $$f"; \
		psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -v vintage="$(VINTAGE)" -f "$$f"; \
	done

python: check-env
	@echo "==> Running Python derived scripts"
	@for f in $(PYTHON_SCRIPTS); do \
		echo " -> $$f"; \
		python3 "$$f"; \
	done

sql_contract: check-env
	@echo "==> Running API contract SQL"
	psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -v vintage="$(VINTAGE)" -f pipeline/sql/10_contract_api.sql

geo: check-env
	@echo "==> Building GeoJSON assets"
	bash pipeline/geo/build_geo.sh

tiles: check-env
	@echo "==> Building PMTiles assets"
	bash pipeline/geo/build_tiles.sh

dev-server:
	@echo "==> Starting API server in dev mode"
	cd web/server && npm run dev

dev-client:
	@echo "==> Starting frontend in dev mode"
	cd web/client && npm run dev

dev:
	@echo "==> Starting API server + frontend in dev mode (Ctrl-C to stop)"
	@trap 'kill 0' INT TERM EXIT; \
		(cd web/server && npm run dev) & \
		(cd web/client && npm run dev) & \
		wait

pipeline: vre sql_base python sql_contract

all: pipeline geo tiles
