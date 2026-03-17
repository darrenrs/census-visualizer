SHELL := /bin/bash

ifneq (,$(wildcard .env))
include .env
export
endif

SQL_BASE_SCRIPTS := $(sort $(wildcard pipeline/sql/0[0-5]_*.sql))
PYTHON_SCRIPTS := $(sort $(wildcard pipeline/python/[0-9][0-9]_*.py))

.PHONY: help check-env vre sql_base python sql_contract geo tiles dev dev-server dev-client pipeline all

help:
	@echo "Available targets:"
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
