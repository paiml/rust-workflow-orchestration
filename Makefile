SHELL := /bin/bash
.DELETE_ON_ERROR:
.SUFFIXES:
.ONESHELL:

LINEAGE_DB ?= lineage.sqlite
SCRATCH_ROOT ?= /tmp/dag-cli-scratch

.PHONY: help init demo demo-forjar lineage clean test fmt lint lint-bash verify comply

help: ## Print available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' "$(MAKEFILE_LIST)"

init: ## Initialize the SQLite schema for state + lineage (idempotent)
	cargo run -q -p dag-cli -- lineage __init__ --lineage-db "$(LINEAGE_DB)" >/dev/null 2>&1 || true
	@test -f "$(LINEAGE_DB)" && echo "ok lineage db ready: $(LINEAGE_DB)" || { echo "fail lineage db init failed"; exit 1; }

demo: ## Run the closing demo with LocalRunner only (no forjar required)
	cargo run -q -p dag-cli --example etl_pipeline_dag --no-default-features

demo-forjar: ## Run the closing demo with both runners (forjar must be on PATH)
	@command -v forjar >/dev/null || { echo "fail forjar not on PATH; run cargo --quiet add forjar globally"; exit 1; }
	cargo run -q -p dag-cli --example etl_pipeline_dag

lineage: ## Dump the lineage of the last LocalRunner demo run as Mermaid
	cargo run -q -p dag-cli -- lineage extract --run-id local-run --lineage-db "$(LINEAGE_DB)" --mermaid

clean: ## Remove SQLite state files + scratch dirs
	rm -f "$(LINEAGE_DB)" "$(LINEAGE_DB)-shm" "$(LINEAGE_DB)-wal" state.sqlite state.sqlite-shm state.sqlite-wal || exit 1
	rm -rf "$(SCRATCH_ROOT)" /tmp/etl-pipeline-dag-* /tmp/dag-cli-scratch || exit 1
	@echo "ok cleaned"

test: ## Run the workspace test suite (LocalRunner + ForjarRunner gated by feature)
	cargo test --workspace --all-targets

fmt: ## cargo fmt --all
	cargo fmt --all

lint: ## cargo clippy with -D warnings on the entire workspace
	cargo clippy --workspace --all-targets -- -D warnings

lint-bash: ## bashrs lint on the Makefile
	bashrs lint Makefile

verify: ## fmt + clippy + tests + pv lint contracts (mirrors CI gate-matrix)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets
	pv lint contracts/

comply: ## Run pmat comply
	pmat comply
