.PHONY: install
install:
	uv sync

.PHONY: install-dev
install-dev:
	uv sync --dev
	uv run pre-commit install

.PHONY: format
format:
	uv run --active ruff check --fix src tests
	uv run --active ruff format src tests

.PHONY: lint
lint:
	uv run --active ruff check src tests
	uv run --active ruff format --check src tests

.PHONY: test
test:
	uv run pytest tests/ --cov=src

.PHONY: run-dev
run-dev:
	uv run foxglove web

.PHONY: run-worker
run-worker:
	uv run foxglove worker

.PHONY: reset-db
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS morpheus"
	psql -h localhost -U postgres -c "CREATE DATABASE morpheus"
	psql -h localhost -U postgres -d morpheus -f src/models.sql
	foxglove patch add_aggregation_view --live --patch-args ':'
	foxglove patch add_spam_status_and_reason_to_messages --live --patch-args ':'

# Run a specific patch by name:
#   make run_patch PATCH=patch_function_name
#   make run_patch PATCH=patch_function_name LIVE=1
.PHONY: run_patch
run_patch:
	foxglove patch $(PATCH) $(if $(LIVE),--live,) --patch-args ':'
