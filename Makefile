black = black -S -l 120 --target-version py38
isort = isort -w 120

PHONY: install
install:
	pip install -r requirements.txt
	pip install -r tests/requirements.txt

.PHONY: format
format:
	$(isort) src tests
	$(black) src tests

.PHONY: lint
lint:
	flake8 src tests
	$(isort) --check-only src tests
	$(black) --check src tests

.PHONY: test
test:
	pytest tests/ --cov=src

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

