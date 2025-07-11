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
	# Run patches in specific order for schema migration
	python -m foxglove.db.patches performance_step1 --live  # Create initial structure
	python -m foxglove.db.patches performance_step2 --live  # Create indexes (direct)
	python -m foxglove.db.patches performance_step3 --live  # Update data (direct)
	python -m foxglove.db.patches performance_step4 --live  # Finalize schema changes
	# Run any remaining patches
	python -m src.patches

.PHONY: run_patch
run_patch:
	python -m src.patches $(PATCH)

