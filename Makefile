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
	python -m src.patches

