.PHONY: install
install:
	uv sync

.PHONY: format
format:
	uv run ruff format app tests
	uv run ruff check --fix app tests

.PHONY: lint
lint:
	uv run ruff check app tests
	uv run ruff format --check app tests
	uv run ty check app

.PHONY: test
test:
	uv run pytest tests/ --cov=app

.PHONY: reset-db
reset-db:
	psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS morpheus"
	psql -h localhost -U postgres -c "CREATE DATABASE morpheus"
	uv run python -c "from app.core.database import create_db_and_tables; create_db_and_tables()"

.PHONY: dev
dev:
	uv run uvicorn app.main:app --reload

.PHONY: worker
worker:
	uv run celery -A app.worker worker --loglevel=info

.PHONY: beat
beat:
	uv run celery -A app.worker beat --loglevel=info
