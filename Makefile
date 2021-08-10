black = black -S -l 120 --target-version py38
isort = isort -w 120

PHONY: install
install:
	pip install -U setuptools pip
	pip install -r requirements.txt

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
