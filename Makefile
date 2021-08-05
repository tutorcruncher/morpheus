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

.PHONY: build
build:
	_find src -name '*.py[co]' -delete
	_find src -name '__pycache__' -delete
	export C=$(git rev-parse HEAD)
	export BT=$(date)
	'docker build morpheus -t src --build-arg COMMIT=$C --build-arg BUILD_TIME="$BT"'
