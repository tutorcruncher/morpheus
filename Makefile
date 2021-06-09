black = black -S -l 120 --target-version py38
isort = isort -w 120

PHONY: install
install:
	pip install -U setuptools pip
	pip install -r requirements.txt

.PHONY: format
format:
	$(isort) morpheus tests
	$(black) morpheus tests

.PHONY: lint
lint:
	flake8 morpheus tests
	$(isort) --check-only morpheus tests
	$(black) --check morpheus tests

.PHONY: test
test:
	pytest --cov=morpheus

.PHONY: build
build:
	_find morpheus -name '*.py[co]' -delete
	_find morpheus -name '__pycache__' -delete
	export C=$(git rev-parse HEAD)
	export BT=$(date)
	'docker build morpheus -t morpheus --build-arg COMMIT=$C --build-arg BUILD_TIME="$BT"'
