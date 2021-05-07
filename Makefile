black = black -S -l 120 --target-version py37 morpheus tests
isort = isort -w 120 morpheus tests

PHONY: install
install:
	pip install -U setuptools pip
	pip install -r requirements.txt

PHONY: format:
- +isort
- +black

lint:
- flake8 morpheus tests
- +isort -- --check-only
- +black -- --check
- ./tests/check_debug.sh

test:
- pytest --cov=morpheus

testcov:
- +test
- coverage html

all:
- +testcov
- +lint

build:
- _find morpheus -name '*.py[co]' -delete
- _find morpheus -name '__pycache__' -delete
- export C=$(git rev-parse HEAD)
- export BT=$(date)
- 'docker build morpheus -t morpheus --build-arg COMMIT=$C --build-arg BUILD_TIME="$BT"'

docker-dev:
- +build
- docker build mandrill-mock -t mandrill-mock
- _echo ================================================================================
- _echo running locally for development and testing
- _echo You'll want to run docker-logs in another window see what's going on
- _echo ================================================================================
- _echo
- _echo running docker compose...
- docker-compose up -d

clean:
- rm -rf `find . -name __pycache__`
- rm -f `find . -type f -name '*.py[co]' `
- rm -f `find . -type f -name '*~' `
- rm -f `find . -type f -name '.*~' `
- rm -rf .cache
- rm -rf htmlcov
- rm -rf *.egg-info
- rm -f .coverage
- rm -f .coverage.*
- rm -rf build
