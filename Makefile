
SHELL = /bin/bash
.DEFAULT_GOAL = format
pkg_src = inbound
tests_src = tests
ifeq ($(OS),Windows_NT)
	BIN = Scripts
else
	BIN = bin
endif

VENV = ./.venv/$(BIN)/activate
PY = ./.venv/$(BIN)/python -m  

isort = $(PY) isort -rc $(pkg_src) $(tests_src)
black = $(PY) black $(pkg_src) $(tests_src)
flake8 = $(PY) flake8 $(pkg_src) $(tests_src)
mypy_base = $(PY) mypy --show-error-codes
mypy = $(mypy_base) $(pkg_src)
mypy_tests = $(mypy_base) $(pkg_src) $(tests_src)

.PHONY: build
build:
	rm -r -f dist
	${PY} build --wheel --outdir dist/
	twine check ./dist/*

.PHONY: install ## install requirements in virtal env 
install:
	python3.9 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		poetry env use ./.venv/bin/python
		poetry install


.PHONY: all  ## Perform the most common development-time rules
all: format lint mypy test

.PHONY: format  ## Auto-format the source code (isort, black)
format:
	$(isort)
	$(black)

.PHONY: test  ## Run tests
test:
	poetry run pytest ./tests/unit --cov=$(pkg_src)

.PHONY: dockerup  ## Run tests
dockerup:
	docker-compose -f ./tests/e2e/docker-compose.yml up 

.PHONY: dockerdown  ## Run tests
dockerdown:
	docker-compose -f ./tests/e2e/docker-compose.yml down 

.PHONY: e2e  ## Run tests
e2e:
	poetry run pytest ./tests/e2e/postgres/
	poetry run pytest ./tests/e2e/oracle/
	poetry run pytest ./tests/e2e/snowflake/
	poetry run pytest ./tests/e2e/metadata/



.PHONY: testcov  ## Run tests, generate a coverage report, and open in browser
testcov:
	pytest ./tests/unit --cov=$(pkg_src)
	@echo "building coverage html"
	@coverage html
	@echo "opening coverage html in browser"
	@open htmlcov/index.html
