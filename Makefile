PYTHON ?= python
MONGO_PORT ?= 27018

.PHONY: help install mongo mongo-stop test test-all lint typecheck docs docs-serve bench build clean publish

help:
	@grep -E '^[a-z-]+:' Makefile | cut -d: -f1 | tr '\n' ' '; echo

install:
	$(PYTHON) -m pip install -e . pytest pytest-asyncio pytest-cov hypothesis httpx ruff mypy "pydantic>=2" fastapi grpcio protobuf "django>=4.2" mkdocs mkdocs-material "mkdocstrings[python]"

mongo:        ## throw-away single-node replica set on :$(MONGO_PORT) (needed for the transaction tests)
	-docker rm -f aggify-test-mongo >/dev/null 2>&1
	docker run -d --name aggify-test-mongo -p $(MONGO_PORT):27017 mongo:7 --replSet rs0 --bind_ip_all
	@sleep 4
	docker exec aggify-test-mongo mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'

mongo-stop:
	-docker rm -f aggify-test-mongo

test:
	AGGIFY_TEST_MONGO_URI="mongodb://localhost:$(MONGO_PORT)/?directConnection=true" $(PYTHON) -m pytest

cov:
	AGGIFY_TEST_MONGO_URI="mongodb://localhost:$(MONGO_PORT)/?directConnection=true" $(PYTHON) -m pytest --cov=aggify --cov-report=term-missing

test-all:
	tox

lint:
	ruff check aggify tests

typecheck:
	mypy aggify

docs:
	mkdocs build --strict

docs-serve:
	mkdocs serve

bench:
	$(PYTHON) benchmarks/bench.py "mongodb://localhost:$(MONGO_PORT)/?directConnection=true"

build:
	$(PYTHON) -m build

clean:
	rm -rf dist build site .pytest_cache .mypy_cache .ruff_cache

publish: clean build
	twine check dist/*
	twine upload dist/*
