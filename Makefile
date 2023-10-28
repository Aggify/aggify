# Makefile for aggify

# Environment variables
PYTHON = python

.PHONY: init build clean

init:
	$(PYTHON) -m pip install build twine

build:
	$(PYTHON) -m build

clean:
	rm -rf dist/

# Publish to PyPI using twine
publish:
	twine check dist/*
	twine upload dist/*
