PYTHON ?= python

.PHONY: install lint test format smoke

install:
	$(PYTHON) -m pip install -e .[dev]

lint:
	$(PYTHON) -m ruff check src tests scripts

format:
	$(PYTHON) -m ruff check --fix src tests scripts

test:
	$(PYTHON) -m pytest -q

smoke:
	mdi run-all --env staging --smoke
