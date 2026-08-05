.PHONY: format format-check lint type-check test quality

format:
	python -m ruff format src tests

format-check:
	python -m ruff format --check src tests

lint:
	python -m ruff check src tests

type-check:
	python -m mypy src

test:
	python -m pytest

quality: format-check lint type-check test
