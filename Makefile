check: lint tests

lint:
	uv run ruff check
	uv run mypy .
	uv run basedpyright .

tests:
	uv run pytest

docs:
	uv run --group docs mkdocs build

.PHONY: check lint tests docs
