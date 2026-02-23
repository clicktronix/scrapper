.PHONY: install dev down test lint lint-fix typecheck

install:
	uv sync

dev:
	uv run python src/main.py

down:
	@lsof -ti :$(or $(PORT),8001) | xargs -r kill && echo "Stopped process on port $(or $(PORT),8001)" || echo "No process on port $(or $(PORT),8001)"

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/ tests/

lint-fix:
	uv run ruff check --fix src/ tests/

typecheck:
	uv run pyright src/
