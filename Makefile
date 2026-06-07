.DEFAULT_GOAL := help
.PHONY: help install test test-unit test-e2e lint fmt up down clean

# ── environment ──────────────────────────────────────────────────────────────
PGFLOWS_TEST_DSN ?= postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test

# ── help ─────────────────────────────────────────────────────────────────────
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  install      Install all dependencies with uv sync"
	@echo "  up           Start Docker Compose (Postgres + pgmq)"
	@echo "  down         Stop Docker Compose"
	@echo "  test         Run full test suite (unit + e2e)"
	@echo "  test-unit    Run unit tests only"
	@echo "  test-e2e     Run e2e tests only (requires Postgres running)"
	@echo "  lint         Check code with ruff"
	@echo "  fmt          Auto-fix lint issues with ruff"
	@echo "  clean        Remove build artifacts and __pycache__"

# ── setup ─────────────────────────────────────────────────────────────────────
install:
	uv sync

# ── docker ────────────────────────────────────────────────────────────────────
up:
	docker compose up -d --wait

down:
	docker compose down

# ── tests ─────────────────────────────────────────────────────────────────────
test: up
	PGFLOWS_TEST_DSN=$(PGFLOWS_TEST_DSN) uv run pytest tests/ -v

test-unit:
	uv run pytest tests/unit/ -v

test-e2e: up
	PGFLOWS_TEST_DSN=$(PGFLOWS_TEST_DSN) uv run pytest tests/e2e/ -v

# ── code quality ──────────────────────────────────────────────────────────────
lint:
	uv run ruff check src/ tests/

fmt:
	uv run ruff check src/ tests/ --fix

# ── cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
