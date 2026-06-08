.DEFAULT_GOAL := help
.PHONY: help install test test-unit test-e2e lint fmt up down clean publish

# ── versioning ────────────────────────────────────────────────────────────────
VERSION := $(shell grep '^version' pyproject.toml | head -1 | sed 's/.*= *"\(.*\)"/\1/')
DOCKER_REPO_DH := niradler/pgflows
DOCKER_REPO_GHCR := ghcr.io/niradler/pgflows

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
	@echo "  publish      Bump pyproject.toml version first, then: build+push to PyPI, Docker Hub, GHCR"

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

# ── publish ───────────────────────────────────────────────────────────────────
publish: lint
	@echo "Publishing pgflows $(VERSION)"
	uv build
	uv publish --trusted-publishing never
	docker build --no-cache -t pgflows-app:$(VERSION) .
	docker tag pgflows-app:$(VERSION) $(DOCKER_REPO_DH):$(VERSION)
	docker tag pgflows-app:$(VERSION) $(DOCKER_REPO_DH):latest
	docker tag pgflows-app:$(VERSION) $(DOCKER_REPO_GHCR):$(VERSION)
	docker tag pgflows-app:$(VERSION) $(DOCKER_REPO_GHCR):latest
	docker push $(DOCKER_REPO_DH):$(VERSION)
	docker push $(DOCKER_REPO_DH):latest
	docker push $(DOCKER_REPO_GHCR):$(VERSION)
	docker push $(DOCKER_REPO_GHCR):latest
	@echo "Published $(VERSION) to PyPI, Docker Hub, and GHCR"

# ── cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
