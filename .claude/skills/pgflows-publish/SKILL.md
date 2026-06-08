---
name: pgflows-publish
description: Publish pgflows to PyPI, Docker Hub, and GHCR
---

# Publish pgflows

## Pre-flight
1. Merge all changes to `master` and `git pull`.
2. Bump `version` in `pyproject.toml` (patch/minor/major as appropriate).
3. Update `Dockerfile` to match the new version string.
4. Commit and push both changes.

## Publish
```bash
make publish
```

`make publish` runs lint, builds the wheel/sdist, publishes to PyPI via `uv publish`
(reads token from `~/.pypirc`), builds the Docker image, and pushes to both
`niradler/pgflows` (Docker Hub) and `ghcr.io/niradler/pgflows` (GHCR) — versioned
tag + `latest`.

## What to verify after
- PyPI: `pip index versions pgflows --no-cache-dir` shows the new version.
- Docker Hub: `docker pull niradler/pgflows:<version>` succeeds.
- GHCR: `docker pull ghcr.io/niradler/pgflows:<version>` succeeds.
