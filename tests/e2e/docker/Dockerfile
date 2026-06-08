# syntax=docker/dockerfile:1.7
#
# Postgres 18 + pg_durable (df) + pgmq + pg_cron — e2e test image.
# pg_durable: downloads a GitHub release binary if one exists; builds from source otherwise.
# pgmq:       pure SQL — copied from the tembo image (ABI-agnostic across PG versions).
# pg_cron:    installed from the PGDG apt repo.

ARG CARGO_PGRX_VERSION=0.16.1
ARG PG_DURABLE_GIT_REF=main
ARG PGMQ_IMAGE=quay.io/tembo/pg17-pgmq:latest

# ── stage 1: pg_durable ───────────────────────────────────────────────────────
FROM rustlang/rust:nightly-bookworm AS pg_durable_builder

ARG CARGO_PGRX_VERSION
ARG PG_DURABLE_GIT_REF

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev libclang-dev clang git curl gnupg2 lsb-release \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] \
       http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       postgresql-18 postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cargo/registry \
    cargo install cargo-pgrx --version "${CARGO_PGRX_VERSION}" --locked

RUN cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config

WORKDIR /build

# Try a pre-built release binary first (much faster); fall back to source build.
# http-allow-all disables the SSRF allowlist so df.http() reaches the local
# pgflows app server in e2e (dev/test image only — never use in production).
# Artifacts are normalised to /artifacts/{share,lib} regardless of which path ran.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/build/target \
    set -ex; \
    ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/'); \
    DOWNLOAD_URL="https://github.com/microsoft/pg_durable/releases/latest/download/pg_durable-pg18-linux-${ARCH}.tar.gz"; \
    mkdir -p /artifacts/share /artifacts/lib; \
    if curl -fsSL "${DOWNLOAD_URL}" -o /tmp/pg_durable.tar.gz 2>/dev/null; then \
        echo "==> pg_durable: using pre-built binary"; \
        tar -xzf /tmp/pg_durable.tar.gz -C /tmp; \
        find /tmp -name 'pg_durable*' -path '*/extension/*' -exec cp {} /artifacts/share/ \; ; \
        find /tmp -name 'pg_durable*.so'                    -exec cp {} /artifacts/lib/ \; ; \
    else \
        echo "==> pg_durable: building from source (ref: ${PG_DURABLE_GIT_REF})"; \
        git clone --depth 1 https://github.com/microsoft/pg_durable.git .; \
        cargo pgrx package \
            --features http-allow-all \
            --pg-config /usr/lib/postgresql/18/bin/pg_config; \
        cp target/release/pg_durable-pg18/usr/share/postgresql/18/extension/* /artifacts/share/; \
        cp target/release/pg_durable-pg18/usr/lib/postgresql/18/lib/*          /artifacts/lib/; \
    fi

# ── stage 2: pgmq SQL files ───────────────────────────────────────────────────
FROM ${PGMQ_IMAGE} AS pgmq_src

# ── stage 3: runtime ──────────────────────────────────────────────────────────
FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg2 lsb-release ca-certificates libssl3 \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] \
       http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-18-cron \
    && rm -rf /var/lib/apt/lists/*

COPY --from=pg_durable_builder /artifacts/share/ /usr/share/postgresql/18/extension/
COPY --from=pg_durable_builder /artifacts/lib/   /usr/lib/postgresql/18/lib/

# pgmq is pure SQL (no .so); glob matches whether the source image is pg17 or pg18.
COPY --from=pgmq_src /usr/share/postgresql/*/extension/pgmq* /usr/share/postgresql/18/extension/

# Extensions are created once on first container start via the postgres initdb hook.
COPY 01-extensions.sql /docker-entrypoint-initdb.d/

# pg_durable and pg_cron require shared_preload_libraries before any extension is
# created; baking config into postgresql.conf.sample makes the image self-contained.
COPY pgflows.conf /tmp/pgflows.conf
RUN cat /tmp/pgflows.conf >> /usr/share/postgresql/postgresql.conf.sample \
    && rm /tmp/pgflows.conf

EXPOSE 5432
