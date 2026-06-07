# Combined Postgres 17 image with BOTH pg_durable (df) and pgmq, for real e2e.
#
# - pg_durable (Microsoft) is built from source with pgrx (mirrors their Dockerfile).
# - pgmq 1.5.1 is pure SQL, so we copy its extension files straight from the tembo
#   image (no .so, no ABI concern — both are PG17/bookworm).

# Stage 1: build pg_durable
FROM rustlang/rust:nightly-bookworm AS builder

RUN apt-get update && apt-get install -y \
    curl gnupg2 lsb-release pkg-config libssl-dev libclang-dev clang git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y \
    postgresql-17 postgresql-server-dev-17 \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-pgrx --version 0.16.1 --locked
RUN cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config

WORKDIR /app
RUN git clone --depth 1 https://github.com/microsoft/pg_durable.git .
# http-allow-all disables the SSRF allowlist so df.http() can reach the local
# pgflows app server in e2e (development/test image only).
RUN cargo pgrx package --features http-allow-all --pg-config /usr/lib/postgresql/17/bin/pg_config

# Stage 2: pgmq source (pure SQL extension files)
FROM quay.io/tembo/pg17-pgmq:latest AS pgmq_src

# Stage 3: runtime
FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# pg_durable artifacts
COPY --from=builder /app/target/release/pg_durable-pg17/usr/share/postgresql/17/extension/* /usr/share/postgresql/17/extension/
COPY --from=builder /app/target/release/pg_durable-pg17/usr/lib/postgresql/17/lib/* /usr/lib/postgresql/17/lib/

# pgmq artifacts (pure SQL — control + version scripts)
COPY --from=pgmq_src /usr/share/postgresql/17/extension/pgmq* /usr/share/postgresql/17/extension/

# Create both extensions on first init
RUN mkdir -p /docker-entrypoint-initdb.d && \
    printf '%s\n' \
      '#!/bin/bash' \
      'set -e' \
      'psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL' \
      '  CREATE EXTENSION IF NOT EXISTS pg_durable;' \
      '  CREATE EXTENSION IF NOT EXISTS pgmq;' \
      'EOSQL' \
      > /docker-entrypoint-initdb.d/01-init-extensions.sh && \
    chmod +x /docker-entrypoint-initdb.d/01-init-extensions.sh

# pg_durable needs its background worker preloaded; point it at the test DB/role.
RUN echo "shared_preload_libraries = 'pg_durable'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "pg_durable.database = 'pgflows_test'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "pg_durable.worker_role = 'pgflows'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "pg_durable.enable_superuser_instances = on" >> /usr/share/postgresql/postgresql.conf.sample

EXPOSE 5432
