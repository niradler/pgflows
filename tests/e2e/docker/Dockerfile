# Combined Postgres 18 image with pg_durable (df), pgmq, AND pg_cron, for real e2e.
#
# - pg_durable (Microsoft) is built from source with pgrx (mirrors their Dockerfile).
# - pgmq 1.5.1 is pure SQL, so we copy its extension files straight from the tembo
#   image into PG18's extension dir (no .so, no ABI concern — version-agnostic SQL).
# - pg_cron is installed from the PGDG apt repo (the proper recurring scheduler;
#   pg_durable loops are NOT meant for recurring cron).

# Stage 1: build pg_durable
FROM rustlang/rust:nightly-bookworm AS builder

RUN apt-get update && apt-get install -y \
    curl gnupg2 lsb-release pkg-config libssl-dev libclang-dev clang git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y \
    postgresql-18 postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-pgrx --version 0.16.1 --locked
RUN cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config

WORKDIR /app
RUN git clone --depth 1 https://github.com/microsoft/pg_durable.git .
# http-allow-all disables the SSRF allowlist so df.http() can reach the local
# pgflows app server in e2e (development/test image only).
RUN cargo pgrx package --features http-allow-all --pg-config /usr/lib/postgresql/18/bin/pg_config

# Stage 2: pgmq source (pure SQL extension files — version-agnostic)
FROM quay.io/tembo/pg17-pgmq:latest AS pgmq_src

# Stage 3: runtime
FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y curl gnupg2 lsb-release libssl3 ca-certificates \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y postgresql-18-cron \
    && rm -rf /var/lib/apt/lists/*

# pg_durable artifacts
COPY --from=builder /app/target/release/pg_durable-pg18/usr/share/postgresql/18/extension/* /usr/share/postgresql/18/extension/
COPY --from=builder /app/target/release/pg_durable-pg18/usr/lib/postgresql/18/lib/* /usr/lib/postgresql/18/lib/

# pgmq artifacts (pure SQL — control + version scripts; copied from PG17 into PG18)
COPY --from=pgmq_src /usr/share/postgresql/17/extension/pgmq* /usr/share/postgresql/18/extension/

# Create all three extensions on first init
RUN mkdir -p /docker-entrypoint-initdb.d && \
    printf '%s\n' \
      '#!/bin/bash' \
      'set -e' \
      'psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL' \
      '  CREATE EXTENSION IF NOT EXISTS pg_durable;' \
      '  CREATE EXTENSION IF NOT EXISTS pgmq;' \
      '  CREATE EXTENSION IF NOT EXISTS pg_cron;' \
      'EOSQL' \
      > /docker-entrypoint-initdb.d/01-init-extensions.sh && \
    chmod +x /docker-entrypoint-initdb.d/01-init-extensions.sh

# pg_durable + pg_cron both need preloading; pg_cron must target the app DB.
RUN echo "shared_preload_libraries = 'pg_durable,pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "pg_durable.database = 'pgflows_test'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "pg_durable.worker_role = 'pgflows'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "pg_durable.enable_superuser_instances = on" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "cron.database_name = 'pgflows_test'" >> /usr/share/postgresql/postgresql.conf.sample

EXPOSE 5432
