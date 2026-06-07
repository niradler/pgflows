# pgflows Python app image — built from local source with the FastAPI extra.
# Pairs with the df+pgmq Postgres image (tests/e2e/docker) via docker-compose.full.yml.
FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY examples ./examples

RUN pip install --no-cache-dir ".[fastapi]"

EXPOSE 8000

CMD ["uvicorn", "examples.server:app", "--host", "0.0.0.0", "--port", "8000"]
