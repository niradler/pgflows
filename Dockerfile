FROM python:3.13-slim

WORKDIR /app

COPY examples ./examples

RUN pip install --no-cache-dir "pgflows[fastapi]==0.1.3"

EXPOSE 8000

CMD ["uvicorn", "examples.server:app", "--host", "0.0.0.0", "--port", "8000"]
