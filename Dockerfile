FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY pyproject.toml .
COPY uv.lock .
RUN pip install uv && uv sync --no-dev



CMD ["uv", "run", "compute_recent_form.py"]
