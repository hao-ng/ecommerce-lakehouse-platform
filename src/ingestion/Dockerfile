FROM apache/spark:3.5.8

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-dev --no-install-project --group ingestion

ENV PATH="/app/.venv/bin:/opt/spark/bin:${PATH}"

COPY src/ingestion ./ingestion