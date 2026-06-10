FROM python:3.9-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-dev --no-install-project --group kafka-producer

# Copy app code
COPY src/source_systems/utils ./utils
COPY src/source_systems/clickstream_producer ./clickstream_producer

ENV PATH="/app/.venv/bin:$PATH"

CMD [ "python3", "-m", "clickstream_producer.produce",\
    "-m", "setup",\
    "-b", "broker:29092",\
    "-s", "http://schema-registry:8081",\
    "-c", "./clickstream_producer/schemas/clickstream_schema.avsc"\
]