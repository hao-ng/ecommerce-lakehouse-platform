FROM python:3.9-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-dev --no-install-project --group oltp-app

# Copy app code
COPY src/source_systems/utils ./utils
COPY src/source_systems/oltp_app ./oltp_app

ENV PATH="/app/.venv/bin:$PATH"

CMD ["/bin/sh", "./oltp_app/run.sh"]