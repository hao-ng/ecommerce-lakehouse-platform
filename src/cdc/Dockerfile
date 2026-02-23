FROM python:3.9-slim

WORKDIR /app

# Install curl and remove cache in single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy app code
COPY configs ./configs
COPY run.sh ./run.sh

CMD ["/bin/sh", "./run.sh", "register_connector", "configs/postgresql-cdc.json"]