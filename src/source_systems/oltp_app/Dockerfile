FROM python:3.9-slim

WORKDIR /app

# Install curl and remove cache in single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY oltp_app/requirements.txt ./oltp_app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r oltp_app/requirements.txt

# Copy app code
COPY utils ./utils
COPY oltp_app ./oltp_app

CMD ["/bin/sh", "./oltp_app/run.sh"]