FROM apache/spark:3.5.8

USER root

WORKDIR /app

# Copy requirements first for better layer caching
COPY ingestion/requirements.txt ./ingestion/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r ingestion/requirements.txt

ENV PATH="/opt/spark/bin:${PATH}"

COPY ingestion ./ingestion