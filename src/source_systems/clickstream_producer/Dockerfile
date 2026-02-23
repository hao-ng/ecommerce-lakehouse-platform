FROM python:3.9-slim

WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

COPY clickstream_producer/requirements.txt ./clickstream_producer/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r clickstream_producer/requirements.txt

# Copy app code
COPY utils ./utils
COPY clickstream_producer ./clickstream_producer

CMD [ "python3", "-m", "clickstream_producer.produce",\
    "-m", "setup",\
    "-b", "broker:29092",\
    "-s", "http://schema-registry:8081",\
    "-c", "./clickstream_producer/schemas/clickstream_schema.avsc"\
]