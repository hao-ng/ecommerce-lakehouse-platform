FROM python:3.9-slim

WORKDIR /app

# Copy app code
COPY utils /app/utils
COPY clickstream_producer /app/clickstream_producer

# Install dependencies
RUN pip install -r clickstream_producer/requirements.txt

CMD [ "python3", "-u", "-m", "clickstream_producer.produce",\
    "-m", "setup",\
    "-b", "broker:29092",\
    "-s", "http://schema-registry:8081",\
    "-c", "./clickstream_producer/schemas/clickstream_schema.avsc"\
]