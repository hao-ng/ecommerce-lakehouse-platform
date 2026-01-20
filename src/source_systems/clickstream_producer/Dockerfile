FROM python:3.9-slim

WORKDIR /app

# Copy app code
COPY ./data_preparation.py /app/data_preparation.py
COPY ./produce.py /app/produce.py
COPY ./schemas/clickstream_schema.avsc /app/schemas/clickstream_schema.avsc
COPY ./requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install -r requirements.txt

CMD [ "python3", "-u", "produce.py", "-m", "setup", "-b", "broker:29092", "-s", "http://schema-registry:8081"]
