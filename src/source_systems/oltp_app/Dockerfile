FROM python:3.9-slim

WORKDIR /app

# Copy app code
COPY utils /app/utils
COPY oltp_app /app/oltp_app

# Install dependencies
RUN pip install -r oltp_app/requirements.txt

CMD ["/bin/sh", "./oltp_app/run.sh"]
