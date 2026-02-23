#!/bin/sh
set -e

wait_for() {
  url=$1
  name=$2

  echo "Waiting for $name..."

  until curl -sf "$url" > /dev/null; do
    sleep 2
  done

  echo "$name is ready!"
}

wait_for http://debezium:8083/connectors "Debezium"

python3 -m oltp_app.create_tables
python3 -m oltp_app.insert_tables