#!/bin/bash
python3 create_buckets.py

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
PACKAGES="${PACKAGES},org.apache.spark:spark-avro_2.12:3.5.8"
PACKAGES="${PACKAGES},io.delta:delta-spark_2.12:3.3.2"
PACKAGES="${PACKAGES},org.apache.hadoop:hadoop-aws:3.3.4"

spark-submit \
  --master spark://spark-master:7077 \
  --executor-cores 1 \
  --executor-memory 512m \
  --total-executor-cores 2 \
  --conf spark.ui.port=4040 \
  --packages ${PACKAGES} \
  clickstream_ingest.py &

sleep 60

spark-submit \
  --master spark://spark-master:7077 \
  --executor-cores 1 \
  --executor-memory 512m \
  --total-executor-cores 2 \
  --conf spark.ui.port=4041 \
  --packages ${PACKAGES} \
  cdc_ingest.py

wait