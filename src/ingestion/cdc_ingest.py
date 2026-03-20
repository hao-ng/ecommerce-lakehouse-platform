import logging

from common.config import CDC_TABLES
from common.logging_config import setup_logging
from common.spark_session import get_spark_session
from common.spark_streaming import (
    deserialize_avro,
    read_from_kafka,
    write_stream_to_lakehouse,
)
from schema_registry import SchemaRegistryClient
from dotenv import load_dotenv
import os

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")


def execute(spark, table, config, schema_registry_client):
    logger.info(f"Starting cdc ingestion job for {table} table")

    # Fetch the latest schema from the registry
    schema = schema_registry_client.get_schema(config["schema_subject"])
    logger.info(f"Fetched schema for subject {config['schema_subject']}")

    # Read from Kafka
    df = read_from_kafka(spark, config)

    # Deserialize Avro data
    deserialized_df = deserialize_avro(df, schema)

    # Write to Lakehouse
    return write_stream_to_lakehouse(deserialized_df, config)


def main():
    spark = get_spark_session("CDC Ingestion")
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_URL)

    for table, config in CDC_TABLES.items():
        execute(spark, table, config, schema_registry_client)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
