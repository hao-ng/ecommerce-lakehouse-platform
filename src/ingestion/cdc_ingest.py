import logging

from pyspark.sql import SparkSession
from schema_registry import SchemaRegistryClient
from spark_streaming import (
    deserialize_avro,
    read_from_kafka,
    write_stream_to_lakehouse,
)

from common.config import BRONZE_CDC_TABLES, BronzeConfig
from common.logging_config import setup_logging
from common.settings import BronzeEnvConfig
from common.spark_session import get_spark_session

setup_logging()
logger = logging.getLogger(__name__)
env = BronzeEnvConfig()


def execute(
    spark: SparkSession,
    env: BronzeEnvConfig,
    config: BronzeConfig,
    schema_registry_client: SchemaRegistryClient,
):
    logger.info(f"Starting cdc ingestion job for {config.name} table")

    # Fetch the latest schema from the registry
    schema = schema_registry_client.get_schema(config.schema_subject)
    logger.info(f"Fetched schema for subject {config.schema_subject}")

    # Read from Kafka
    df = read_from_kafka(spark, env, config)

    # Deserialize Avro data
    deserialized_df = deserialize_avro(df, schema)

    # Write to Lakehouse
    return write_stream_to_lakehouse(deserialized_df, config)


def main():
    spark = get_spark_session("CDC Ingestion", env)
    schema_registry_client = SchemaRegistryClient(env.schema_registry_url)

    for _, config in BRONZE_CDC_TABLES.items():
        execute(spark, env, config, schema_registry_client)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
