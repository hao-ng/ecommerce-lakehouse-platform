from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr
from pyspark.sql.streaming import StreamingQuery

from common.config import BronzeConfig
from common.settings import BronzeEnvConfig


def read_from_kafka(
    spark: SparkSession, env: BronzeEnvConfig, config: BronzeConfig
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env.kafka_bootstrap_server)
        .option("subscribe", config.topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def write_stream_to_lakehouse(df: DataFrame, config: BronzeConfig) -> StreamingQuery:
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime=config.trigger)
        .start()
    )


def deserialize_avro(df: DataFrame, avro_schema: str) -> DataFrame:
    """
    Deserialize Avro-encoded Kafka messages produced by Confluent Schema Registry.

    Confluent wraps Avro payloads with a 5-byte header:
    - byte 0:    magic byte (0x00)
    - bytes 1-4: schema ID (int32)
    This function strips that header before passing the payload to from_avro().

    Args:
        df (DataFrame): DataFrame with a binary 'value' column containing Avro-encoded messages
        avro_schema (str): Avro schema in JSON string format for deserialization

    Returns:
        DataFrame: DataFrame with deserialized Avro data
    """
    return df.select(
        from_avro(expr("substring(value, 6, length(value)-5)"), avro_schema).alias(
            "data"
        )
    ).select("data.*")
