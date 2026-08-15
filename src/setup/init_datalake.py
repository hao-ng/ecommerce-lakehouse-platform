import logging

from minio import Minio
from pyspark.sql import SparkSession

from common.config import BronzeConfig, GoldConfig, SilverConfig
from common.logging_config import setup_logging
from common.settings import BaseEnvConfig
from common.spark_session import get_spark_session

setup_logging()
logger = logging.getLogger(__name__)

BUCKET_NAME = "datalake"
DATABASES = [BronzeConfig.layer, SilverConfig.layer, GoldConfig.layer]


def create_bucket(env: BaseEnvConfig, bucket_name: str) -> None:
    client = Minio(
        env.minio_endpoint.removeprefix("http://"),
        access_key=env.minio_access_key,
        secret_key=env.minio_secret_key,
        secure=False,
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")
    else:
        logger.info(f"Bucket already exists: {bucket_name}")


def create_databases(
    spark: SparkSession,
    bucket_name: str,
    databases: list[str],
) -> None:
    for database in databases:
        spark.sql(
            f"""
            CREATE DATABASE IF NOT EXISTS `{database}`
            LOCATION 's3a://{bucket_name}/{database}'
            """
        )


def main():
    env = BaseEnvConfig()

    logger.info("Initializing MinIO bucket")
    create_bucket(env, BUCKET_NAME)

    logger.info("Starting Spark")
    spark = get_spark_session("Initialize lakehouse", env)

    try:
        logger.info("Initializing databases")
        create_databases(spark, BUCKET_NAME, DATABASES)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
