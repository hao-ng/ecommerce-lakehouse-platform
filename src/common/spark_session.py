from pyspark.sql import SparkSession
from common.settings import BaseEnvConfig


def get_spark_session(app_name: str, env: BaseEnvConfig) -> SparkSession:
    """Create and return a SparkSession configured for Delta Lake and MinIO.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", env.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", env.minio_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", env.minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.hive.metastore.uris", env.hive_metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )
