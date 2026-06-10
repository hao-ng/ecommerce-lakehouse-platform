from clickstream import build_clickstream_append_writer
from writers import build_batch_writer

from common.config import BRONZE_APPEND_TABLES, SILVER_TABLES
from common.settings import SilverEnvConfig
from common.spark_session import get_spark_session

env = SilverEnvConfig()


def main():
    spark = get_spark_session("Bronze to Silver Clickstream", env)
    config = SILVER_TABLES["clickstream"]
    table = BRONZE_APPEND_TABLES["clickstream"].output_path
    append_fn = build_batch_writer(config, build_clickstream_append_writer)
    query = (
        spark.readStream.format("delta")
        .load(table)
        .writeStream.foreachBatch(append_fn)
        .option("checkpointLocation", config.checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
