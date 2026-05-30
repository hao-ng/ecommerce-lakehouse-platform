import argparse
from cdc import make_cdc_merge_fn
from writers import build_batch_writer
from common.spark_session import get_spark_session
from common.config import SILVER_TABLES, BRONZE_TABLES
from common.settings import SilverEnvConfig

env = SilverEnvConfig()


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze to Silver CDC")
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        choices=SILVER_TABLES.keys(),
        help="Table name as defined in SILVER_TABLES config",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = SILVER_TABLES[args.table]
    spark = get_spark_session(f"CDC for {args.table}", env)
    merge_fn = build_batch_writer(config, make_cdc_merge_fn)
    table = BRONZE_TABLES[args.table].output_path
    query = (
        spark.readStream.format("delta")
        .load(table)
        .writeStream.foreachBatch(merge_fn)
        .option("checkpointLocation", config.checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
