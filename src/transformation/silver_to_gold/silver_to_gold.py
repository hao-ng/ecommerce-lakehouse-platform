from common.spark_session import get_spark_session
from common.settings import GoldEnvConfig
from common.config import GOLD_TABLES
import argparse
from transforms import run_gold_transform, TRANSFORM_REGISTRY


def parse_args():
    parser = argparse.ArgumentParser(description="Silver to Gold")
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        choices=GOLD_TABLES.keys(),
        help="Table name as defined in GOLD_TABLES config",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        required=False,
        help="Start date when create dim_date table",
        default="2016-01-01",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        required=False,
        help="End date when create dim_date table",
        default="2035-12-31",
    )
    return parser.parse_args()


env = GoldEnvConfig()


def main():
    args = parse_args()
    config = GOLD_TABLES[args.table]
    transform_fn = TRANSFORM_REGISTRY[args.table]
    spark = get_spark_session("Silver to Gold", env)

    output_df = run_gold_transform(spark, config, transform_fn, vars(args))

    output_df.write.format("delta").mode("append").save(config.gold_path)


if __name__ == "__main__":
    main()
