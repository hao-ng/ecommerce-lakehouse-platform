import argparse

from pyspark.sql import SparkSession, DataFrame
from transforms import TRANSFORM_REGISTRY, run_gold_transform

from common.config import GOLD_TABLES, GoldConfig
from common.settings import GoldEnvConfig
from common.spark_session import get_spark_session


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


def overwrite_and_register_gold_table(
    spark: SparkSession, df: DataFrame, config: GoldConfig
) -> None:
    (df.write.format("delta").mode("overwrite").save(config.table_path))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.qualified_name}
        USING DELTA
        LOCATION '{config.table_path}'
    """)


def main():
    args = parse_args()
    config = GOLD_TABLES[args.table]
    transform_fn = TRANSFORM_REGISTRY[args.table]
    spark = get_spark_session("Silver to Gold", env)

    output_df = run_gold_transform(spark, config, transform_fn, vars(args))

    overwrite_and_register_gold_table(spark, output_df, config)


if __name__ == "__main__":
    main()
