from typing import Callable, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, row_number
from pyspark.sql.window import Window
from transforms import build_transform

from common.config import SilverConfig


def prepare_cdc_batch_for_merge(
    df: DataFrame,
    dedup_key_cols: list[str],
    transform_fn: Optional[Callable] = None,
) -> DataFrame:
    df = df.select(
        coalesce(col("after"), col("before")).alias("data"),
        col("op"),
        col("source.ts_ms").alias("source_ts_ms"),
        col("source.lsn").alias("source_lsn"),
    ).select(
        col("data.*"),
        col("op"),
        col("source_ts_ms"),
        col("source_lsn"),
    )

    # Deduplicate
    w = Window.partitionBy(*dedup_key_cols).orderBy(
        col("source_ts_ms").desc(), col("source_lsn").desc()
    )

    df = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

    # Transform
    if transform_fn is not None:
        df = transform_fn(df)

    return df.drop("source_ts_ms", "source_lsn")


def build_cdc_merge_writer(
    config: SilverConfig, transform_fns: Optional[list[Callable]] = None
) -> Callable:
    merge_key_cols = config.silver_key_cols
    silver_path = config.silver_path
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in merge_key_cols])

    transform_fn = build_transform(config, transform_fns)

    def merge_cdc_batch(df: DataFrame, batch_id: int) -> None:
        clean_df = prepare_cdc_batch_for_merge(
            df, dedup_key_cols=config.dedup_key_cols, transform_fn=transform_fn
        )
        col_map = {c: f"source.{c}" for c in clean_df.columns if c != "op"}

        if not DeltaTable.isDeltaTable(df.sparkSession, silver_path):
            clean_df.limit(0).drop("op").write.format("delta").save(silver_path)

        (
            DeltaTable.forPath(df.sparkSession, silver_path)
            .alias("target")
            .merge(clean_df.alias("source"), merge_condition)
            .whenMatchedDelete(condition="source.op = 'd'")
            .whenMatchedUpdate(condition="source.op != 'd'", set=col_map)
            .whenNotMatchedInsert(condition="source.op != 'd'", values=col_map)
            .execute()
        )

    return merge_cdc_batch
