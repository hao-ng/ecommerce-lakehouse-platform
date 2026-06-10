from typing import Callable, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from transforms import build_transform

from common.config import SilverConfig


def build_clickstream_append_writer(
    config: SilverConfig, transform_fns: Optional[list[Callable]] = None
) -> Callable:
    silver_path = config.silver_path
    transform_fn = build_transform(config, transform_fns)

    def append_clickstream(df: DataFrame, batch_id: int) -> None:
        clean_df = transform_fn(df)
        if not DeltaTable.isDeltaTable(df.sparkSession, silver_path):
            clean_df.limit(0).write.format("delta").save(silver_path)

        clean_df.write.format("delta").mode("append").save(silver_path)

    return append_clickstream
