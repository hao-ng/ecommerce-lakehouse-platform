from typing import Callable, Optional

from pyspark.sql import DataFrame
from transforms import build_transform

from common.config import SilverConfig


def build_clickstream_append_writer(
    config: SilverConfig, transform_fns: Optional[list[Callable]] = None
) -> Callable:
    transform_fn = build_transform(config, transform_fns)

    def append_clickstream(df: DataFrame, batch_id: int) -> None:
        clean_df = transform_fn(df)
        (
            clean_df.write.format("delta")
            .mode("append")
            .option("path", config.table_path)
            .saveAsTable(config.qualified_name)
        )

    return append_clickstream
