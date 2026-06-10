from typing import Callable

from pyspark.sql import DataFrame
from transforms import TRANSFORM_REGISTRY

from common.config import SilverConfig


def build_composite_writer(
    *writer_fns: Callable[[DataFrame, int], None],
) -> Callable:
    def writer_batch(df: DataFrame, batch_id: int) -> None:
        df.cache()

        for writer_fn in writer_fns:
            writer_fn(df, batch_id)

        df.unpersist()

    return writer_batch


def build_batch_writer(
    config: SilverConfig, writer_factory: Callable[[SilverConfig, list[Callable]], None]
) -> Callable:
    parent_writer = writer_factory(config, TRANSFORM_REGISTRY.get(config.name))

    if not config.children:
        return parent_writer

    children_writers = [
        writer_factory(child_config, TRANSFORM_REGISTRY.get(child_name))
        for child_name, child_config in config.children.items()
    ]

    return build_composite_writer(parent_writer, *children_writers)
