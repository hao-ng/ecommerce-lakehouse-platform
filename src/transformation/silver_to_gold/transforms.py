from dimensions import build_dim_customers, build_dim_date, build_dim_products
from facts import build_fact_transactions

from common.config import GoldConfig
from pyspark.sql import SparkSession
from typing import Callable
import inspect


def select_matching_kwargs(function: Callable, extra_kwargs: dict):
    args = set(inspect.signature(function).parameters)
    return {key: value for key, value in extra_kwargs.items() if key in args}


def run_gold_transform(
    spark: SparkSession,
    config: GoldConfig,
    transform_fn: Callable,
    extra_kwargs: dict,
):
    dfs = {
        source: spark.read.format("delta").load(path)
        for source, path in config.source_paths.items()
    }
    candidate_kwargs = {**dfs, **extra_kwargs, "spark": spark}
    kwargs = select_matching_kwargs(transform_fn, candidate_kwargs)

    return transform_fn(**kwargs)


TRANSFORM_REGISTRY = {
    "dim_customers": build_dim_customers,
    "dim_products": build_dim_products,
    "dim_date": build_dim_date,
    "fact_transactions": build_fact_transactions,
}
