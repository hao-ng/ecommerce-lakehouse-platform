from typing import Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_add,
    from_json,
    lit,
    posexplode,
    regexp_replace,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StructField,
    StructType,
    StringType,
)

from common.config import SilverConfig


def build_transform(
    config: SilverConfig, transform_fns: Optional[list[Callable]] = None
) -> Callable:
    def transform(df: DataFrame) -> DataFrame:
        fns = transform_fns or []
        # Cast types based on config
        for cast_type, cols in config.cast:
            if cols and cast_type in CAST_REGISTRY:
                df = CAST_REGISTRY[cast_type](df, cols)
        # Transform based on custom transform_fns
        for fn in fns:
            df = fn(df)
        return df

    return transform


# Cast function
def cast_epoch_days_to_date(df: DataFrame, cols: list[str]) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, date_add(lit("1970-01-01").cast("date"), col(c)))
    return df


def cast_string_to_timestamp(
    df: DataFrame, cols: list[str], fmt: str = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][XXX]"
) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, to_timestamp(c, fmt))
    return df


# Transform table function
def build_transaction_items(df: DataFrame) -> DataFrame:
    product_metadata_schema = ArrayType(
        StructType(
            [
                StructField("product_id", IntegerType()),
                StructField("quantity", IntegerType()),
                StructField("item_price", IntegerType()),
            ]
        )
    )

    clean_metadata = regexp_replace(
        "product_metadata",
        r'^"|"$',
        "",
    )

    return (
        df.select("op", "booking_id", "product_metadata")
        .select(
            "op",
            "booking_id",
            posexplode(from_json(clean_metadata, product_metadata_schema)).alias(
                "line_number",
                "product",
            ),
        )
        .select(
            "op",
            "booking_id",
            concat_ws("-", col("booking_id"), col("line_number")).alias("id"),
            "product.*",
        )
    )


def drop_product_metadata(df: DataFrame):
    return df.drop("product_metadata")


def drop_event_metadata(df: DataFrame):
    return df.drop("event_metadata")


def build_event_transform(event_name: str, schema: StructType) -> Callable:
    def transform(df: DataFrame) -> DataFrame:
        return (
            df.filter(col("event_name") == event_name)
            .select("event_id", from_json("event_metadata", schema).alias("metadata"))
            .select("event_id", "metadata.*")
        )

    return transform


ADD_TO_CART_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("item_price", IntegerType()),
    ]
)

PROMO_SCHEMA = StructType(
    [
        StructField("promo_code", StringType()),
        StructField("promo_amount", IntegerType()),
    ]
)

BOOKING_SCHEMA = StructType(
    [
        StructField("payment_status", StringType()),
    ]
)

SEARCH_SCHEMA = StructType(
    [
        StructField("search_keywords", StringType()),
    ]
)


CAST_REGISTRY: dict[str, Callable] = {
    "epoch_days": cast_epoch_days_to_date,
    "timestamp": cast_string_to_timestamp,
}
TRANSFORM_REGISTRY: dict[str, list[Callable]] = {
    "transactions": [drop_product_metadata],
    "transaction_items": [build_transaction_items],
    "clickstream": [drop_event_metadata],
    "clickstream_add_to_cart_events": [
        build_event_transform("ADD_TO_CART", ADD_TO_CART_SCHEMA)
    ],
    "clickstream_add_promo_events": [build_event_transform("ADD_PROMO", PROMO_SCHEMA)],
    "clickstream_booking_events": [build_event_transform("BOOKING", BOOKING_SCHEMA)],
    "clickstream_search_events": [build_event_transform("SEARCH", SEARCH_SCHEMA)],
}
