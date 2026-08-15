from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    sequence,
    to_date,
    lit,
    expr,
    date_format,
    dayofweek,
    dayofmonth,
    dayofyear,
    weekofyear,
    month,
    quarter,
    year,
)
from utils import generate_surrogate_key
from pyspark.sql import SparkSession


def build_dim_products(products: DataFrame) -> DataFrame:
    return products.select(
        col("id").alias("product_id"),
        "gender",
        "master_category",
        "sub_category",
        "article_type",
        "base_colour",
        "season",
        col("year").alias("product_year"),
        col("usage").alias("product_usage"),
        "product_display_name",
    ).select(generate_surrogate_key(["product_id"]).alias("product_key"), "*")


def build_dim_customers(customers: DataFrame) -> DataFrame:
    return customers.select(
        generate_surrogate_key(["customer_id"]).alias("customer_key"),
        "customer_id",
        "first_name",
        "last_name",
        "username",
        "email",
        "gender",
        "birthdate",
        "device_type",
        "device_id",
        "device_version",
        "home_location_lat",
        "home_location_long",
        "home_location",
        "home_country",
        "first_join_date",
    )


def build_dim_date(
    spark: SparkSession, start_date: str = "2016-01-01", end_date: str = "2035-12-31"
) -> DataFrame:
    return (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(start_date)),
                    to_date(lit(end_date)),
                    expr("interval 1 day"),
                )
            ).alias("full_date")
        )
        .select(
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("full_date"),
            dayofweek("full_date").alias("day_of_week"),
            dayofmonth("full_date").alias("day_of_month"),
            dayofyear("full_date").alias("day_of_year"),
            weekofyear("full_date").alias("week_of_year"),
            month("full_date").alias("month_of_year"),
            date_format("full_date", "MMMM").alias("month_name"),
            quarter("full_date").alias("quarter_of_year"),
            year("full_date").alias("year_number"),
        )
    )
