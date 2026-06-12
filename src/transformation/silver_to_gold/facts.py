from pyspark.sql import DataFrame
from utils import generate_surrogate_key
from pyspark.sql.functions import date_format, col


def build_fact_transactions(
    transactions: DataFrame,
) -> DataFrame:
    return transactions.select(
        generate_surrogate_key(["booking_id"]).alias("transaction_key"),
        "booking_id",
        generate_surrogate_key(["customer_id"]).alias("customer_key"),
        date_format(col("created_at"), "yyyyMMdd")
        .cast("int")
        .alias("transaction_date_key"),
        date_format(col("shipment_date_limit"), "yyyyMMdd")
        .cast("int")
        .alias("shipment_date_limit_key"),
        "session_id",
        "payment_method",
        "payment_status",
        "promo_amount",
        "promo_code",
        "shipment_fee",
        "shipment_location_lat",
        "shipment_location_long",
        "total_amount",
    )
