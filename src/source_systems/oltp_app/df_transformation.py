import numpy as np
import pandas as pd
from oltp_app.models import Customer, Product, Transaction

CUSTOMERS_SCHEMA = {
    "int": ["customer_id"],
    "float": ["home_location_lat", "home_location_long"],
    "str": [
        "first_name",
        "last_name",
        "username",
        "email",
        "gender",
        "device_type",
        "device_id",
        "device_version",
        "home_location",
        "home_country",
    ],
    "date": ["birthdate", "first_join_date"],
    "datetime": [],
    "rename": {},
}

PRODUCTS_SCHEMA = {
    "rename": {
        "masterCategory": "master_category",
        "subCategory": "sub_category",
        "articleType": "article_type",
        "baseColour": "base_colour",
        "productDisplayName": "product_display_name",
    },
    "int": ["id", "year"],
    "float": [],
    "str": [
        "gender",
        "master_category",
        "sub_category",
        "article_type",
        "base_colour",
        "season",
        "usage",
        "product_display_name",
    ],
    "date": [],
    "datetime": [],
}

TRANSACTIONS_SCHEMA = {
    "rename": {},
    "int": ["customer_id", "promo_amount", "shipment_fee", "total_amount"],
    "float": ["shipment_location_lat", "shipment_location_long"],
    "str": [
        "booking_id",
        "session_id",
        "product_metadata",
        "payment_method",
        "payment_status",
        "promo_code",
    ],
    "date": [],
    "datetime": ["created_at", "shipment_date_limit"],
}

DATASETS = {
    "products": {
        "file": "products.parquet",
        "schema": PRODUCTS_SCHEMA,
        "mode": "batch",
        "model": Product,
    },
    "customers": {
        "file": "customers.parquet",
        "schema": CUSTOMERS_SCHEMA,
        "mode": "batch",
        "model": Customer,
    },
    "transactions": {
        "file": "transactions.parquet",
        "schema": TRANSACTIONS_SCHEMA,
        "mode": "streaming",
        "model": Transaction,
    },
}


def rename_columns(df: pd.DataFrame, rename_mapping: dict) -> pd.DataFrame:
    return df.rename(columns=rename_mapping)


def convert_dtypes(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    for dtype, columns in schema.items():
        for col in columns:
            if col not in df.columns:
                continue
            if dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce").replace(
                    {np.nan: None}
                )
            elif dtype == "str":
                df[col] = df[col].astype(str)
            elif dtype == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def normalize_df(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = rename_columns(df, schema.get("rename", {}))
    df = convert_dtypes(df, schema)
    return df


def df_to_dicts(df: pd.DataFrame, schema: dict) -> list[dict]:
    df = normalize_df(df, schema)
    return df.to_dict(orient="records")
