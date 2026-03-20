_DEFAULTS = {
    "kafka_bootstrap_servers": "broker:29092",
    "trigger": "10 seconds",
}

CLICKSTREAM = {
    **_DEFAULTS,
    "topic": "clickstream",
    "checkpoint_location": "s3a://datalake/checkpoints/bronze/clickstream_checkpoint",
    "output_path": "s3a://datalake/bronze/clickstream",
    "schema_subject": "clickstream-value",
}

CDC_TABLES = {
    "customers": {
        **_DEFAULTS,
        "topic": "ecommerce.public.customers",
        "checkpoint_location": "s3a://datalake/checkpoints/bronze/customers_checkpoint",
        "output_path": "s3a://datalake/bronze/customers",
        "schema_subject": "ecommerce.public.customers-value",
    },
    "products": {
        **_DEFAULTS,
        "topic": "ecommerce.public.products",
        "checkpoint_location": "s3a://datalake/checkpoints/bronze/products_checkpoint",
        "output_path": "s3a://datalake/bronze/products",
        "schema_subject": "ecommerce.public.products-value",
    },
    "transactions": {
        **_DEFAULTS,
        "topic": "ecommerce.public.transactions",
        "checkpoint_location": "s3a://datalake/checkpoints/bronze/transactions_checkpoint",
        "output_path": "s3a://datalake/bronze/transactions",
        "schema_subject": "ecommerce.public.transactions-value",
    },
}
