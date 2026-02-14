import logging
import os
from pathlib import Path
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from oltp_app.df_transformation import DATASETS, df_to_dicts
from oltp_app.logging_config import setup_logging
from oltp_app.postgresql_client import PostgresSQLClient
from utils.event_ordering import order_event_by_time

FILE_FOLDER = Path(__file__).parent.parent / "data/oltp"

setup_logging()

logger = logging.getLogger(__name__)

load_dotenv()


def insert_data(client: PostgresSQLClient, config: dict, batch_size=10000):
    """insert data from a parquet file into the database

    Args:
        client (PostgresSQLClient): PostgreSQL client instance
        file (str): file name of the parquet file
        model: SQLAlchemy model class representing the table
        batch_size (int): batch size
    """
    file = config["file"]
    model = config["model"]
    schema = config["schema"]
    mode = config["mode"]

    file_path = FILE_FOLDER / file
    df = pd.read_parquet(file_path)

    if mode == "streaming":
        df = order_event_by_time(file_path, "created_at")

    records = df_to_dicts(df, schema)

    total_records = len(records)
    logger.info(f"Inserting {total_records} records into {model.__tablename__} table")

    for i in range(0, total_records, batch_size):
        batch = records[i : i + batch_size]
        client.insert(model, batch)
        logger.info(
            f"Inserted batch {i // batch_size + 1} of {model.__tablename__} table"
        )
        sleep(0.5)


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
    )

    for dataset, config in DATASETS.items():
        if config["mode"] == "batch":
            insert_data(pc, config, batch_size=10000)
            logger.info(f"Finished inserting data into {dataset} table")

    for dataset, config in DATASETS.items():
        if config["mode"] == "streaming":
            insert_data(pc, config, batch_size=200)
            logger.info(f"Finished inserting data into {dataset} table")


if __name__ == "__main__":
    main()
