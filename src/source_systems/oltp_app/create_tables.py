import logging
import os

from dotenv import load_dotenv
from oltp_app.logging_config import setup_logging
from oltp_app.postgresql_client import PostgresSQLClient

setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
    )

    logger.info("Dropping existing tables... ")
    pc.drop_tables()
    logger.info("Creating tables... ")
    pc.create_tables()
    logger.info("Tables created successfully.")


if __name__ == "__main__":
    main()
