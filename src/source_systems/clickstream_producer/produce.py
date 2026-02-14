import argparse
import logging
from pathlib import Path
from time import sleep

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from utils.event_ordering import order_event_by_time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_FILE = Path(__file__).parent.parent / "data/clickstream/click_stream.parquet"


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        default="setup",
        choices=["setup", "teardown"],
        help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
    )

    parser.add_argument(
        "-b",
        "--bootstrap_servers",
        type=str,
        default="localhost:9092",
        help="Where the bootstrap server is",
    )

    parser.add_argument(
        "-s",
        "--schema_registry_server",
        type=str,
        default="http://localhost:8081",
        help="Where to host schema",
    )

    parser.add_argument(
        "-c",
        "--schema_path",
        type=str,
        default="./schemas/clickstream_schema.avsc",
        help="Folder containing all generated avro schemas",
    )

    parser.add_argument(
        "-n",
        "--topic_name",
        type=str,
        default="clickstream",
        help="The topic name",
    )

    return parser.parse_args()


def create_topic(admin: AdminClient, topic_name: str) -> NewTopic:
    """Create Kafka topic

    Args:
        admin (AdminClient): Kafka AdminClient
        topic_name (str): Name of the topic

    Returns:
        NewTopic: The created topic
    """
    try:
        topic = NewTopic(
            topic=topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        admin.create_topics([topic])
        logger.info(f"A new topic {topic_name} has been created")
    except Exception:
        logger.info(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred, or None on success.

        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        logger.error(f"Delivery failed, {err}")
        return
    logger.info(f"Record successfully produced to {msg.topic()}")


def create_stream(
    data_path: str,
    servers: str,
    schema_path: str,
    topic_name: str,
    schema_registry_client: SchemaRegistryClient,
) -> None:
    """
    Simulate a clickstream by reading data from disk and producing Avro
    records to a Kafka topic in event-time order.

    Args:
        data_path (str): Path to CSV or Parquet file
        servers (str): Kafka bootstrap servers (e.g. "localhost:9092")
        schema_path (str): Path to Avro schema (.avsc)
        topic_name (str): Name of the topic
        schema_registry_client (SchemaRegistryClient): Schema Registry Client
    """

    producer = None
    admin = None

    producer_conf = {"bootstrap.servers": servers}
    admin_conf = {"bootstrap.servers": servers}

    # Create Producer and AdminClient
    for _ in range(10):
        try:
            producer = Producer(producer_conf)
            admin = AdminClient(admin_conf)
            logger.info("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            logger.error(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

    # Simulate a clickstream
    logger.info("Preparing data")
    df = order_event_by_time(data_path, "event_time")

    # Read schema and create serializer
    with open(schema_path, "r") as file:
        schema = file.read()

    avro_serializer = AvroSerializer(schema_registry_client, schema)

    # Create topic
    logger.info("Create topic")
    create_topic(admin, topic_name)

    try:
        for row in df.itertuples(index=False):
            producer.poll(0.0)
            row_str = row._asdict()
            try:
                logger.info(f"Sending: {row_str}")
                producer.produce(
                    topic=topic_name,
                    value=avro_serializer(
                        row_str,
                        SerializationContext(topic_name, MessageField.VALUE),
                    ),
                    on_delivery=delivery_report,
                )
            except Exception as e:
                logger.error(f"Producing failed {e}")
            sleep(1)
    finally:
        producer.flush()
        logger.info("Producing completed!!")


def teardown_stream(topic_name: str, servers: str = "localhost:9092") -> None:
    admin_conf = {"bootstrap.servers": servers}
    try:
        admin = AdminClient(admin_conf)
        logger.info(admin.delete_topics([topic_name]))
        logger.info(f"Topic {topic_name} deleted.")
    except Exception as e:
        logger.error(e)
        pass


if __name__ == "__main__":
    args = parse_args()
    mode = args.mode
    servers = args.bootstrap_servers
    schema_registry_server = args.schema_registry_server
    topic_name = args.topic_name
    schema_path = args.schema_path

    # Tear down previous stream
    logger.info("Tearing down existing topic!")
    try:
        teardown_stream(topic_name, servers)
    except Exception:
        logger.info(f"Topic {topic_name} does not exist. Skipping...!")

    if mode == "setup":
        schema_registry_conf = {"url": schema_registry_server}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        create_stream(
            DATA_FILE,
            servers,
            schema_path,
            topic_name,
            schema_registry_client,
        )
