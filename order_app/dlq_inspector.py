from __future__ import annotations

import logging

from kafka import KafkaConsumer

from order_app.avro_codec import deserialize_avro, load_schema
from order_app.config import DLQ_SCHEMA_PATH, env_str

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("dlq-inspector")


def main() -> None:
    bootstrap_servers = env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    dlq_topic = env_str("DLQ_TOPIC", "orders.dlq")
    dlq_schema = load_schema(DLQ_SCHEMA_PATH)
    consumer = KafkaConsumer(dlq_topic, bootstrap_servers=bootstrap_servers, group_id="dlq-inspector", auto_offset_reset="earliest", key_deserializer=lambda value: value.decode("utf-8") if value else None)
    LOGGER.info("watching DLQ topic=%s", dlq_topic)
    for message in consumer:
        LOGGER.info("dlq record=%s", deserialize_avro(message.value, dlq_schema))


if __name__ == "__main__":
    main()
