from __future__ import annotations

import itertools
import logging
import random
import time

from kafka import KafkaProducer

from order_app.avro_codec import load_schema, serialize_avro
from order_app.config import ORDER_SCHEMA_PATH, env_bool, env_float, env_str

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("order-producer")
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def build_order(sequence_number: int, include_demo_failures: bool) -> dict:
    if include_demo_failures and sequence_number % 15 == 0:
        product = "BrokenItem"
    elif include_demo_failures and sequence_number % 10 == 0:
        product = "RetryItem"
    else:
        product = random.choice(PRODUCTS)
    return {"orderId": str(1000 + sequence_number), "product": product, "price": round(random.uniform(10.0, 500.0), 2)}


def main() -> None:
    bootstrap_servers = env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env_str("ORDER_TOPIC", "orders")
    interval_seconds = env_float("MESSAGE_INTERVAL_SECONDS", 1.0)
    include_demo_failures = env_bool("DEMO_FAILURE_MESSAGES", True)
    schema = load_schema(ORDER_SCHEMA_PATH)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: serialize_avro(value, schema),
        retries=5,
        linger_ms=50,
    )
    for sequence_number in itertools.count(1):
        order = build_order(sequence_number, include_demo_failures)
        producer.send(topic, key=order["orderId"], value=order)
        producer.flush()
        LOGGER.info("produced order=%s topic=%s", order, topic)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
