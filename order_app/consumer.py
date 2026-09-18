from __future__ import annotations

import logging
import time
from datetime import datetime, timezone


from order_app.avro_codec import deserialize_avro, load_schema, serialize_avro
from order_app.config import DLQ_SCHEMA_PATH, ORDER_SCHEMA_PATH, env_csv, env_float, env_int, env_str
from order_app.failure_policy import PermanentOrderError, TemporaryOrderError, validate_order

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("order-consumer")


class RunningAverage:
    def __init__(self) -> None:
        self.count = 0
        self.total_price = 0.0

    def add(self, price: float) -> float:
        self.count += 1
        self.total_price += price
        return self.total_price / self.count


def build_dlq_record(order: dict, error: Exception, retry_count: int) -> dict:
    return {
        "orderId": str(order.get("orderId", "")),
        "product": str(order.get("product", "")),
        "price": float(order.get("price", 0.0)),
        "errorType": error.__class__.__name__,
        "errorMessage": str(error),
        "retryCount": retry_count,
        "failedAt": datetime.now(timezone.utc).isoformat(),
    }


def process_with_retry(order: dict, running_average: RunningAverage, temporary_products: set[str], permanent_products: set[str], max_retries: int, retry_backoff_seconds: float) -> float:
    attempt = 0
    while True:
        try:
            validate_order(order, temporary_products, permanent_products)
            return running_average.add(float(order["price"]))
        except TemporaryOrderError:
            attempt += 1
            if attempt > max_retries:
                raise
            LOGGER.warning("temporary failure orderId=%s attempt=%s/%s", order.get("orderId"), attempt, max_retries)
            time.sleep(retry_backoff_seconds)


def main() -> None:
    from kafka import KafkaConsumer, KafkaProducer

    bootstrap_servers = env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    order_topic = env_str("ORDER_TOPIC", "orders")
    dlq_topic = env_str("DLQ_TOPIC", "orders.dlq")
    group_id = env_str("CONSUMER_GROUP", "order-average-consumer")
    max_retries = env_int("MAX_RETRIES", 3)
    retry_backoff_seconds = env_float("RETRY_BACKOFF_SECONDS", 1.0)
    temporary_products = env_csv("FAIL_TEMPORARY_PRODUCTS", "RetryItem")
    permanent_products = env_csv("FAIL_PERMANENT_PRODUCTS", "BrokenItem")
    order_schema = load_schema(ORDER_SCHEMA_PATH)
    dlq_schema = load_schema(DLQ_SCHEMA_PATH)
    running_average = RunningAverage()
    consumer = KafkaConsumer(order_topic, bootstrap_servers=bootstrap_servers, group_id=group_id, auto_offset_reset="earliest", enable_auto_commit=False, key_deserializer=lambda value: value.decode("utf-8") if value else None)
    dlq_producer = KafkaProducer(bootstrap_servers=bootstrap_servers, key_serializer=lambda key: key.encode("utf-8"), value_serializer=lambda value: serialize_avro(value, dlq_schema), retries=5)
    LOGGER.info("consumer started topic=%s dlq=%s", order_topic, dlq_topic)
    for message in consumer:
        order = deserialize_avro(message.value, order_schema)
        try:
            average = process_with_retry(order, running_average, temporary_products, permanent_products, max_retries, retry_backoff_seconds)
            LOGGER.info("processed orderId=%s product=%s price=%.2f count=%s running_average=%.2f", order["orderId"], order["product"], order["price"], running_average.count, average)
        except (PermanentOrderError, TemporaryOrderError) as error:
            retry_count = max_retries if isinstance(error, TemporaryOrderError) else 0
            dlq_record = build_dlq_record(order, error, retry_count)
            dlq_producer.send(dlq_topic, key=dlq_record["orderId"], value=dlq_record)
            dlq_producer.flush()
            LOGGER.error("sent to dlq record=%s", dlq_record)
        finally:
            consumer.commit()


if __name__ == "__main__":
    main()

