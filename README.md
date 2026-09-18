# Kafka Avro Order Processing Assignment

This project implements the Chapter 3 assignment: a Kafka order messaging system with Avro serialization, real-time running average aggregation, retry handling for temporary failures, and a Dead Letter Queue for permanently failed messages.

## Requirements Coverage

| Requirement | Implementation |
|---|---|
| Kafka producer and consumer | `order_app/producer.py` publishes to `orders`; `order_app/consumer.py` consumes from `orders`. |
| Avro serialization | `schemas/order.avsc` and `order_app/avro_codec.py` implement Avro binary encoding for the assignment schema. |
| Real-time aggregation | `RunningAverage` maintains the running average of successful order prices. |
| Retry logic | `process_with_retry` retries temporary failures up to `MAX_RETRIES`. |
| Dead Letter Queue | Failed records are written to the Kafka topic `orders.dlq`. |
| Live demo | `docs/DEMO_CHECKLIST.md` contains the demonstration steps. |

## Run

```powershell
docker compose up --build
```

Watch the consumer:

```powershell
docker compose logs -f consumer
```

Inspect DLQ messages:

```powershell
docker compose --profile tools run --rm dlq-inspector
```

Stop:

```powershell
docker compose down
```

## Tests

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## Demo Behavior

- Normal products are processed and included in the running average.
- `RetryItem` simulates temporary failures and demonstrates retry logic.
- `BrokenItem` simulates permanent failures and goes directly to the DLQ.
