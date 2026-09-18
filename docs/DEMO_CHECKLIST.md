# Live Demonstration Checklist

1. Start the platform.

```powershell
docker compose up --build
```

2. Show the Avro schema in `schemas/order.avsc`.

3. Show producer logs.

```powershell
docker compose logs -f producer
```

4. Show consumer logs with running average.

```powershell
docker compose logs -f consumer
```

5. Point out retry lines for `RetryItem`.

```text
temporary failure orderId=1010 attempt=1/3
```

6. Point out DLQ lines for `BrokenItem` and retry-exhausted messages.

```text
sent to dlq record=...
```

7. Inspect DLQ topic.

```powershell
docker compose --profile tools run --rm dlq-inspector
```

8. Stop services.

```powershell
docker compose down
```
