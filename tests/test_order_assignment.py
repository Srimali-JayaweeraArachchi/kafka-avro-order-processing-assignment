from __future__ import annotations

import pytest

from order_app.avro_codec import deserialize_avro, load_schema, serialize_avro
from order_app.config import DLQ_SCHEMA_PATH, ORDER_SCHEMA_PATH
from order_app.consumer import RunningAverage, build_dlq_record, process_with_retry
from order_app.failure_policy import PermanentOrderError, TemporaryOrderError, validate_order


def test_order_avro_round_trip() -> None:
    schema = load_schema(ORDER_SCHEMA_PATH)
    order = {"orderId": "1001", "product": "Item1", "price": 99.5}
    decoded = deserialize_avro(serialize_avro(order, schema), schema)
    assert decoded["orderId"] == "1001"
    assert decoded["product"] == "Item1"
    assert decoded["price"] == pytest.approx(99.5)


def test_running_average_updates_after_successful_orders() -> None:
    average = RunningAverage()
    assert average.add(100.0) == pytest.approx(100.0)
    assert average.add(200.0) == pytest.approx(150.0)
    assert average.add(300.0) == pytest.approx(200.0)


def test_permanent_validation_failure_for_invalid_price() -> None:
    with pytest.raises(PermanentOrderError):
        validate_order({"orderId": "1002", "product": "Item2", "price": 0.0}, set(), set())


def test_temporary_failure_retries_then_raises() -> None:
    average = RunningAverage()
    with pytest.raises(TemporaryOrderError):
        process_with_retry({"orderId": "1010", "product": "RetryItem", "price": 10.0}, average, {"RetryItem"}, set(), 2, 0)
    assert average.count == 0


def test_dlq_record_uses_avro_schema() -> None:
    schema = load_schema(DLQ_SCHEMA_PATH)
    record = build_dlq_record({"orderId": "1015", "product": "BrokenItem", "price": 20.0}, PermanentOrderError("broken product"), 0)
    decoded = deserialize_avro(serialize_avro(record, schema), schema)
    assert decoded["orderId"] == "1015"
    assert decoded["errorType"] == "PermanentOrderError"
    assert decoded["retryCount"] == 0
