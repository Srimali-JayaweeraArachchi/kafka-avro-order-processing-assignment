from __future__ import annotations


class TemporaryOrderError(RuntimeError):
    pass


class PermanentOrderError(RuntimeError):
    pass


def validate_order(order: dict, temporary_products: set[str], permanent_products: set[str]) -> None:
    order_id = str(order.get("orderId", "")).strip()
    product = str(order.get("product", "")).strip()
    price = float(order.get("price", -1))

    if not order_id:
        raise PermanentOrderError("orderId is required")
    if not product:
        raise PermanentOrderError("product is required")
    if price <= 0:
        raise PermanentOrderError("price must be greater than zero")
    if product in permanent_products:
        raise PermanentOrderError(f"product {product!r} is configured as permanently failing")
    if product in temporary_products:
        raise TemporaryOrderError(f"temporary downstream failure for product {product!r}")
