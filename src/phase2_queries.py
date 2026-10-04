from typing import Any

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database, get_mongo_client


DEFAULT_LIMIT = 100


def _projection() -> dict[str, int]:
    return {
        "_id": 0,
        "order_id": 1,
        "quality_status": 1,
        "record_clean": 1,
    }


def orders_by_city(
    city: str,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """إرجاع الطلبات التابعة لمدينة محددة."""

    client = get_mongo_client()

    try:
        collection = get_database(client)[VALIDATED_COLLECTION]

        return list(
            collection.find(
                {"record_clean.city": city},
                _projection(),
            ).limit(limit)
        )

    finally:
        client.close()


def orders_by_status(
    status: str,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """إرجاع الطلبات حسب حالة الطلب."""

    client = get_mongo_client()

    try:
        collection = get_database(client)[VALIDATED_COLLECTION]

        return list(
            collection.find(
                {"record_clean.status": status},
                _projection(),
            ).limit(limit)
        )

    finally:
        client.close()


def customer_orders_by_date(
    customer_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """إرجاع طلبات عميل محدد ضمن نطاق زمني اختياري."""

    client = get_mongo_client()

    try:
        collection = get_database(client)[VALIDATED_COLLECTION]

        query: dict[str, Any] = {
            "record_clean.customer_id": customer_id,
        }

        date_filter: dict[str, str] = {}

        if start_date:
            date_filter["$gte"] = start_date

        if end_date:
            date_filter["$lte"] = end_date

        if date_filter:
            query["record_clean.order_date"] = date_filter

        return list(
            collection.find(
                query,
                _projection(),
            )
            .sort("record_clean.order_date", 1)
            .limit(limit)
        )

    finally:
        client.close()


def orders_by_payment_method(
    payment_method: str,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """إرجاع الطلبات حسب طريقة الدفع."""

    client = get_mongo_client()

    try:
        collection = get_database(client)[VALIDATED_COLLECTION]

        return list(
            collection.find(
                {"record_clean.payment_method": payment_method},
                _projection(),
            ).limit(limit)
        )

    finally:
        client.close()


def high_value_orders(
    min_total: float,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """إرجاع الطلبات التي تساوي أو تتجاوز قيمة مالية محددة."""

    client = get_mongo_client()

    try:
        collection = get_database(client)[VALIDATED_COLLECTION]

        return list(
            collection.find(
                {
                    "record_clean.total_amount": {
                        "$gte": float(min_total),
                    }
                },
                _projection(),
            )
            .sort("record_clean.total_amount", -1)
            .limit(limit)
        )

    finally:
        client.close()


QUERY_NAMES = [
    "orders_by_city",
    "orders_by_status",
    "customer_orders_by_date",
    "orders_by_payment_method",
    "high_value_orders",
]


def list_queries() -> list[str]:
    """إرجاع أسماء الاستعلامات العملية المتاحة."""

    return list(QUERY_NAMES)
