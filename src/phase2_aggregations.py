from typing import Any

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database, get_mongo_client


def _run_pipeline(pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """تشغيل Aggregation على البيانات الفعلية."""

    client = get_mongo_client()

    try:
        collection = get_database(client)[VALIDATED_COLLECTION]

        return list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

    finally:
        client.close()


def sales_by_city() -> list[dict[str, Any]]:
    """إجمالي عدد الطلبات والمبيعات حسب المدينة."""

    return _run_pipeline([
        {
            "$group": {
                "_id": "$record_clean.city",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$record_clean.total_amount"},
            }
        },
        {"$sort": {"total_sales": -1}},
    ])


def orders_by_status_summary() -> list[dict[str, Any]]:
    """ملخص الطلبات حسب حالة الطلب."""

    return _run_pipeline([
        {
            "$group": {
                "_id": "$record_clean.status",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$record_clean.total_amount"},
            }
        },
        {"$sort": {"order_count": -1}},
    ])


def payment_methods_summary() -> list[dict[str, Any]]:
    """ملخص طرق الدفع."""

    return _run_pipeline([
        {
            "$group": {
                "_id": "$record_clean.payment_method",
                "order_count": {"$sum": 1},
                "total_payment": {"$sum": "$record_clean.payment_amount"},
            }
        },
        {"$sort": {"order_count": -1}},
    ])


def delivery_types_summary() -> list[dict[str, Any]]:
    """ملخص الطلبات حسب نوع التوصيل."""

    return _run_pipeline([
        {
            "$group": {
                "_id": "$record_clean.delivery_type",
                "order_count": {"$sum": 1},
                "total_delivery_cost": {"$sum": "$record_clean.delivery_cost"},
            }
        },
        {"$sort": {"order_count": -1}},
    ])


def sales_by_month() -> list[dict[str, Any]]:
    """إجمالي الطلبات والمبيعات حسب الشهر."""

    return _run_pipeline([
        {
            "$group": {
                "_id": {
                    "$substrBytes": [
                        "$record_clean.order_date",
                        0,
                        7,
                    ]
                },
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$record_clean.total_amount"},
            }
        },
        {"$sort": {"_id": 1}},
    ])


AGGREGATIONS = {
    "sales_by_city": sales_by_city,
    "orders_by_status_summary": orders_by_status_summary,
    "payment_methods_summary": payment_methods_summary,
    "delivery_types_summary": delivery_types_summary,
    "sales_by_month": sales_by_month,
}


def list_aggregations() -> list[str]:
    """إرجاع أسماء تقارير Aggregation المتاحة."""

    return list(AGGREGATIONS.keys())


def run_aggregation(name: str) -> list[dict[str, Any]]:
    """تشغيل Aggregation محدد بالاسم."""

    if name not in AGGREGATIONS:
        raise ValueError(f"Unknown aggregation: {name}")

    return AGGREGATIONS[name]()
