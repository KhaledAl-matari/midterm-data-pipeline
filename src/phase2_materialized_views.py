from datetime import datetime, timezone
from typing import Any

from pymongo import UpdateOne

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database, get_mongo_client


DAILY_MV = "mv_daily_sales_summary"
CITY_MV = "mv_city_sales_summary"
MV_ORDER_STATE = "mv_order_contributions"
MV_REFRESH_STATE = "mv_refresh_state"


def _full_refresh(database) -> dict[str, Any]:
    """البناء الأول الكامل للـMaterialized Views."""

    source = database[VALIDATED_COLLECTION]
    daily = database[DAILY_MV]
    city = database[CITY_MV]
    contribution = database[MV_ORDER_STATE]

    daily.delete_many({})
    city.delete_many({})
    contribution.delete_many({})

    source.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "$substrBytes": [
                            "$record_clean.order_date",
                            0,
                            10,
                        ]
                    },
                    "order_count": {"$sum": 1},
                    "total_sales": {
                        "$sum": "$record_clean.total_amount"
                    },
                }
            },
            {
                "$set": {
                    "refreshed_at": "$$NOW",
                }
            },
            {
                "$merge": {
                    "into": DAILY_MV,
                    "on": "_id",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
        allowDiskUse=True,
    )

    source.aggregate(
        [
            {
                "$group": {
                    "_id": "$record_clean.city",
                    "order_count": {"$sum": 1},
                    "total_sales": {
                        "$sum": "$record_clean.total_amount"
                    },
                }
            },
            {
                "$set": {
                    "refreshed_at": "$$NOW",
                }
            },
            {
                "$merge": {
                    "into": CITY_MV,
                    "on": "_id",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
        allowDiskUse=True,
    )

    source.aggregate(
        [
            {
                "$project": {
                    "_id": "$order_id",
                    "day": {
                        "$substrBytes": [
                            "$record_clean.order_date",
                            0,
                            10,
                        ]
                    },
                    "city": "$record_clean.city",
                    "total_amount": "$record_clean.total_amount",
                    "source_updated_at": "$updated_at",
                }
            },
            {
                "$merge": {
                    "into": MV_ORDER_STATE,
                    "on": "_id",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
        allowDiskUse=True,
    )

    latest = source.find_one(
        {},
        {"updated_at": 1},
        sort=[("updated_at", -1)],
    )

    watermark = (
        latest.get("updated_at")
        if latest
        else datetime.now(timezone.utc)
    )

    database[MV_REFRESH_STATE].update_one(
        {"_id": "phase2_materialized_views"},
        {
            "$set": {
                "last_source_updated_at": watermark,
                "last_refresh_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )

    return {
        "mode": "full",
        "changed_orders": source.count_documents({}),
        "last_source_updated_at": watermark,
    }


def refresh_materialized_views() -> dict[str, Any]:
    """
    تحديث الـMaterialized Views.

    أول تشغيل: Full Refresh.
    التشغيلات التالية: Incremental Refresh للسجلات التي تغيرت فقط.
    """

    client = get_mongo_client()

    try:
        database = get_database(client)
        source = database[VALIDATED_COLLECTION]
        refresh_state = database[MV_REFRESH_STATE]

        state = refresh_state.find_one(
            {"_id": "phase2_materialized_views"}
        )

        if not state or "last_source_updated_at" not in state:
            return _full_refresh(database)

        watermark = state["last_source_updated_at"]

        changed = list(
            source.find(
                {"updated_at": {"$gt": watermark}},
                {
                    "_id": 0,
                    "order_id": 1,
                    "updated_at": 1,
                    "record_clean.city": 1,
                    "record_clean.order_date": 1,
                    "record_clean.total_amount": 1,
                },
            ).sort("updated_at", 1)
        )

        if not changed:
            return {
                "mode": "incremental",
                "changed_orders": 0,
                "last_source_updated_at": watermark,
            }

        daily_ops = []
        city_ops = []
        contribution_ops = []

        contribution_collection = database[MV_ORDER_STATE]
        now = datetime.now(timezone.utc)

        for document in changed:
            order_id = document["order_id"]
            record = document["record_clean"]

            new_day = record["order_date"][:10]
            new_city = record["city"]
            new_total = float(record["total_amount"])

            previous = contribution_collection.find_one(
                {"_id": order_id}
            )

            if previous:
                daily_ops.append(
                    UpdateOne(
                        {"_id": previous["day"]},
                        {
                            "$inc": {
                                "order_count": -1,
                                "total_sales": -float(
                                    previous["total_amount"]
                                ),
                            },
                            "$set": {
                                "refreshed_at": now,
                            },
                        },
                        upsert=True,
                    )
                )

                city_ops.append(
                    UpdateOne(
                        {"_id": previous["city"]},
                        {
                            "$inc": {
                                "order_count": -1,
                                "total_sales": -float(
                                    previous["total_amount"]
                                ),
                            },
                            "$set": {
                                "refreshed_at": now,
                            },
                        },
                        upsert=True,
                    )
                )

            daily_ops.append(
                UpdateOne(
                    {"_id": new_day},
                    {
                        "$inc": {
                            "order_count": 1,
                            "total_sales": new_total,
                        },
                        "$set": {
                            "refreshed_at": now,
                        },
                    },
                    upsert=True,
                )
            )

            city_ops.append(
                UpdateOne(
                    {"_id": new_city},
                    {
                        "$inc": {
                            "order_count": 1,
                            "total_sales": new_total,
                        },
                        "$set": {
                            "refreshed_at": now,
                        },
                    },
                    upsert=True,
                )
            )

            contribution_ops.append(
                UpdateOne(
                    {"_id": order_id},
                    {
                        "$set": {
                            "day": new_day,
                            "city": new_city,
                            "total_amount": new_total,
                            "source_updated_at": document["updated_at"],
                        }
                    },
                    upsert=True,
                )
            )

        if daily_ops:
            database[DAILY_MV].bulk_write(daily_ops)

        if city_ops:
            database[CITY_MV].bulk_write(city_ops)

        if contribution_ops:
            contribution_collection.bulk_write(contribution_ops)

        database[DAILY_MV].delete_many(
            {"order_count": {"$lte": 0}}
        )

        database[CITY_MV].delete_many(
            {"order_count": {"$lte": 0}}
        )

        latest_watermark = changed[-1]["updated_at"]

        refresh_state.update_one(
            {"_id": "phase2_materialized_views"},
            {
                "$set": {
                    "last_source_updated_at": latest_watermark,
                    "last_refresh_at": now,
                }
            },
            upsert=True,
        )

        return {
            "mode": "incremental",
            "changed_orders": len(changed),
            "last_source_updated_at": latest_watermark,
        }

    finally:
        client.close()


def list_materialized_views() -> list[str]:
    """إرجاع أسماء الـMaterialized Views."""

    return [
        DAILY_MV,
        CITY_MV,
    ]


def get_materialized_view(
    name: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """قراءة Materialized View بالاسم."""

    if name not in list_materialized_views():
        raise ValueError(f"Unknown materialized view: {name}")

    client = get_mongo_client()

    try:
        database = get_database(client)

        return list(
            database[name]
            .find({})
            .sort("_id", 1)
            .limit(limit)
        )

    finally:
        client.close()


if __name__ == "__main__":
    print(list_materialized_views())
