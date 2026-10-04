from pymongo import ASCENDING

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database, get_mongo_client


PHASE2_INDEXES = [
    {
        "name": "ix_phase2_city",
        "keys": [
            ("record_clean.city", ASCENDING),
        ],
    },
    {
        "name": "ix_phase2_status",
        "keys": [
            ("record_clean.status", ASCENDING),
        ],
    },
    {
        "name": "ix_phase2_customer_date",
        "keys": [
            ("record_clean.customer_id", ASCENDING),
            ("record_clean.order_date", ASCENDING),
        ],
    },
]


def create_phase2_indexes() -> list[str]:
    """إنشاء فهارس الأداء الخاصة بالمرحلة الثانية."""

    client = get_mongo_client()
    created_indexes: list[str] = []

    try:
        database = get_database(client)
        collection = database[VALIDATED_COLLECTION]

        for index_spec in PHASE2_INDEXES:
            index_name = collection.create_index(
                index_spec["keys"],
                name=index_spec["name"],
            )
            created_indexes.append(index_name)

        return created_indexes

    finally:
        client.close()


def drop_phase2_indexes() -> list[str]:
    """حذف فهارس Phase 2 فقط لاستخدامها في مقارنة Explain قبل وبعد."""

    client = get_mongo_client()
    dropped_indexes: list[str] = []

    try:
        database = get_database(client)
        collection = database[VALIDATED_COLLECTION]

        existing_indexes = {
            index["name"]
            for index in collection.list_indexes()
        }

        for index_spec in PHASE2_INDEXES:
            index_name = index_spec["name"]

            if index_name in existing_indexes:
                collection.drop_index(index_name)
                dropped_indexes.append(index_name)

        return dropped_indexes

    finally:
        client.close()


def list_phase2_indexes() -> list[dict]:
    """إرجاع معلومات فهارس Phase 2 الموجودة حاليًا."""

    client = get_mongo_client()

    try:
        database = get_database(client)
        collection = database[VALIDATED_COLLECTION]

        phase2_names = {
            index_spec["name"]
            for index_spec in PHASE2_INDEXES
        }

        return [
            {
                "name": index["name"],
                "key": dict(index["key"]),
            }
            for index in collection.list_indexes()
            if index["name"] in phase2_names
        ]

    finally:
        client.close()


if __name__ == "__main__":
    indexes = create_phase2_indexes()

    print("Phase 2 indexes ready:")
    for name in indexes:
        print(f"- {name}")
