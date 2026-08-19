from pymongo import ASCENDING, MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_mongo_client() -> MongoClient:
    """إنشاء اتصال مع MongoDB."""
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )


def get_database(client: MongoClient):
    """إرجاع قاعدة البيانات الخاصة بالمشروع."""
    return client[MONGO_DATABASE]


def test_connection() -> None:
    """اختبار الاتصال مع MongoDB."""
    client = get_mongo_client()

    try:
        client.admin.command("ping")
        print("تم الاتصال بـ MongoDB بنجاح")
        print(f"قاعدة البيانات: {MONGO_DATABASE}")

    finally:
        client.close()


def create_base_collections() -> None:
    """إنشاء Collections الأساسية إذا لم تكن موجودة."""
    client = get_mongo_client()

    try:
        database = get_database(client)
        existing = database.list_collection_names()

        for collection_name in (
            RAW_COLLECTION,
            VALIDATED_COLLECTION,
            QUARANTINE_COLLECTION,
        ):
            if collection_name not in existing:
                database.create_collection(collection_name)
                print(f"تم إنشاء Collection: {collection_name}")
            else:
                print(f"Collection موجودة مسبقًا: {collection_name}")

    finally:
        client.close()


def configure_indexes_and_validation() -> None:
    """إنشاء الفهارس وSchema Validation المطلوبة للمشروع."""
    client = get_mongo_client()

    try:
        database = get_database(client)

        raw = database[RAW_COLLECTION]
        validated = database[VALIDATED_COLLECTION]
        quarantine = database[QUARANTINE_COLLECTION]

        # تسريع الوصول إلى سجلات Run محدد
        raw.create_index(
            [
                ("id_run", ASCENDING),
                ("_id", ASCENDING),
            ],
            name="ix_orders_raw_id_run_id",
        )

        # منع تكرار المفتاح التجاري في البيانات النهائية
        validated.create_index(
            "order_id",
            unique=True,
            name="uq_orders_validated_order_id",
        )

        # منع تكرار نفس سجل Raw داخل Quarantine
        quarantine.create_index(
            "raw_id",
            unique=True,
            name="uq_orders_quarantine_raw_id",
        )

        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": [
                    "order_id",
                    "record_clean",
                    "quality_status",
                    "corrections",
                    "last_id_run",
                    "last_raw_id",
                    "file_source",
                    "updated_at",
                ],
                "properties": {
                    "order_id": {
                        "bsonType": "string",
                    },
                    "record_clean": {
                        "bsonType": "object",
                    },
                    "quality_status": {
                        "enum": ["valid", "corrected"],
                    },
                    "corrections": {
                        "bsonType": "array",
                    },
                    "last_id_run": {
                        "bsonType": "string",
                    },
                    "last_raw_id": {
                        "bsonType": "objectId",
                    },
                    "file_source": {
                        "bsonType": "string",
                    },
                    "updated_at": {
                        "bsonType": "date",
                    },
                    "created_at": {
                        "bsonType": "date",
                    },
                },
            }
        }

        database.command(
            {
                "collMod": VALIDATED_COLLECTION,
                "validator": validator,
                "validationLevel": "strict",
                "validationAction": "error",
            }
        )

        print("تم إنشاء MongoDB Indexes بنجاح")
        print("تم تفعيل Schema Validation بنجاح")

    finally:
        client.close()


if __name__ == "__main__":
    test_connection()
    create_base_collections()
    configure_indexes_and_validation()
