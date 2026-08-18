from pymongo import MongoClient

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
        # إرسال أمر بسيط للتأكد أن MongoDB يستجيب
        client.admin.command("ping")

        print("تم الاتصال بـ MongoDB بنجاح")
        print(f"قاعدة البيانات: {MONGO_DATABASE}")

    finally:
        # إغلاق الاتصال مهما كانت نتيجة التنفيذ
        client.close()


def create_base_collections() -> None:
    """إنشاء Collections الأساسية إذا لم تكن موجودة."""

    client = get_mongo_client()

    try:
        database = get_database(client)

        existing_collections = database.list_collection_names()

        required_collections = [
            RAW_COLLECTION,
            VALIDATED_COLLECTION,
            QUARANTINE_COLLECTION,
        ]

        for collection_name in required_collections:

            # إنشاء الـ Collection فقط إذا لم تكن موجودة
            if collection_name not in existing_collections:
                database.create_collection(collection_name)
                print(f"تم إنشاء Collection: {collection_name}")

            else:
                print(f"Collection موجودة مسبقًا: {collection_name}")

    finally:
        # إغلاق الاتصال بصورة سليمة
        client.close()


if __name__ == "__main__":
    test_connection()
    create_base_collections()