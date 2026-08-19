import time
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.quality_rules import clean_and_classify


def _process_batch(
    raw_collection,
    validated_collection,
    quarantine_collection,
    raw_documents: list[dict[str, Any]],
    id_run: str,
) -> dict[str, int]:
    """تنظيف دفعة واحدة ثم حفظ نتيجة كل سجل في المسار المناسب."""

    evaluated: list[tuple[dict[str, Any], dict[str, Any]]] = []

    # تنظيف وتصنيف السجلات مع إبقاء record_raw دون تعديل
    for raw_document in raw_documents:
        raw_record = raw_document.get("record_raw", {})
        result = clean_and_classify(raw_record)
        evaluated.append((raw_document, result))

    # جلب السجلات الموجودة مسبقًا لقياس Insert / Update / Unchanged
    business_keys = []

    for _, result in evaluated:
        if result["outcome"] != "quarantine":
            order_id = result["cleaned_record"].get("order_id")

            if order_id:
                business_keys.append(order_id)

    existing_map: dict[str, Any] = {}

    if business_keys:
        unique_keys = list(dict.fromkeys(business_keys))

        cursor = validated_collection.find(
            {"order_id": {"$in": unique_keys}},
            {
                "_id": 0,
                "order_id": 1,
                "record_clean": 1,
            },
        )

        for document in cursor:
            existing_map[document["order_id"]] = document.get(
                "record_clean"
            )

    validated_operations = []
    quarantine_operations = []
    raw_outcome_operations = []

    stats = {
        "processed": 0,
        "valid": 0,
        "corrected": 0,
        "quarantined": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
    }

    processed_at = datetime.now(timezone.utc)

    for raw_document, result in evaluated:
        stats["processed"] += 1

        raw_id = raw_document["_id"]
        outcome = result["outcome"]

        if outcome == "quarantine":
            stats["quarantined"] += 1

            quarantine_document = {
                "raw_id": raw_id,
                "id_run": id_run,
                "order_id": result["cleaned_record"].get("order_id"),
                "file_source": raw_document.get("file_source"),
                "number_row_source": raw_document.get(
                    "number_row_source"
                ),
                "engine_used": raw_document.get("engine_used"),
                "record_raw": raw_document.get("record_raw"),
                "corrections_attempted": result["corrections"],
                "error_codes": result["error_codes"],
                "error_details": result["error_details"],
                "quarantined_at": processed_at,
            }

            # raw_id يجعل إعادة تشغيل نفس Run لا تكرر سجل الحجر
            quarantine_operations.append(
                UpdateOne(
                    {"raw_id": raw_id},
                    {"$set": quarantine_document},
                    upsert=True,
                )
            )

            raw_outcome_operations.append(
                UpdateOne(
                    {"_id": raw_id},
                    {
                        "$set": {
                            "processing_outcome": "orders_quarantine",
                            "quality_status": "quarantine",
                            "processing_error_codes": result[
                                "error_codes"
                            ],
                            "processed_at": processed_at,
                        }
                    },
                )
            )

            continue

        # السجل صالح أو تم تصحيحه، ولذلك يذهب إلى validated
        stats[outcome] += 1

        cleaned_record = result["cleaned_record"]
        order_id = cleaned_record["order_id"]

        existing_record = existing_map.get(order_id)

        validated_document = {
            "order_id": order_id,
            "record_clean": cleaned_record,
            "quality_status": outcome,
            "corrections": result["corrections"],
            "last_id_run": id_run,
            "last_raw_id": raw_id,
            "file_source": raw_document.get("file_source"),
            "updated_at": processed_at,
        }

        if existing_record is None:
            stats["inserted"] += 1

            validated_operations.append(
                UpdateOne(
                    {"order_id": order_id},
                    {
                        "$set": validated_document,
                        "$setOnInsert": {
                            "created_at": processed_at,
                        },
                    },
                    upsert=True,
                )
            )

            # تحديث الخريطة داخل نفس الدفعة لمعالجة التكرار بأمان
            existing_map[order_id] = cleaned_record

        elif existing_record == cleaned_record:
            stats["unchanged"] += 1

        else:
            stats["updated"] += 1

            validated_operations.append(
                UpdateOne(
                    {"order_id": order_id},
                    {"$set": validated_document},
                    upsert=True,
                )
            )

            existing_map[order_id] = cleaned_record

        raw_outcome_operations.append(
            UpdateOne(
                {"_id": raw_id},
                {
                    "$set": {
                        "processing_outcome": "orders_validated",
                        "quality_status": outcome,
                        "processing_error_codes": [],
                        "processed_at": processed_at,
                    }
                },
            )
        )

    # أولًا نكتب النتائج النهائية، وبعد نجاحها نعلّم سجل Raw بأنه عولج
    if validated_operations:
        validated_collection.bulk_write(
            validated_operations,
            ordered=True,
        )

    if quarantine_operations:
        quarantine_collection.bulk_write(
            quarantine_operations,
            ordered=False,
        )

    if raw_outcome_operations:
        raw_collection.bulk_write(
            raw_outcome_operations,
            ordered=False,
        )

    return stats


def process_elt_run(
    id_run: str,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """
    معالجة Run موجود مسبقًا داخل orders_raw.

    لا يتم تعديل record_raw.
    كل سجل Raw يحصل على Processing Outcome واحد.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    client = None
    cursor = None
    started_at = time.perf_counter()

    total_stats = {
        "processed": 0,
        "valid": 0,
        "corrected": 0,
        "quarantined": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
    }

    try:
        client = MongoClient(MONGO_URI)
        database = client[MONGO_DATABASE]

        raw_collection = database[RAW_COLLECTION]
        validated_collection = database[VALIDATED_COLLECTION]
        quarantine_collection = database[QUARANTINE_COLLECTION]

        total_raw = raw_collection.count_documents(
            {"id_run": id_run}
        )

        if total_raw == 0:
            raise ValueError(
                f"No raw records found for id_run={id_run}"
            )

        print("=" * 60)
        print("بدء مرحلة ELT")
        print(f"id_run: {id_run}")
        print(f"عدد سجلات Raw: {total_raw}")
        print(f"حجم الدفعة: {batch_size}")
        print("=" * 60)

        cursor = (
            raw_collection.find({"id_run": id_run})
            .sort("_id", 1)
            .batch_size(batch_size)
        )

        batch = []
        batch_number = 0

        for raw_document in cursor:
            batch.append(raw_document)

            if len(batch) < batch_size:
                continue

            batch_number += 1
            batch_started = time.perf_counter()

            batch_stats = _process_batch(
                raw_collection,
                validated_collection,
                quarantine_collection,
                batch,
                id_run,
            )

            for key in total_stats:
                total_stats[key] += batch_stats[key]

            batch_elapsed = time.perf_counter() - batch_started
            rate = (
                batch_stats["processed"] / batch_elapsed
                if batch_elapsed > 0
                else 0
            )

            print(
                f"Batch {batch_number}: "
                f"processed={batch_stats['processed']} | "
                f"quarantine={batch_stats['quarantined']} | "
                f"inserted={batch_stats['inserted']} | "
                f"updated={batch_stats['updated']} | "
                f"unchanged={batch_stats['unchanged']} | "
                f"time={batch_elapsed:.2f}s | "
                f"rate={rate:.2f} rows/s"
            )

            batch = []

        # معالجة آخر دفعة إذا كان حجمها أقل من batch_size
        if batch:
            batch_number += 1
            batch_started = time.perf_counter()

            batch_stats = _process_batch(
                raw_collection,
                validated_collection,
                quarantine_collection,
                batch,
                id_run,
            )

            for key in total_stats:
                total_stats[key] += batch_stats[key]

            batch_elapsed = time.perf_counter() - batch_started
            rate = (
                batch_stats["processed"] / batch_elapsed
                if batch_elapsed > 0
                else 0
            )

            print(
                f"Batch {batch_number}: "
                f"processed={batch_stats['processed']} | "
                f"quarantine={batch_stats['quarantined']} | "
                f"inserted={batch_stats['inserted']} | "
                f"updated={batch_stats['updated']} | "
                f"unchanged={batch_stats['unchanged']} | "
                f"time={batch_elapsed:.2f}s | "
                f"rate={rate:.2f} rows/s"
            )

        elapsed = time.perf_counter() - started_at

        # شرط الاتساق الأساسي: لا يوجد سجل Raw ضائع
        if total_stats["processed"] != total_raw:
            raise RuntimeError(
                "Consistency failure: processed rows do not equal raw rows"
            )

        if (
            total_stats["valid"]
            + total_stats["corrected"]
            + total_stats["quarantined"]
            != total_raw
        ):
            raise RuntimeError(
                "Consistency failure: outcomes do not equal raw rows"
            )

        result = {
            "id_run": id_run,
            "raw_rows": total_raw,
            **total_stats,
            "seconds_elapsed": round(elapsed, 4),
            "throughput": round(
                total_raw / elapsed if elapsed > 0 else 0,
                2,
            ),
        }

        print("=" * 60)
        print("اكتملت مرحلة ELT")
        print(f"Raw: {result['raw_rows']}")
        print(f"Valid: {result['valid']}")
        print(f"Corrected: {result['corrected']}")
        print(f"Quarantine: {result['quarantined']}")
        print(f"Inserted: {result['inserted']}")
        print(f"Updated: {result['updated']}")
        print(f"Unchanged: {result['unchanged']}")
        print(f"الزمن: {result['seconds_elapsed']:.2f} ثانية")
        print(f"المعدل: {result['throughput']:.2f} سجل/ثانية")
        print("=" * 60)

        return result

    except PyMongoError as exc:
        print(f"MongoDB ELT ERROR: {exc}")
        raise

    finally:
        if cursor is not None:
            cursor.close()

        if client is not None:
            client.close()
