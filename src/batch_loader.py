import csv
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo.errors import PyMongoError

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    RAW_COLLECTION,
)
from src.mongo_setup import get_database, get_mongo_client


def load_raw_with_python_batch(
    file_path: Path,
    batch_size: int = BATCH_SIZE,
) -> dict:
    """تحميل ملف CSV إلى orders_raw باستخدام القراءة التدريجية والدفعات."""

    # إنشاء معرف فريد لعملية التشغيل الحالية
    id_run = str(uuid.uuid4())

    # وقت بداية التشغيل لقياس الأداء
    start_time = time.perf_counter()

    total_loaded = 0
    batch_number = 0
    batch = []

    client = get_mongo_client()

    try:
        database = get_database(client)
        raw_collection = database[RAW_COLLECTION]

        print("=" * 60)
        print("بدء Python Batch Loader")
        print(f"id_run: {id_run}")
        print(f"الملف: {file_path}")
        print(f"حجم الدفعة: {batch_size}")
        print("=" * 60)

        # فتح CSV وقراءته تدريجيًا بدون تحميل الملف كاملًا إلى الذاكرة
        with file_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline="",
        ) as source_file:

            reader = csv.DictReader(source_file)

            # يبدأ رقم سجل البيانات من 2 لأن الصف الأول هو Header
            for row_number, raw_record in enumerate(reader, start=2):

                # حفظ السجل الخام كما وصل بدون تنظيف أو تحويل
                raw_document = {
                    "id_run": id_run,
                    "file_source": str(file_path),
                    "number_row_source": row_number,
                    "at_ingested": datetime.now(timezone.utc),
                    "engine_used": "python_batch",
                    "record_raw": raw_record,
                }

                batch.append(raw_document)

                # عند وصول الدفعة للحجم المحدد يتم إرسالها إلى MongoDB
                if len(batch) >= batch_size:
                    batch_number += 1
                    batch_start = time.perf_counter()

                    try:
                        result = raw_collection.insert_many(batch)

                    except PyMongoError as error:
                        print(f"فشل إدخال الدفعة رقم {batch_number}")
                        print(f"سبب الخطأ: {error}")
                        raise

                    batch_elapsed = time.perf_counter() - batch_start
                    batch_count = len(result.inserted_ids)
                    total_loaded += batch_count

                    batch_rate = (
                        batch_count / batch_elapsed
                        if batch_elapsed > 0
                        else 0
                    )

                    print(
                        f"الدفعة {batch_number}: "
                        f"{batch_count} سجل | "
                        f"{batch_elapsed:.2f} ثانية | "
                        f"{batch_rate:.2f} سجل/ثانية"
                    )

                    # تفريغ الدفعة بعد نجاح كتابتها
                    batch.clear()

            # إدخال آخر دفعة إذا كانت أصغر من batch_size
            if batch:
                batch_number += 1
                batch_start = time.perf_counter()

                try:
                    result = raw_collection.insert_many(batch)

                except PyMongoError as error:
                    print(f"فشل إدخال الدفعة رقم {batch_number}")
                    print(f"سبب الخطأ: {error}")
                    raise

                batch_elapsed = time.perf_counter() - batch_start
                batch_count = len(result.inserted_ids)
                total_loaded += batch_count

                batch_rate = (
                    batch_count / batch_elapsed
                    if batch_elapsed > 0
                    else 0
                )

                print(
                    f"الدفعة {batch_number}: "
                    f"{batch_count} سجل | "
                    f"{batch_elapsed:.2f} ثانية | "
                    f"{batch_rate:.2f} سجل/ثانية"
                )

        total_elapsed = time.perf_counter() - start_time

        throughput = (
            total_loaded / total_elapsed
            if total_elapsed > 0
            else 0
        )

        print("=" * 60)
        print("اكتمل Python Batch Loader")
        print(f"إجمالي السجلات المحملة: {total_loaded}")
        print(f"عدد الدفعات: {batch_number}")
        print(f"الزمن الكلي: {total_elapsed:.2f} ثانية")
        print(f"معدل المعالجة: {throughput:.2f} سجل/ثانية")
        print("=" * 60)

        # إرجاع المقاييس لاستخدامها لاحقًا في results.json
        return {
            "id_run": id_run,
            "file_name": file_path.name,
            "used_engine": "python_batch",
            "loaded_raw": total_loaded,
            "size_batch": batch_size,
            "batch_count": batch_number,
            "seconds_elapsed": round(total_elapsed, 4),
            "throughput": round(throughput, 2),
        }

    finally:
        # إغلاق اتصال MongoDB مهما كانت نتيجة التشغيل
        client.close()