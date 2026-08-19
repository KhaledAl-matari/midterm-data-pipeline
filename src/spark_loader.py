import sys
import time
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, struct
from pyspark.sql.types import StringType, StructField, StructType

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    SPARK_TEMP_DIR,
    SPARK_WAREHOUSE_DIR,
)


# Schema ثابتة للملف الكبير.
# نقرأ الحقول الخام كـ String حتى لا نفقد القيم غير النظيفة.
ORDERS_RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """إنشاء SparkSession بإعدادات المشروع."""

    # إجبار PySpark Worker على استخدام نفس Python الذي يشغل المشروع.
    # هذا مهم في Windows لمنع مشكلة Python worker failed to connect back.
    python_executable = sys.executable

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("MidtermDataPipeline")
        .config("spark.pyspark.python", python_executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", python_executable)
        .config("spark.local.dir", str(SPARK_TEMP_DIR))
        .config(
            "spark.sql.warehouse.dir",
            SPARK_WAREHOUSE_DIR.as_uri(),
        )
        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI,
        )
        .getOrCreate()
    )

    # تقليل رسائل Spark غير الضرورية في شاشة التشغيل.
    spark.sparkContext.setLogLevel("WARN")

    return spark


def load_raw_with_pyspark(file_path: Path) -> dict:
    """تحميل ملف CSV الكبير إلى orders_raw باستخدام PySpark."""

    # معرف فريد لكل عملية تشغيل.
    id_run = str(uuid.uuid4())

    # إنشاء المجلدات المؤقتة إذا لم تكن موجودة.
    SPARK_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    SPARK_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    spark = None
    start_time = time.perf_counter()

    try:
        spark = create_spark_session()

        print("=" * 60)
        print("بدء PySpark Raw Loader")
        print(f"id_run: {id_run}")
        print(f"الملف: {file_path}")
        print("=" * 60)

        # قراءة CSV باستخدام Schema ثابتة.
        # لا نستخدم inferSchema حتى تبقى القيم الخام كما وصلت.
        source_df = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .schema(ORDERS_RAW_SCHEMA)
            .csv(str(file_path))
        )

        # عدد Partitions التي أنشأها Spark للملف.
        input_partitions = source_df.rdd.getNumPartitions()

        print(f"عدد Input Partitions: {input_partitions}")

        # تجميع الحقول الأصلية داخل record_raw بدون تنظيف.
        raw_columns = [
            source_df[column_name]
            for column_name in ORDERS_RAW_SCHEMA.fieldNames()
        ]

        raw_df = source_df.select(
            lit(id_run).alias("id_run"),
            lit(str(file_path)).alias("file_source"),

            # في مسار Spark لا نعتمد رقم صف زائفًا؛
            # لأن ترتيب Partitions لا يضمن رقم صف أصلي ثابت.
            lit(None).cast("long").alias("number_row_source"),

            current_timestamp().alias("at_ingested"),
            lit("pyspark").alias("engine_used"),
            struct(*raw_columns).alias("record_raw"),
        )

        write_start = time.perf_counter()

        # الكتابة بالتوازي إلى MongoDB باستخدام MongoDB Spark Connector.
        # نستخدم append لأن orders_raw طبقة تاريخية لكل id_run.
        (
            raw_df.write
            .format("mongodb")
            .mode("append")
            .option("database", MONGO_DATABASE)
            .option("collection", RAW_COLLECTION)
            .save()
        )

        write_elapsed = time.perf_counter() - write_start

        # بعد نجاح الكتابة نتحقق من عدد السجلات التي وصلت لهذا التشغيل.
        # نقرأ العدد من MongoDB حتى لا نعيد مسح ملف الـCSV الكبير فقط للعد.
        from pymongo import MongoClient

        mongo_client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
        )

        try:
            raw_collection = mongo_client[MONGO_DATABASE][RAW_COLLECTION]

            loaded_raw = raw_collection.count_documents(
                {"id_run": id_run}
            )

        finally:
            mongo_client.close()

        total_elapsed = time.perf_counter() - start_time

        throughput = (
            loaded_raw / total_elapsed
            if total_elapsed > 0
            else 0
        )

        print("=" * 60)
        print("اكتمل PySpark Raw Loader")
        print(f"إجمالي السجلات المحملة: {loaded_raw}")
        print(f"عدد Partitions: {input_partitions}")
        print(f"زمن الكتابة: {write_elapsed:.2f} ثانية")
        print(f"الزمن الكلي: {total_elapsed:.2f} ثانية")
        print(f"معدل المعالجة: {throughput:.2f} سجل/ثانية")
        print("=" * 60)

        return {
            "id_run": id_run,
            "file_name": file_path.name,
            "used_engine": "pyspark",
            "read_rows": loaded_raw,
            "loaded_raw": loaded_raw,
            "partitions": input_partitions,
            "seconds_elapsed": round(total_elapsed, 4),
            "write_seconds": round(write_elapsed, 4),
            "throughput": round(throughput, 2),
        }

    finally:
        # إغلاق Spark بصورة سليمة مهما كانت نتيجة التشغيل.
        if spark is not None:
            spark.stop()