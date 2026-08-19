from pathlib import Path

# مسارات المشروع
PROJECT_ROOT = Path(r"D:\midterm-data-pipeline")
DATA_DIR = Path(r"D:\Big data")
REPORTS_DIR = PROJECT_ROOT / "reports"

# ملفات البيانات
LARGE_DIRTY_FILE = DATA_DIR / "orders_huge_mixed_quality.csv"
LARGE_CLEAN_FILE = DATA_DIR / "orders_huge_clean.csv"
SMALL_SAMPLE_FILE = PROJECT_ROOT / "data" / "orders_small_sample.csv"

# الحد الفاصل الذي يستخدمه File Router
# إذا كان حجم الملف أقل أو يساوي 200 MB سيتم استخدام Python Batch
# وإذا كان أكبر سيتم استخدام PySpark
SMALL_FILE_THRESHOLD_MB = 200

# عدد الصفوف التي سيتم استخراجها للعينة الصغيرة
SAMPLE_ROWS = 100_000

# عدد السجلات في كل دفعة أثناء استخدام Python Batch
BATCH_SIZE = 5_000

# إعدادات الاتصال بقاعدة بيانات MongoDB
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DATABASE = "midterm_data_pipeline"

# أسماء المجموعات داخل MongoDB
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# ملف حفظ نتائج التشغيل والقياسات
RESULTS_FILE = REPORTS_DIR / "results.json"

# مجلدات Spark المؤقتة على القرص D لتجنب استهلاك مساحة القرص C
SPARK_TEMP_DIR = PROJECT_ROOT / "spark_temp"
SPARK_WAREHOUSE_DIR = PROJECT_ROOT / "spark_warehouse"