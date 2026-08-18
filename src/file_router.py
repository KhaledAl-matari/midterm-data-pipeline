from pathlib import Path

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path: Path) -> float:
    """إرجاع حجم الملف بالميجابايت."""

    if not file_path.exists():
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")

    return file_path.stat().st_size / (1024 * 1024)


def select_engine(file_path: Path) -> str:
    """اختيار محرك المعالجة تلقائيًا حسب حجم الملف."""

    file_size_mb = get_file_size_mb(file_path)

    # الملفات الصغيرة أو المساوية للحد تستخدم Python Batch
    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"حجم الملف {file_size_mb:.2f} MB "
            f"أقل من أو يساوي الحد {SMALL_FILE_THRESHOLD_MB} MB"
        )

    # الملفات الأكبر من الحد تستخدم PySpark
    else:
        engine = "pyspark"
        reason = (
            f"حجم الملف {file_size_mb:.2f} MB "
            f"أكبر من الحد {SMALL_FILE_THRESHOLD_MB} MB"
        )

    # طباعة قرار الـ Router كما يطلب التكليف
    print(f"حجم الملف: {file_size_mb:.2f} MB")
    print(f"المحرك المختار: {engine}")
    print(f"سبب الاختيار: {reason}")

    return engine