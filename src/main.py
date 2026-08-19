import argparse
from pathlib import Path

from config.settings import SMALL_FILE_THRESHOLD_MB
from src.batch_loader import load_raw_with_python_batch
from src.elt_pipeline import process_elt_run
from src.file_router import get_file_size_mb, select_engine
from src.metrics import build_run_metrics, save_run_metrics
from src.spark_loader import load_raw_with_pyspark


def main() -> None:
    """نقطة التشغيل الرئيسية لخط معالجة بيانات الطلبات."""

    parser = argparse.ArgumentParser(
        description="تشغيل خط معالجة بيانات الطلبات تلقائيًا."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="مسار ملف CSV الذي سيتم معالجته.",
    )

    args = parser.parse_args()
    input_file = Path(args.input)

    print("=" * 60)
    print("بدء تشغيل خط البيانات الكامل")
    print("=" * 60)

    # اختيار المحرك تلقائيًا حسب حجم الملف
    engine = select_engine(input_file)

    # مرحلة Extract + Load إلى orders_raw
    if engine == "python_batch":
        loader_metrics = load_raw_with_python_batch(input_file)

    elif engine == "pyspark":
        loader_metrics = load_raw_with_pyspark(input_file)

    else:
        raise RuntimeError(f"محرك غير معروف: {engine}")

    # مرحلة Transform بعد اكتمال تحميل Raw
    elt_metrics = process_elt_run(loader_metrics["id_run"])

    # جمع وحفظ المقاييس النهائية
    run_metrics = build_run_metrics(
        loader_metrics=loader_metrics,
        elt_metrics=elt_metrics,
        file_size_mb=round(get_file_size_mb(input_file), 2),
        threshold_mb=SMALL_FILE_THRESHOLD_MB,
    )

    save_run_metrics(run_metrics)

    print("=" * 60)
    print("اكتمل خط البيانات بنجاح")
    print(f"id_run: {loader_metrics['id_run']}")
    print(f"المحرك المستخدم: {engine}")
    print(
        "Consistency:",
        run_metrics["consistency"]["pass"],
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
