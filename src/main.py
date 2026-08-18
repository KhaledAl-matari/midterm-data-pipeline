import argparse
from pathlib import Path

from src.file_router import select_engine

def main() -> None:
    """نقطة التشغيل الرئيسية للمشروع."""

    # استقبال مسار ملف البيانات من المستخدم
    parser = argparse.ArgumentParser(
        description="تشغيل خط معالجة بيانات الطلبات."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="مسار ملف CSV الذي سيتم معالجته.",
    )

    args = parser.parse_args()

    # تحويل المسار النصي إلى Path
    input_file = Path(args.input)

    print("=" * 60)
    print("بدء تشغيل خط البيانات")
    print("=" * 60)

    # اختيار محرك المعالجة تلقائيًا حسب حجم الملف
    engine = select_engine(input_file)

    print("=" * 60)
    print(f"قرار الـ Router النهائي: {engine}")
    print("=" * 60)


if __name__ == "__main__":
    main()