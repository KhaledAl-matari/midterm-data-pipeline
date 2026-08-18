import argparse
import csv
from pathlib import Path


def create_small_sample(input_file: Path, output_file: Path, rows: int) -> None:
    """إنشاء عينة صغيرة من ملف CSV كبير بدون تحميل الملف كاملًا في الذاكرة."""

    # إنشاء مجلد الإخراج إذا لم يكن موجودًا
    output_file.parent.mkdir(parents=True, exist_ok=True)

    written_rows = 0

    # فتح الملف الأصلي للقراءة بطريقة تدريجية
    with input_file.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as source_file:

        reader = csv.reader(source_file)

        # قراءة أسماء الأعمدة
        header = next(reader)

        # فتح ملف العينة للكتابة
        with output_file.open(
            mode="w",
            encoding="utf-8-sig",
            newline="",
        ) as sample_file:

            writer = csv.writer(sample_file)

            # كتابة أسماء الأعمدة أولًا
            writer.writerow(header)

            # قراءة العدد المطلوب فقط من السجلات
            for row in reader:
                if written_rows >= rows:
                    break

                writer.writerow(row)
                written_rows += 1

    print("تم إنشاء العينة بنجاح")
    print(f"الملف المصدر: {input_file}")
    print(f"ملف العينة: {output_file}")
    print(f"عدد السجلات المكتوبة: {written_rows}")


def main() -> None:
    # استقبال القيم من سطر الأوامر
    parser = argparse.ArgumentParser(
        description="إنشاء عينة صغيرة من ملف CSV كبير."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="مسار ملف CSV الأصلي.",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="مسار ملف العينة الناتج.",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="عدد السجلات المطلوب استخراجها.",
    )

    args = parser.parse_args()

    # التأكد من أن عدد الصفوف قيمة صحيحة
    if args.rows <= 0:
        raise ValueError("يجب أن يكون عدد السجلات أكبر من صفر.")

    create_small_sample(
        input_file=Path(args.input),
        output_file=Path(args.output),
        rows=args.rows,
    )


if __name__ == "__main__":
    main()