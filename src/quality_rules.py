import json
import re
from datetime import datetime
from typing import Any


# الحالات المسموح بها في البيانات
VALID_STATUSES = {
    "تم التسليم",
    "ملغي",
    "مؤكد",
    "قيد الشحن",
    "مرتجع",
    "قيد الانتظار",
}

VALID_PAYMENT_STATUSES = {
    "بانتظار الدفع",
    "تم الدفع",
    "قيد الدفع",
}

VALID_CURRENCIES = {"YER"}

# تحويل معروف وآمن فقط بدون تخمين
PAYMENT_STATUS_MAP = {
    "مدفوع": "تم الدفع",
}

CURRENCY_MAP = {
    "ريال يمني": "YER",
}

# قيم وجدناها فعليًا داخل البيانات
WORD_NUMBERS = {
    "ألفان": 2000.0,
    "خمسة آلاف": 5000.0,
}

ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩٫",
    "0123456789.",
)


def _add_correction(
    corrections: list[dict[str, Any]],
    field: str,
    old_value: Any,
    new_value: Any,
    rule: str,
) -> None:
    """إضافة تعديل إلى سجل التدقيق Audit Trail."""
    if old_value != new_value:
        corrections.append(
            {
                "field": field,
                "old_value": old_value,
                "new_value": new_value,
                "rule": rule,
            }
        )


def _add_error(
    error_codes: list[str],
    error_details: list[dict[str, Any]],
    code: str,
    field: str,
    value: Any,
    message: str,
) -> None:
    """إضافة سبب واضح للحجر Quarantine."""
    if code not in error_codes:
        error_codes.append(code)

    error_details.append(
        {
            "code": code,
            "field": field,
            "value": value,
            "message": message,
        }
    )


def normalize_text(value: Any) -> Any:
    """إزالة المسافات الزائدة من النصوص فقط."""
    if not isinstance(value, str):
        return value

    return value.strip()


def parse_number(value: Any) -> float | None:
    """
    تحويل القيم المالية المعروفة إلى رقم.
    لا يتم تخمين أي قيمة غير معروفة.
    """
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    if text in WORD_NUMBERS:
        return WORD_NUMBERS[text]

    text = text.translate(ARABIC_DIGITS)
    text = text.replace(",", "")
    text = text.replace("ريال يمني", "")
    text = text.replace("ريال", "")
    text = text.replace("YER", "")
    text = text.strip()

    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def normalize_money(
    record: dict[str, Any],
    field: str,
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """توحيد الحقول المالية إلى قيمة رقمية."""
    old_value = record.get(field)
    new_value = parse_number(old_value)

    if new_value is None:
        _add_error(
            error_codes,
            error_details,
            "INVALID_MONEY",
            field,
            old_value,
            "القيمة المالية غير قابلة للتحويل بشكل آمن",
        )
        return

    _add_correction(
        corrections,
        field,
        old_value,
        new_value,
        "NORMALIZE_MONEY",
    )
    record[field] = new_value


def normalize_date(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """توحيد صيغ التاريخ المعروفة إلى صيغة واحدة."""
    field = "order_date"
    old_value = record.get(field)

    if not old_value:
        _add_error(
            error_codes,
            error_details,
            "MISSING_ORDER_DATE",
            field,
            old_value,
            "تاريخ الطلب مفقود",
        )
        return

    value = str(old_value).strip()

    formats = (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    )

    parsed = None

    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            break
        except ValueError:
            continue

    if parsed is None:
        _add_error(
            error_codes,
            error_details,
            "INVALID_ORDER_DATE",
            field,
            old_value,
            "صيغة التاريخ غير معروفة أو التاريخ غير صالح",
        )
        return

    new_value = parsed.strftime("%Y-%m-%d %H:%M:%S")

    _add_correction(
        corrections,
        field,
        old_value,
        new_value,
        "NORMALIZE_ORDER_DATE",
    )

    record[field] = new_value


def normalize_phone(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """توحيد رقم الهاتف بدون تخمين الرقم نفسه."""
    field = "customer_phone"
    old_value = record.get(field)

    if old_value is None or not str(old_value).strip():
        return

    value = str(old_value).strip().translate(ARABIC_DIGITS)
    value = re.sub(r"[\s\-]", "", value)

    # توحيد رمز اليمن إذا كان موجودًا
    if value.startswith("00967"):
        value = "+967" + value[5:]
    elif value.startswith("967"):
        value = "+967" + value[3:]

    if value.startswith("+967"):
        local_part = value[4:]

        if not local_part.isdigit() or len(local_part) != 9:
            _add_error(
                error_codes,
                error_details,
                "INVALID_PHONE",
                field,
                old_value,
                "رقم الهاتف غير صالح",
            )
            return

    elif value.isdigit() and len(value) == 9:
        value = "+967" + value

    else:
        _add_error(
            error_codes,
            error_details,
            "INVALID_PHONE",
            field,
            old_value,
            "رقم الهاتف غير صالح",
        )
        return

    _add_correction(
        corrections,
        field,
        old_value,
        value,
        "NORMALIZE_PHONE",
    )

    record[field] = value


def normalize_email(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """توحيد البريد الصحيح وحجر البريد غير القابل للتصحيح."""
    field = "customer_email"
    old_value = record.get(field)

    if old_value is None or not str(old_value).strip():
        return

    value = str(old_value).strip().lower()

    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    if not re.fullmatch(pattern, value) or ".." in value:
        _add_error(
            error_codes,
            error_details,
            "INVALID_EMAIL",
            field,
            old_value,
            "صيغة البريد الإلكتروني غير صالحة ولا يمكن تصحيحها بأمان",
        )
        return

    _add_correction(
        corrections,
        field,
        old_value,
        value,
        "NORMALIZE_EMAIL",
    )

    record[field] = value


def normalize_enums(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """التحقق من الحالات والعملات وتطبيق التحويلات المعروفة فقط."""

    status = record.get("status")

    if status not in VALID_STATUSES:
        _add_error(
            error_codes,
            error_details,
            "INVALID_STATUS",
            "status",
            status,
            "حالة الطلب غير موجودة ضمن الحالات المعتمدة",
        )

    old_payment_status = record.get("payment_status")

    if old_payment_status in PAYMENT_STATUS_MAP:
        new_payment_status = PAYMENT_STATUS_MAP[old_payment_status]

        _add_correction(
            corrections,
            "payment_status",
            old_payment_status,
            new_payment_status,
            "NORMALIZE_PAYMENT_STATUS",
        )

        record["payment_status"] = new_payment_status

    if record.get("payment_status") not in VALID_PAYMENT_STATUSES:
        _add_error(
            error_codes,
            error_details,
            "INVALID_PAYMENT_STATUS",
            "payment_status",
            record.get("payment_status"),
            "حالة الدفع غير معروفة",
        )

    old_currency = record.get("currency")

    if old_currency in CURRENCY_MAP:
        new_currency = CURRENCY_MAP[old_currency]

        _add_correction(
            corrections,
            "currency",
            old_currency,
            new_currency,
            "NORMALIZE_CURRENCY",
        )

        record["currency"] = new_currency

    if record.get("currency") not in VALID_CURRENCIES:
        _add_error(
            error_codes,
            error_details,
            "INVALID_CURRENCY",
            "currency",
            record.get("currency"),
            "العملة غير معروفة",
        )


def validate_items(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> list[dict[str, Any]] | None:
    """التحقق من JSON والعناصر والكميات مع تصحيح الكمية الرقمية النصية."""
    field = "items_json"
    raw_items = record.get(field)

    if raw_items is None or not str(raw_items).strip():
        _add_error(
            error_codes,
            error_details,
            "EMPTY_ITEMS",
            field,
            raw_items,
            "قائمة عناصر الطلب فارغة",
        )
        return None

    try:
        items = json.loads(raw_items)
    except (TypeError, json.JSONDecodeError):
        _add_error(
            error_codes,
            error_details,
            "BAD_JSON",
            field,
            raw_items,
            "JSON الخاص بعناصر الطلب غير صالح",
        )
        return None

    if not isinstance(items, list) or not items:
        _add_error(
            error_codes,
            error_details,
            "EMPTY_ITEMS",
            field,
            raw_items,
            "قائمة عناصر الطلب فارغة",
        )
        return None

    qty_changed = False

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            _add_error(
                error_codes,
                error_details,
                "INVALID_ITEM",
                field,
                item,
                "عنصر الطلب ليس كائنًا صالحًا",
            )
            return None

        qty = item.get("qty")

        # تصحيح الكمية إذا كانت رقمًا صحيحًا مكتوبًا كنص
        if isinstance(qty, str):
            normalized_qty = qty.strip().translate(ARABIC_DIGITS)

            if re.fullmatch(r"[+-]?\d+", normalized_qty):
                new_qty = int(normalized_qty)

                _add_correction(
                    corrections,
                    f"items_json[{index}].qty",
                    qty,
                    new_qty,
                    "NORMALIZE_ITEM_QTY",
                )

                item["qty"] = new_qty
                qty = new_qty
                qty_changed = True

        item_total = item.get("total")

        if not isinstance(qty, (int, float)) or qty <= 0:
            _add_error(
                error_codes,
                error_details,
                "INVALID_QTY",
                field,
                item,
                "كمية العنصر يجب أن تكون رقمًا أكبر من صفر",
            )
            return None

        if not isinstance(item_total, (int, float)) or item_total < 0:
            _add_error(
                error_codes,
                error_details,
                "INVALID_ITEM_TOTAL",
                field,
                item,
                "إجمالي العنصر غير صالح",
            )
            return None

    # حفظ JSON المصحح داخل النسخة النظيفة فقط
    if qty_changed:
        record[field] = json.dumps(
            items,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    return items

def recalculate_total(
    record: dict[str, Any],
    items: list[dict[str, Any]] | None,
    corrections: list[dict[str, Any]],
) -> None:
    """إعادة حساب إجمالي الطلب عندما تكون المكونات سليمة."""
    if not items:
        return

    delivery_cost = record.get("delivery_cost")
    old_total = record.get("total_amount")

    if not isinstance(delivery_cost, (int, float)):
        return

    if not isinstance(old_total, (int, float)):
        return

    calculated_total = round(
        sum(float(item["total"]) for item in items)
        + float(delivery_cost),
        2,
    )

    if abs(calculated_total - float(old_total)) > 0.01:
        _add_correction(
            corrections,
            "total_amount",
            old_total,
            calculated_total,
            "RECALCULATE_TOTAL",
        )

        record["total_amount"] = calculated_total


def clean_and_classify(
    raw_record: dict[str, Any],
) -> dict[str, Any]:
    """
    تنظيف سجل واحد وتصنيفه إلى:
    valid أو corrected أو quarantine.
    """
    record = dict(raw_record)

    corrections: list[dict[str, Any]] = []
    error_codes: list[str] = []
    error_details: list[dict[str, Any]] = []

    # القاعدة 1: إزالة المسافات الزائدة
    for field, value in list(record.items()):
        if isinstance(value, str):
            new_value = normalize_text(value)

            _add_correction(
                corrections,
                field,
                value,
                new_value,
                "TRIM_WHITESPACE",
            )

            record[field] = new_value

    # القاعدة 2: المفتاح التجاري إلزامي
    if not record.get("order_id"):
        _add_error(
            error_codes,
            error_details,
            "MISSING_ORDER_ID",
            "order_id",
            record.get("order_id"),
            "رقم الطلب مفقود ولا يمكن إنشاء المفتاح التجاري",
        )

    # القاعدة 3: العميل إلزامي
    if not record.get("customer_id"):
        _add_error(
            error_codes,
            error_details,
            "MISSING_CUSTOMER_ID",
            "customer_id",
            record.get("customer_id"),
            "رقم العميل مفقود",
        )

    # القاعدة 4: توحيد التاريخ
    normalize_date(
        record,
        corrections,
        error_codes,
        error_details,
    )

    # القاعدة 5: توحيد الأرقام المالية
    for money_field in (
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ):
        normalize_money(
            record,
            money_field,
            corrections,
            error_codes,
            error_details,
        )

    # القاعدة 6: القيم المالية السالبة غير آمنة
    if isinstance(record.get("delivery_cost"), (int, float)):
        if record["delivery_cost"] < 0:
            _add_error(
                error_codes,
                error_details,
                "NEGATIVE_DELIVERY_COST",
                "delivery_cost",
                record["delivery_cost"],
                "تكلفة التوصيل سالبة",
            )

    if isinstance(record.get("payment_amount"), (int, float)):
        if record["payment_amount"] < 0:
            _add_error(
                error_codes,
                error_details,
                "NEGATIVE_PAYMENT_AMOUNT",
                "payment_amount",
                record["payment_amount"],
                "قيمة الدفع سالبة",
            )

    if isinstance(record.get("total_amount"), (int, float)):
        if record["total_amount"] < 0:
            _add_error(
                error_codes,
                error_details,
                "NEGATIVE_TOTAL_AMOUNT",
                "total_amount",
                record["total_amount"],
                "إجمالي الطلب سالب",
            )

    # القاعدة 7: توحيد رقم الهاتف
    normalize_phone(
        record,
        corrections,
        error_codes,
        error_details,
    )

    # القاعدة 8: فحص البريد الإلكتروني
    normalize_email(
        record,
        corrections,
        error_codes,
        error_details,
    )

    # القاعدة 9: توحيد القيم التصنيفية
    normalize_enums(
        record,
        corrections,
        error_codes,
        error_details,
    )

    # القاعدة 10: التحقق من JSON والكميات
    items = validate_items(
        record,
        corrections,
        error_codes,
        error_details,
    )

    # القاعدة 11: إعادة حساب الإجمالي عند سلامة المكونات
    recalculate_total(
        record,
        items,
        corrections,
    )

    # تحديد النتيجة النهائية للسجل
    if error_codes:
        outcome = "quarantine"
    elif corrections:
        outcome = "corrected"
    else:
        outcome = "valid"

    return {
        "outcome": outcome,
        "cleaned_record": record,
        "corrections": corrections,
        "error_codes": error_codes,
        "error_details": error_details,
    }
