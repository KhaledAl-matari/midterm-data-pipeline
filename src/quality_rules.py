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
                "original_value": old_value,
                "corrected_value": new_value,
                "rule_code": rule,
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
    """Normalize monetary values while treating standard numeric CSV strings as already valid."""
    old_value = record.get(field)
    new_value = parse_number(old_value)

    if new_value is None:
        _add_error(
            error_codes,
            error_details,
            "INVALID_MONEY",
            field,
            old_value,
            "Invalid monetary value",
        )
        return

    # CSV naturally reads numeric fields as strings.
    # A standard ASCII numeric string such as "2000.0" is not a correction.
    correction_needed = False

    if not isinstance(old_value, (int, float)):
        raw_text = str(old_value).strip()
        standard_numeric = re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", raw_text)

        if standard_numeric is None:
            correction_needed = True

    if correction_needed:
        translated_text = raw_text.translate(ARABIC_DIGITS)

        if field == "delivery_cost" and translated_text != raw_text:
            rule_code = "arabic_digits_delivery_cost"
        elif field == "payment_amount" and translated_text != raw_text:
            rule_code = "arabic_digits_payment_amount"
        elif field == "total_amount" and "," in raw_text:
            rule_code = "price_with_thousands_commas"
        else:
            rule_code = "normalize_money"

        _add_correction(
            corrections,
            field,
            old_value,
            new_value,
            rule_code,
        )

    record[field] = new_value

def normalize_date(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """Validate and normalize known date formats without marking canonical ISO dates as corrected."""
    field = "order_date"
    old_value = record.get(field)

    if not old_value:
        _add_error(
            error_codes,
            error_details,
            "MISSING_ORDER_DATE",
            field,
            old_value,
            "Missing order date",
        )
        return

    value = str(old_value).strip()

    formats = (
        ("%Y-%m-%dT%H:%M:%S", False),
        ("%Y-%m-%d %H:%M:%S", False),
        ("%d-%m-%Y %H:%M:%S", True),
        ("%d-%m-%Y", True),
        ("%Y/%m/%d %H:%M:%S", True),
    )

    parsed = None
    correction_needed = False

    for fmt, needs_correction in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            correction_needed = needs_correction
            break
        except ValueError:
            continue

    if parsed is None:
        _add_error(
            error_codes,
            error_details,
            "invalid_date_impossible",
            field,
            old_value,
            "Invalid or unsupported order date",
        )
        return

    new_value = parsed.strftime("%Y-%m-%d %H:%M:%S")

    if correction_needed:
        _add_correction(
            corrections,
            field,
            old_value,
            new_value,
            (
                "date_dd_mm_yyyy"
                if re.match(r"^\d{2}-\d{2}-\d{4}", value)
                else "normalize_order_date"
            ),
        )

    record[field] = new_value

def normalize_phone(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """Validate Yemen phone numbers; local 9-digit format is canonical."""
    field = "customer_phone"
    old_value = record.get(field)

    if old_value is None or not str(old_value).strip():
        return

    raw_value = str(old_value).strip()
    value = raw_value.translate(ARABIC_DIGITS)
    value = re.sub(r"[\s\-]", "", value)

    correction_needed = False

    if value.startswith("+967"):
        local_part = value[4:]
        correction_needed = True
    elif value.startswith("00967"):
        local_part = value[5:]
        correction_needed = True
    elif value.startswith("967") and len(value) == 12:
        local_part = value[3:]
        correction_needed = True
    else:
        local_part = value

    if not local_part.isdigit() or len(local_part) != 9:
        _add_error(
            error_codes,
            error_details,
            (
                "invalid_phone_too_short"
                if len(local_part) < 9
                else "invalid_phone"
            ),
            field,
            old_value,
            "Invalid Yemen phone number",
        )
        return

    formatting_changed = local_part != raw_value

    if correction_needed or formatting_changed:
        rule_code = (
            "phone_with_country_code"
            if correction_needed
            else "normalize_phone"
        )

        _add_correction(
            corrections,
            field,
            old_value,
            local_part,
            rule_code,
        )

    record[field] = local_part

def normalize_email(
    record: dict[str, Any],
    corrections: list[dict[str, Any]],
    error_codes: list[str],
    error_details: list[dict[str, Any]],
) -> None:
    """????? ????? ?????? ??????? ???? ???? ???? ?????."""
    field = "customer_email"
    old_value = record.get(field)

    if old_value is None or not str(old_value).strip():
        return

    # ????? ?????? ?????? ???????? ????????.
    value = str(old_value).strip().lower()

    # ????? ??????? ?????? ??? ??? ???? ???????:
    # user@@mail..com -> user@mail.com
    corrected_value = re.sub(r"@{2,}", "@", value)
    corrected_value = re.sub(r"\.{2,}", ".", corrected_value)

    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    # ?? ??? ?? ??? ??? ???? ??? ??????? ????? ??? ????.
    if not re.fullmatch(pattern, corrected_value):
        domain_part = (
            corrected_value.rsplit("@", 1)[-1]
            if "@" in corrected_value
            else ""
        )
        email_error_code = (
            "email_missing_domain"
            if not domain_part or "." not in domain_part
            else "invalid_email"
        )

        _add_error(
            error_codes,
            error_details,
            email_error_code,
            field,
            old_value,
            "???? ?????? ?????????? ??? ????? ??? ???? ??????? ?????",
        )
        return

    if corrected_value != value:
        if "@@" in value:
            rule_code = "email_double_at"
        else:
            rule_code = "normalize_email_symbols"
    else:
        rule_code = "NORMALIZE_EMAIL"

    _add_correction(
        corrections,
        field,
        old_value,
        corrected_value,
        rule_code,
    )

    record[field] = corrected_value


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
            "unknown_order_status",
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
            "currency_arabic_name",
        )

        record["currency"] = new_currency

    if record.get("currency") not in VALID_CURRENCIES:
        _add_error(
            error_codes,
            error_details,
            "unknown_currency",
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
            "empty_items",
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
            "corrupted_items_json",
            field,
            raw_items,
            "JSON الخاص بعناصر الطلب غير صالح",
        )
        return None

    if not isinstance(items, list) or not items:
        _add_error(
            error_codes,
            error_details,
            "empty_items",
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

        sku = item.get("sku")

        if sku is None or not str(sku).strip():
            _add_error(
                error_codes,
                error_details,
                "missing_item_sku",
                f"items_json[{index}].sku",
                sku,
                "Missing item SKU",
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
                    "qty_as_string_in_items",
                )

                item["qty"] = new_qty
                qty = new_qty
                qty_changed = True

        item_total = item.get("total")

        if not isinstance(qty, (int, float)) or qty <= 0:
            _add_error(
                error_codes,
                error_details,
                (
                    "negative_quantity"
                    if isinstance(qty, (int, float)) and qty < 0
                    else "invalid_quantity"
                ),
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
            "total_amount_mismatch_recomputable",
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
                (
                    "status_extra_spaces"
                    if field == "status"
                    else "trim_whitespace"
                ),
            )

            record[field] = new_value

    # القاعدة 2: المفتاح التجاري إلزامي
    if not record.get("order_id"):
        _add_error(
            error_codes,
            error_details,
            "missing_order_id",
            "order_id",
            record.get("order_id"),
            "رقم الطلب مفقود ولا يمكن إنشاء المفتاح التجاري",
        )

    # القاعدة 3: العميل إلزامي
    if not record.get("customer_id"):
        _add_error(
            error_codes,
            error_details,
            "missing_customer_id",
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
    if len(error_codes) > 1:
        error_details.append(
            {
                "code": "multiple_conflicting_errors",
                "field": "multiple_fields",
                "value": list(error_codes),
                "message": "Multiple conflicting validation errors",
            }
        )
        error_codes[:] = ["multiple_conflicting_errors"]

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
