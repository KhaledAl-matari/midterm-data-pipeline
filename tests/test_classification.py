from src.quality_rules import clean_and_classify


def base_record():
    return {
        "order_id": "ORD-100",
        "order_date": "2025-01-01T12:00:00",
        "status": "مؤكد",
        "customer_id": "C-100",
        "customer_name": "Customer",
        "customer_phone": "714123456",
        "customer_email": "user@example.com",
        "city": "صنعاء",
        "district": "اختبار",
        "delivery_type": "عادي",
        "delivery_cost": "1000",
        "payment_method": "نقد",
        "payment_status": "تم الدفع",
        "payment_amount": "3000",
        "currency": "YER",
        "total_amount": "3000",
        "items_json": '[{"sku":"SKU-1","name":"Item","qty":2,"unit_price":1000.0,"total":2000.0}]',
    }


def test_correctable_record():
    result = clean_and_classify(base_record())

    assert result["outcome"] == "corrected"
    assert result["error_codes"] == []
    assert len(result["corrections"]) > 0


def test_bad_json_is_quarantined():
    row = base_record()
    row["items_json"] = "{bad json"

    result = clean_and_classify(row)

    assert result["outcome"] == "quarantine"
    assert "BAD_JSON" in result["error_codes"]


def test_missing_order_id_is_quarantined():
    row = base_record()
    row["order_id"] = ""

    result = clean_and_classify(row)

    assert result["outcome"] == "quarantine"
    assert "MISSING_ORDER_ID" in result["error_codes"]


def test_negative_quantity_is_quarantined():
    row = base_record()
    row["items_json"] = '[{"sku":"SKU-1","name":"Item","qty":-2,"unit_price":1000.0,"total":2000.0}]'

    result = clean_and_classify(row)

    assert result["outcome"] == "quarantine"
    assert "INVALID_QTY" in result["error_codes"]

def test_valid_record_needs_no_correction():
    from src.quality_rules import VALID_STATUSES, VALID_PAYMENT_STATUSES

    row = {
        "order_id": "ORD-VALID-1",
        "order_date": "2025-01-01 12:00:00",
        "status": next(iter(VALID_STATUSES)),
        "customer_id": "C-VALID-1",
        "customer_name": "Customer",
        "customer_phone": "+967714123456",
        "customer_email": "user@example.com",
        "city": "Sanaa",
        "district": "Test",
        "delivery_type": "normal",
        "delivery_cost": 1000.0,
        "payment_method": "cash",
        "payment_status": next(iter(VALID_PAYMENT_STATUSES)),
        "payment_amount": 3000.0,
        "currency": "YER",
        "total_amount": 3000.0,
        "items_json": '[{"sku":"SKU-1","name":"Item","qty":2,"unit_price":1000.0,"total":2000.0}]',
    }

    result = clean_and_classify(row)

    assert result["outcome"] == "valid"
    assert result["corrections"] == []
    assert result["error_codes"] == []

