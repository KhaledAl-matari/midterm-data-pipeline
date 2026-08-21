from src.quality_rules import clean_and_classify, parse_number


def test_parse_arabic_money():
    assert parse_number("٢٦٠٠٠٫٠") == 26000.0


def test_parse_money_with_currency():
    assert parse_number("32,000.00 ريال") == 32000.0


def test_normalize_text_qty():
    row = {
        "order_id": "ORD-1",
        "order_date": "2025-01-01T10:00:00",
        "status": "مؤكد",
        "customer_id": "C-1",
        "customer_name": "Test",
        "customer_phone": "714123456",
        "customer_email": "test@example.com",
        "city": "صنعاء",
        "district": "اختبار",
        "delivery_type": "عادي",
        "delivery_cost": "1000",
        "payment_method": "نقد",
        "payment_status": "تم الدفع",
        "payment_amount": "3000",
        "currency": "YER",
        "total_amount": "3000",
        "items_json": '[{"sku":"SKU-1","name":"X","qty":"٢","unit_price":1000.0,"total":2000.0}]',
    }

    result = clean_and_classify(row)

    assert "INVALID_QTY" not in result["error_codes"]
    assert result["cleaned_record"]["total_amount"] == 3000.0


def test_invalid_date_goes_to_quarantine():
    row = {
        "order_id": "ORD-2",
        "order_date": "2025-19-45 99:70:00",
        "status": "مؤكد",
        "customer_id": "C-2",
        "customer_name": "Test",
        "customer_phone": "714123456",
        "customer_email": "test@example.com",
        "city": "صنعاء",
        "district": "اختبار",
        "delivery_type": "عادي",
        "delivery_cost": "1000",
        "payment_method": "نقد",
        "payment_status": "تم الدفع",
        "payment_amount": "3000",
        "currency": "YER",
        "total_amount": "3000",
        "items_json": '[{"sku":"SKU-1","name":"X","qty":2,"unit_price":1000.0,"total":2000.0}]',
    }

    result = clean_and_classify(row)

    assert result["outcome"] == "quarantine"
    assert "INVALID_ORDER_DATE" in result["error_codes"]
