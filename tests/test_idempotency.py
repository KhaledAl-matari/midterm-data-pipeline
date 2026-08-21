from src.quality_rules import clean_and_classify


def test_same_input_produces_same_clean_result():
    row = {
        "order_id": "ORD-IDEMPOTENT-1",
        "order_date": "2025-01-01T12:00:00",
        "status": "مؤكد",
        "customer_id": "C-1",
        "customer_name": "Customer",
        "customer_phone": "714123456",
        "customer_email": "USER@EXAMPLE.COM",
        "city": "صنعاء",
        "district": "اختبار",
        "delivery_type": "عادي",
        "delivery_cost": "1,000.00",
        "payment_method": "نقد",
        "payment_status": "تم الدفع",
        "payment_amount": "3000",
        "currency": "YER",
        "total_amount": "3000",
        "items_json": '[{"sku":"SKU-1","name":"Item","qty":"٢","unit_price":1000.0,"total":2000.0}]',
    }

    first = clean_and_classify(row)
    second = clean_and_classify(row)

    assert first["outcome"] == second["outcome"]
    assert first["cleaned_record"] == second["cleaned_record"]
    assert first["error_codes"] == second["error_codes"]


def test_business_key_remains_stable():
    row = {
        "order_id": "ORD-STABLE-100",
        "order_date": "2025-01-01T12:00:00",
        "status": "مؤكد",
        "customer_id": "C-1",
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

    result = clean_and_classify(row)

    assert result["cleaned_record"]["order_id"] == "ORD-STABLE-100"
