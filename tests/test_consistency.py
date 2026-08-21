from src.quality_rules import clean_and_classify


def make_record(order_id, bad=False):
    return {
        "order_id": order_id,
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
        "items_json": (
            "{bad json"
            if bad
            else '[{"sku":"SKU-1","name":"Item","qty":2,"unit_price":1000.0,"total":2000.0}]'
        ),
    }


def test_every_record_has_exactly_one_outcome():
    records = [
        make_record("ORD-1"),
        make_record("ORD-2", bad=True),
        make_record("ORD-3"),
        make_record("ORD-4", bad=True),
    ]

    results = [clean_and_classify(row) for row in records]

    assert len(results) == len(records)

    for result in results:
        assert result["outcome"] in {
            "valid",
            "corrected",
            "quarantine",
        }

    outcome_count = sum(
        1
        for result in results
        if result["outcome"] in {
            "valid",
            "corrected",
            "quarantine",
        }
    )

    assert outcome_count == len(records)
