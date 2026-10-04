from typing import Any

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database, get_mongo_client
from src.phase2_indexes import (
    create_phase2_indexes,
    drop_phase2_indexes,
)


def _has_ixscan(value: Any) -> bool:
    """التحقق مما إذا كانت خطة التنفيذ استخدمت IXSCAN."""

    if isinstance(value, dict):
        if value.get("stage") == "IXSCAN":
            return True

        return any(_has_ixscan(item) for item in value.values())

    if isinstance(value, list):
        return any(_has_ixscan(item) for item in value)

    return False


def _explain_find(
    collection,
    filter_query: dict[str, Any],
    sort: dict[str, int] | None = None,
) -> dict[str, Any]:
    """تشغيل explain مع executionStats على Find Query."""

    command: dict[str, Any] = {
        "find": collection.name,
        "filter": filter_query,
    }

    if sort:
        command["sort"] = sort

    explanation = collection.database.command(
        "explain",
        command,
        verbosity="executionStats",
    )

    execution = explanation["executionStats"]
    winning_plan = explanation["queryPlanner"]["winningPlan"]

    return {
        "n_returned": execution.get("nReturned", 0),
        "docs_examined": execution.get("totalDocsExamined", 0),
        "keys_examined": execution.get("totalKeysExamined", 0),
        "execution_time_ms": execution.get("executionTimeMillis", 0),
        "uses_ixscan": _has_ixscan(winning_plan),
    }


def run_explain_comparison() -> dict[str, Any]:
    """
    مقارنة ثلاث استعلامات قبل وبعد فهارس Phase 2.

    يتم اختيار قيم اختبار فعلية من قاعدة البيانات
    لذلك لا توجد نتائج أو قيم بيانات ثابتة Hardcoded.
    """

    client = get_mongo_client()

    try:
        database = get_database(client)
        collection = database[VALIDATED_COLLECTION]

        sample = collection.find_one(
            {},
            {
                "_id": 0,
                "record_clean.city": 1,
                "record_clean.status": 1,
                "record_clean.customer_id": 1,
                "record_clean.order_date": 1,
            },
        )

        if not sample or "record_clean" not in sample:
            raise RuntimeError(
                "No validated data available for explain tests."
            )

        record = sample["record_clean"]

        queries = {
            "orders_by_city": {
                "filter": {
                    "record_clean.city": record["city"],
                },
                "sort": None,
            },
            "orders_by_status": {
                "filter": {
                    "record_clean.status": record["status"],
                },
                "sort": None,
            },
            "customer_orders_by_date": {
                "filter": {
                    "record_clean.customer_id": record["customer_id"],
                },
                "sort": {
                    "record_clean.order_date": 1,
                },
            },
        }

        drop_phase2_indexes()

        before = {
            name: _explain_find(
                collection,
                spec["filter"],
                spec["sort"],
            )
            for name, spec in queries.items()
        }

        create_phase2_indexes()

        after = {
            name: _explain_find(
                collection,
                spec["filter"],
                spec["sort"],
            )
            for name, spec in queries.items()
        }

        return {
            "before_indexes": before,
            "after_indexes": after,
        }

    finally:
        # ضمان بقاء فهارس Phase 2 موجودة بعد انتهاء الاختبار
        create_phase2_indexes()
        client.close()


if __name__ == "__main__":
    result = run_explain_comparison()

    for query_name in result["before_indexes"]:
        print("=" * 60)
        print(query_name)

        print("BEFORE:")
        for key, value in result["before_indexes"][query_name].items():
            print(f"  {key}: {value}")

        print("AFTER:")
        for key, value in result["after_indexes"][query_name].items():
            print(f"  {key}: {value}")

    print("=" * 60)
    print("EXPLAIN_COMPARISON_COMPLETED")
