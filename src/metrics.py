import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RESULTS_FILE,
)


def _json_default(value: Any):
    """تحويل الأنواع غير المدعومة مباشرة في JSON."""
    if isinstance(value, datetime):
        return value.isoformat()

    return str(value)


def collect_error_counts(id_run: str) -> dict[str, int]:
    """حساب تكرار أسباب الحجر الخاصة بتشغيل محدد."""
    client = None

    try:
        client = MongoClient(MONGO_URI)
        collection = client[MONGO_DATABASE][QUARANTINE_COLLECTION]

        pipeline = [
            {"$match": {"id_run": id_run}},
            {"$unwind": "$error_codes"},
            {
                "$group": {
                    "_id": "$error_codes",
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"count": -1}},
        ]

        return {
            item["_id"]: item["count"]
            for item in collection.aggregate(pipeline)
        }

    finally:
        if client is not None:
            client.close()


def build_run_metrics(
    loader_metrics: dict[str, Any],
    elt_metrics: dict[str, Any],
    file_size_mb: float | None = None,
    threshold_mb: float | None = None,
) -> dict[str, Any]:
    """دمج مقاييس التحميل والـ ELT في سجل تشغيل واحد."""

    id_run = loader_metrics["id_run"]

    result = {
        "id_run": id_run,
        "run_id": id_run,
        "generated_at": datetime.now(timezone.utc),
        "input": {
            "file_name": loader_metrics.get("file_name"),
            "file_size_mb": file_size_mb,
            "threshold_mb": threshold_mb,
            "engine": loader_metrics.get("used_engine"),
        },
        "raw_load": {
            "rows": loader_metrics.get(
                "loaded_raw",
                loader_metrics.get("read_rows", 0),
            ),
            "partitions": loader_metrics.get("partitions"),
            "seconds_elapsed": loader_metrics.get("seconds_elapsed"),
            "write_seconds": loader_metrics.get("write_seconds"),
            "throughput": loader_metrics.get("throughput"),
        },
        "quality": {
            "valid": elt_metrics.get("valid", 0),
            "corrected": elt_metrics.get("corrected", 0),
            "quarantined": elt_metrics.get("quarantined", 0),
            "error_counts": collect_error_counts(id_run),
        },
        "upsert": {
            "inserted": elt_metrics.get("inserted", 0),
            "updated": elt_metrics.get("updated", 0),
            "unchanged": elt_metrics.get("unchanged", 0),
        },
        "elt": {
            "processed": elt_metrics.get("processed", 0),
            "seconds_elapsed": elt_metrics.get("seconds_elapsed"),
            "throughput": elt_metrics.get("throughput"),
        },
    }

    raw_rows = result["raw_load"]["rows"] or 0

    outcome_rows = (
        result["quality"]["valid"]
        + result["quality"]["corrected"]
        + result["quality"]["quarantined"]
    )

    result["consistency"] = {
        "raw_rows": raw_rows,
        "outcome_rows": outcome_rows,
        "pass": raw_rows == outcome_rows,
    }

    # ???????? ???????? ?????? ??? ???? ?? ????? ???????.
    raw_seconds = loader_metrics.get("seconds_elapsed") or 0
    elt_seconds = elt_metrics.get("seconds_elapsed") or 0
    total_seconds = raw_seconds + elt_seconds

    result.update({
        "file_name": loader_metrics.get("file_name"),
        "file_size_mb": file_size_mb,
        "engine_used": loader_metrics.get("used_engine"),
        "rows_read": loader_metrics.get("read_rows", 0),
        "raw_loaded": loader_metrics.get("loaded_raw", 0),
        "valid_count": elt_metrics.get("valid", 0),
        "corrected_count": elt_metrics.get("corrected", 0),
        "quarantine_count": elt_metrics.get("quarantined", 0),
        "elapsed_seconds": round(total_seconds, 4),
        "throughput": (
            round((loader_metrics.get("read_rows", 0) or 0) / total_seconds, 2)
            if total_seconds > 0 else 0
        ),
        "batch_size": loader_metrics.get("batch_size", loader_metrics.get("size_batch")),
        "partitions": loader_metrics.get("partitions"),
        "error_case_counts": result["quality"]["error_counts"],
        "inserted_count": elt_metrics.get("inserted", 0),
        "updated_count": elt_metrics.get("updated", 0),
        "unchanged_count": elt_metrics.get("unchanged", 0),
    })

    return result


def save_run_metrics(
    run_metrics: dict[str, Any],
    output_path: Path = RESULTS_FILE,
) -> None:
    """
    حفظ نتيجة التشغيل داخل results.json.
    يحتفظ الملف بتاريخ جميع التشغيلات السابقة.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    existing_runs = []

    if output_path.exists() and output_path.stat().st_size > 0:
        try:
            existing_data = json.loads(
                output_path.read_text(encoding="utf-8")
            )

            if isinstance(existing_data, dict):
                existing_runs = existing_data.get("runs", [])

            elif isinstance(existing_data, list):
                existing_runs = existing_data

        except json.JSONDecodeError:
            existing_runs = []

    # تحديث نفس id_run بدل تكراره داخل ملف النتائج
    id_run = run_metrics["id_run"]

    existing_runs = [
        run
        for run in existing_runs
        if run.get("id_run") != id_run
    ]

    existing_runs.append(run_metrics)

    document = {
        "updated_at": datetime.now(timezone.utc),
        "total_runs": len(existing_runs),
        "runs": existing_runs,
    }

    temp_path = output_path.with_suffix(".tmp")

    temp_path.write_text(
        json.dumps(
            document,
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        ),
        encoding="utf-8",
    )

    temp_path.replace(output_path)

    print(f"تم حفظ المقاييس في: {output_path}")
