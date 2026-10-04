from datetime import datetime, timezone
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler

from src.mongo_setup import get_database, get_mongo_client
from src.phase2_materialized_views import refresh_materialized_views
from src.phase2_aggregations import run_aggregation, list_aggregations


JOB_LOG_COLLECTION = "phase2_job_logs"
AGGREGATION_SNAPSHOT_COLLECTION = "phase2_aggregation_snapshots"

scheduler = BackgroundScheduler()


def _log_job(
    job_name: str,
    status: str,
    started_at: datetime,
    ended_at: datetime | None = None,
    details: Any = None,
) -> None:
    """حفظ سجل تشغيل الـJob داخل MongoDB."""

    client = get_mongo_client()

    try:
        database = get_database(client)

        database[JOB_LOG_COLLECTION].insert_one(
            {
                "job_name": job_name,
                "status": status,
                "started_at": started_at,
                "ended_at": ended_at,
                "details": details,
            }
        )

    finally:
        client.close()


def refresh_materialized_views_job() -> dict[str, Any]:
    """Job لتحديث الـMaterialized Views."""

    job_name = "refresh_materialized_views"
    started_at = datetime.now(timezone.utc)

    _log_job(
        job_name,
        "started",
        started_at,
    )

    try:
        result = refresh_materialized_views()

        _log_job(
            job_name,
            "success",
            started_at,
            datetime.now(timezone.utc),
            result,
        )

        return result

    except Exception as exc:
        _log_job(
            job_name,
            "failure",
            started_at,
            datetime.now(timezone.utc),
            {"error": str(exc)},
        )
        raise


def aggregation_snapshot_job() -> dict[str, Any]:
    """Job لتشغيل التقارير الخمسة وحفظ Snapshot منها."""

    job_name = "aggregation_snapshot"
    started_at = datetime.now(timezone.utc)

    _log_job(
        job_name,
        "started",
        started_at,
    )

    try:
        reports = {
            name: run_aggregation(name)
            for name in list_aggregations()
        }

        snapshot = {
            "created_at": datetime.now(timezone.utc),
            "reports": reports,
        }

        client = get_mongo_client()

        try:
            database = get_database(client)

            database[
                AGGREGATION_SNAPSHOT_COLLECTION
            ].insert_one(snapshot)

        finally:
            client.close()

        result = {
            "reports_generated": len(reports),
            "created_at": snapshot["created_at"],
        }

        _log_job(
            job_name,
            "success",
            started_at,
            datetime.now(timezone.utc),
            result,
        )

        return result

    except Exception as exc:
        _log_job(
            job_name,
            "failure",
            started_at,
            datetime.now(timezone.utc),
            {"error": str(exc)},
        )
        raise


JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "function": refresh_materialized_views_job,
        "schedule": "every 30 minutes",
    },
    "aggregation_snapshot": {
        "function": aggregation_snapshot_job,
        "schedule": "daily at 23:00",
    },
}


def configure_scheduler() -> None:
    """إضافة الـJobs إلى APScheduler."""

    if scheduler.get_job("refresh_materialized_views") is None:
        scheduler.add_job(
            refresh_materialized_views_job,
            trigger="interval",
            minutes=30,
            id="refresh_materialized_views",
            replace_existing=True,
        )

    if scheduler.get_job("aggregation_snapshot") is None:
        scheduler.add_job(
            aggregation_snapshot_job,
            trigger="cron",
            hour=23,
            minute=0,
            id="aggregation_snapshot",
            replace_existing=True,
        )


def start_scheduler() -> None:
    """تشغيل الـScheduler."""

    configure_scheduler()

    if not scheduler.running:
        scheduler.start()


def list_jobs() -> list[dict[str, Any]]:
    """إرجاع أسماء الـJobs والجداول الزمنية."""

    return [
        {
            "name": name,
            "schedule": spec["schedule"],
        }
        for name, spec in JOB_DEFINITIONS.items()
    ]


def run_job(name: str) -> dict[str, Any]:
    """تشغيل Job يدويا بالاسم."""

    if name not in JOB_DEFINITIONS:
        raise ValueError(f"Unknown job: {name}")

    return JOB_DEFINITIONS[name]["function"]()
