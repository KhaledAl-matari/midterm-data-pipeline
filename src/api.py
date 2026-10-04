from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from config.settings import SMALL_FILE_THRESHOLD_MB
from src.batch_loader import load_raw_with_python_batch
from src.elt_pipeline import process_elt_run
from src.file_router import get_file_size_mb, select_engine
from src.metrics import build_run_metrics, save_run_metrics
from src.mongo_setup import get_mongo_client
from src.phase2_aggregations import (
    list_aggregations,
    run_aggregation,
)
from src.phase2_indexes import create_phase2_indexes
from src.phase2_jobs import (
    list_jobs,
    run_job,
    scheduler,
    start_scheduler,
)
from src.phase2_materialized_views import (
    refresh_materialized_views,
)
from src.phase2_queries import (
    customer_orders_by_date,
    high_value_orders,
    list_queries,
    orders_by_city,
    orders_by_payment_method,
    orders_by_status,
)
from src.spark_loader import load_raw_with_pyspark


class IngestRequest(BaseModel):
    input_path: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()

    yield

    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="Big Data Pipeline API",
    version="2.0.0",
    lifespan=lifespan,
)


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Big Data Pipeline Dashboard</title>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f4f7fb;
            color: #172033;
        }
        header {
            background: linear-gradient(135deg, #0f172a, #0f766e);
            color: white;
            padding: 28px 24px;
        }
        header h1 { margin: 0 0 6px; font-size: 28px; }
        header p { margin: 0; opacity: .9; }
        main {
            max-width: 1180px;
            margin: 0 auto;
            padding: 24px;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }
        .card {
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            padding: 18px;
            box-shadow: 0 4px 16px rgba(15, 23, 42, .06);
        }
        .card h2, .card h3 { margin-top: 0; }
        .metric {
            font-size: 30px;
            font-weight: bold;
            margin-top: 10px;
        }
        .ok { color: #047857; }
        .bad { color: #b91c1c; }
        button {
            border: 0;
            border-radius: 9px;
            padding: 10px 14px;
            cursor: pointer;
            background: #0f766e;
            color: white;
            font-weight: 700;
            margin: 4px 4px 4px 0;
        }
        button.secondary { background: #334155; }
        button:hover { opacity: .9; }
        select, input {
            width: 100%;
            padding: 10px;
            border: 1px solid #cbd5e1;
            border-radius: 8px;
            margin: 5px 0 10px;
        }
        pre {
            background: #0f172a;
            color: #e2e8f0;
            padding: 14px;
            border-radius: 10px;
            overflow: auto;
            min-height: 90px;
            white-space: pre-wrap;
        }
        .two-col {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 16px;
        }
        .muted { color: #64748b; font-size: 14px; }
        .pill {
            display: inline-block;
            padding: 5px 9px;
            margin: 3px;
            border-radius: 999px;
            background: #e2e8f0;
            font-size: 13px;
        }
        a { color: #0f766e; font-weight: 700; }
    </style>
</head>
<body>
<header>
    <h1>Big Data Pipeline Dashboard</h1>
    <p>Phase 2 monitoring and manual operation panel</p>
</header>

<main>
    <div class="grid">
        <div class="card">
            <h3>MongoDB</h3>
            <div id="healthStatus" class="metric">Checking...</div>
            <div class="muted">GET /health</div>
        </div>
        <div class="card">
            <h3>Queries</h3>
            <div id="queryCount" class="metric">-</div>
            <div class="muted">Available practical queries</div>
        </div>
        <div class="card">
            <h3>Aggregations</h3>
            <div id="aggregationCount" class="metric">-</div>
            <div class="muted">Available reports</div>
        </div>
        <div class="card">
            <h3>Scheduled Jobs</h3>
            <div id="jobCount" class="metric">-</div>
            <div class="muted">Configured jobs</div>
        </div>
    </div>

    <div class="two-col">
        <section class="card">
            <h2>System Actions</h2>
            <button onclick="createIndexes()">Create Indexes</button>
            <button onclick="refreshMV()">Refresh Materialized Views</button>
            <button class="secondary" onclick="loadOverview()">Refresh Dashboard</button>
            <p class="muted">Uses the existing Phase 2 API endpoints.</p>
            <pre id="actionOutput">Ready.</pre>
        </section>

        <section class="card">
            <h2>Scheduled Jobs</h2>
            <div id="jobsList">Loading...</div>
            <pre id="jobOutput">Choose a job to run manually.</pre>
        </section>
    </div>

    <div class="two-col" style="margin-top:16px;">
        <section class="card">
            <h2>Run Practical Query</h2>

            <label>Query</label>
            <select id="queryName" onchange="showQueryInput()"></select>

            <div id="queryInputs"></div>

            <label>Limit</label>
            <input id="queryLimit" type="number" value="10" min="1" max="1000">

            <button onclick="runQuery()">Run Query</button>
            <pre id="queryOutput">Select a query and run it.</pre>
        </section>

        <section class="card">
            <h2>Run Aggregation Report</h2>

            <label>Aggregation</label>
            <select id="aggregationName"></select>

            <button onclick="runAggregation()">Run Aggregation</button>
            <pre id="aggregationOutput">Select a report and run it.</pre>
        </section>
    </div>

    <section class="card" style="margin-top:16px;">
        <h2>Available Components</h2>
        <h3>Queries</h3>
        <div id="queryList"></div>
        <h3>Aggregations</h3>
        <div id="aggregationList"></div>
        <p><a href="/docs" target="_blank">Open Swagger /docs</a></p>
    </section>
</main>

<script>
function pretty(data) {
    return JSON.stringify(data, null, 2);
}

async function request(url, options = {}) {
    const response = await fetch(url, options);
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.detail || pretty(data));
    }

    return data;
}

async function loadOverview() {
    const healthStatus = document.getElementById("healthStatus");

    try {
        const health = await request("/health");
        healthStatus.textContent = health.mongodb || health.status;
        healthStatus.className = "metric ok";
    } catch (error) {
        healthStatus.textContent = "Error";
        healthStatus.className = "metric bad";
    }

    try {
        const data = await request("/queries");
        const items = data.queries || [];
        document.getElementById("queryCount").textContent = items.length;
        document.getElementById("queryList").innerHTML =
            items.map(x => `<span class="pill">${x}</span>`).join("");

        const select = document.getElementById("queryName");
        select.innerHTML = items.map(x => `<option value="${x}">${x}</option>`).join("");
        showQueryInput();
    } catch (error) {
        document.getElementById("queryCount").textContent = "Error";
    }

    try {
        const data = await request("/aggregations");
        const items = data.aggregations || [];
        document.getElementById("aggregationCount").textContent = items.length;
        document.getElementById("aggregationList").innerHTML =
            items.map(x => `<span class="pill">${x}</span>`).join("");

        document.getElementById("aggregationName").innerHTML =
            items.map(x => `<option value="${x}">${x}</option>`).join("");
    } catch (error) {
        document.getElementById("aggregationCount").textContent = "Error";
    }

    await loadJobs();
}

async function loadJobs() {
    try {
        const data = await request("/jobs");
        const jobs = data.jobs || [];
        document.getElementById("jobCount").textContent = jobs.length;

        document.getElementById("jobsList").innerHTML = jobs.map(job => {
            const name = job.name || "";
            const schedule = job.schedule || "Not scheduled";
            return `
                <div style="margin-bottom:12px;">
                    <strong>${name}</strong><br>
                    <span class="muted">Schedule: ${schedule}</span><br>
                    <button onclick="runJob('${name}')">Run Now</button>
                </div>
            `;
        }).join("");
    } catch (error) {
        document.getElementById("jobCount").textContent = "Error";
        document.getElementById("jobsList").textContent = error.message;
    }
}

async function createIndexes() {
    const out = document.getElementById("actionOutput");
    out.textContent = "Creating indexes...";

    try {
        out.textContent = pretty(await request("/indexes", { method: "POST" }));
    } catch (error) {
        out.textContent = error.message;
    }
}

async function refreshMV() {
    const out = document.getElementById("actionOutput");
    out.textContent = "Refreshing materialized views...";

    try {
        out.textContent = pretty(await request("/refresh-mv", { method: "POST" }));
    } catch (error) {
        out.textContent = error.message;
    }
}

async function runJob(name) {
    const out = document.getElementById("jobOutput");
    out.textContent = `Running ${name}...`;

    try {
        out.textContent = pretty(
            await request(`/jobs/${encodeURIComponent(name)}/run`, { method: "POST" })
        );
        await loadJobs();
    } catch (error) {
        out.textContent = error.message;
    }
}

function showQueryInput() {
    const name = document.getElementById("queryName").value;
    const box = document.getElementById("queryInputs");

    const fields = {
        orders_by_city:
            '<label>City</label><input id="queryValue" placeholder="Example: Sanaa">',
        orders_by_status:
            '<label>Status</label><input id="queryValue" placeholder="Enter status">',
        customer_orders_by_date:
            '<label>Customer ID</label><input id="queryValue" placeholder="Enter customer_id">' +
            '<label>Start Date (optional)</label><input id="startDate" placeholder="YYYY-MM-DD">' +
            '<label>End Date (optional)</label><input id="endDate" placeholder="YYYY-MM-DD">',
        orders_by_payment_method:
            '<label>Payment Method</label><input id="queryValue" placeholder="Example: cash">',
        high_value_orders:
            '<label>Minimum Total</label><input id="queryValue" type="number" value="1000">'
    };

    box.innerHTML = fields[name] || "";
}

async function runQuery() {
    const name = document.getElementById("queryName").value;
    const limit = document.getElementById("queryLimit").value || "10";
    const value = document.getElementById("queryValue")?.value || "";
    const params = new URLSearchParams({ limit });

    if (name === "orders_by_city") {
        params.set("city", value);
    } else if (name === "orders_by_status") {
        params.set("status", value);
    } else if (name === "customer_orders_by_date") {
        params.set("customer_id", value);

        const start = document.getElementById("startDate")?.value;
        const end = document.getElementById("endDate")?.value;

        if (start) params.set("start_date", start);
        if (end) params.set("end_date", end);
    } else if (name === "orders_by_payment_method") {
        params.set("payment_method", value);
    } else if (name === "high_value_orders") {
        params.set("min_total", value);
    }

    const out = document.getElementById("queryOutput");
    out.textContent = "Running query...";

    try {
        out.textContent = pretty(
            await request(`/queries/${encodeURIComponent(name)}?${params.toString()}`)
        );
    } catch (error) {
        out.textContent = error.message;
    }
}

async function runAggregation() {
    const name = document.getElementById("aggregationName").value;
    const out = document.getElementById("aggregationOutput");
    out.textContent = "Running aggregation...";

    try {
        out.textContent = pretty(
            await request(`/aggregations/${encodeURIComponent(name)}`)
        );
    } catch (error) {
        out.textContent = error.message;
    }
}

loadOverview();
</script>
</body>
</html>
"""


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> HTMLResponse:
    return HTMLResponse(content=DASHBOARD_HTML)


@app.get("/health")
def health() -> dict[str, Any]:
    client = get_mongo_client()

    try:
        client.admin.command("ping")

        return {
            "status": "ok",
            "mongodb": "connected",
        }

    finally:
        client.close()


@app.post("/ingest")
def ingest(request: IngestRequest) -> dict[str, Any]:
    input_file = Path(request.input_path)

    if not input_file.exists():
        raise HTTPException(
            status_code=404,
            detail="Input file not found.",
        )

    try:
        engine = select_engine(input_file)

        if engine == "python_batch":
            loader_metrics = load_raw_with_python_batch(input_file)

        elif engine == "pyspark":
            loader_metrics = load_raw_with_pyspark(input_file)

        else:
            raise RuntimeError(f"Unknown engine: {engine}")

        elt_metrics = process_elt_run(
            loader_metrics["id_run"]
        )

        run_metrics = build_run_metrics(
            loader_metrics=loader_metrics,
            elt_metrics=elt_metrics,
            file_size_mb=round(
                get_file_size_mb(input_file),
                2,
            ),
            threshold_mb=SMALL_FILE_THRESHOLD_MB,
        )

        save_run_metrics(run_metrics)

        return run_metrics

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.post("/indexes")
def indexes() -> dict[str, Any]:
    try:
        created = create_phase2_indexes()

        return {
            "status": "ok",
            "indexes": created,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get("/queries")
def queries() -> dict[str, Any]:
    return {
        "queries": list_queries(),
    }


@app.get("/queries/{name}")
def query_by_name(
    name: str,
    city: str | None = None,
    status: str | None = None,
    customer_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    payment_method: str | None = None,
    min_total: float | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:

    try:
        if name == "orders_by_city":
            if city is None:
                raise HTTPException(
                    status_code=400,
                    detail="city is required.",
                )

            data = orders_by_city(city, limit)

        elif name == "orders_by_status":
            if status is None:
                raise HTTPException(
                    status_code=400,
                    detail="status is required.",
                )

            data = orders_by_status(status, limit)

        elif name == "customer_orders_by_date":
            if customer_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="customer_id is required.",
                )

            data = customer_orders_by_date(
                customer_id,
                start_date,
                end_date,
                limit,
            )

        elif name == "orders_by_payment_method":
            if payment_method is None:
                raise HTTPException(
                    status_code=400,
                    detail="payment_method is required.",
                )

            data = orders_by_payment_method(
                payment_method,
                limit,
            )

        elif name == "high_value_orders":
            if min_total is None:
                raise HTTPException(
                    status_code=400,
                    detail="min_total is required.",
                )

            data = high_value_orders(
                min_total,
                limit,
            )

        else:
            raise HTTPException(
                status_code=404,
                detail="Unknown query.",
            )

        return {
            "name": name,
            "count": len(data),
            "results": data,
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get("/aggregations")
def aggregations() -> dict[str, Any]:
    return {
        "aggregations": list_aggregations(),
    }


@app.get("/aggregations/{name}")
def aggregation_by_name(name: str) -> dict[str, Any]:
    try:
        if name not in list_aggregations():
            raise HTTPException(
                status_code=404,
                detail="Unknown aggregation.",
            )

        data = run_aggregation(name)

        return {
            "name": name,
            "count": len(data),
            "results": data,
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.post("/refresh-mv")
def refresh_mv() -> dict[str, Any]:
    try:
        return refresh_materialized_views()

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get("/jobs")
def jobs() -> dict[str, Any]:
    return {
        "jobs": list_jobs(),
    }


@app.post("/jobs/{name}/run")
def job_by_name(name: str) -> dict[str, Any]:
    try:
        available = {
            job["name"]
            for job in list_jobs()
        }

        if name not in available:
            raise HTTPException(
                status_code=404,
                detail="Unknown job.",
            )

        result = run_job(name)

        return {
            "name": name,
            "status": "success",
            "result": result,
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc

