"""Factory app: import is inert; startup requires the exact approved manifest."""


def create_app():
    import os
    from processing.e2e.guards import gate_c, load_manifest, runtime_env, require
    path = os.environ["VIQ_E2E_MANIFEST"]; m = load_manifest(path)
    gate_c(m, path, os.environ["VIQ_E2E_APPROVAL"], "start")
    runtime_env(m, role="api")
    from fastapi import FastAPI, HTTPException, Query
    from api.routers.quality import router as quality_router
    from api.routers.health import router as health_router
    from api.clickhouse import clickhouse
    from api.config import settings
    require(settings.clickhouse_database == m["resources"]["database"] and
            settings.clickhouse_user == m["resources"]["ch_api_user"], "API_SETTINGS_ALREADY_IMPORTED_WITH_WRONG_DB")
    app = FastAPI(title="VIQ isolated historical replay to API", version="1.0")
    app.include_router(quality_router); app.include_router(health_router)

    @app.middleware("http")
    async def bind_run(request, call_next):
        response = await call_next(request)
        response.headers["X-VIQ-E2E-Run-ID"] = m["run_id"]
        return response

    @app.get("/e2e/run")
    def run_receipt(expected_run_id: str = Query(...)):
        if expected_run_id != m["run_id"]:
            raise HTTPException(409, "Run ID mismatch")
        fields = ("e2e_run_id,rid,raw_sha256,gold_manifest_sha256,schema_sha256,"
                  "row_count,sample_count,status")
        rows = clickhouse.query_json("SELECT " + fields + " FROM run_receipts")
        if not rows:
            raise HTTPException(404, "No verified result yet")
        if len(rows) != 1 or rows[0]["e2e_run_id"] != m["run_id"] or rows[0]["status"] != "verified":
            raise HTTPException(409, "Receipt binding failed")
        row = rows[0]
        for key in ("row_count", "sample_count"):
            row[key] = int(row[key])
        return row

    return app
