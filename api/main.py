"""FastAPI application entrypoint."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings
from api.routers.asn import (
    router as asn_router,
)
from api.routers.health import (
    router as health_router,
)
from api.routers.incidents import (
    router as incidents_router,
)
from api.routers.measurements import (
    router as measurements_router,
)
from api.routers.probes import (
    router as probes_router,
)
from api.routers.quality import (
    router as quality_router,
)


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "API for Vietnam Internet Quality "
        "and Network Anomaly Monitoring."
    ),
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(
    health_router,
)

app.include_router(
    measurements_router,
)

app.include_router(
    quality_router,
)

app.include_router(
    incidents_router,
)

app.include_router(
    probes_router,
)

app.include_router(
    asn_router,
)


@app.get(
    "/",
    tags=["root"],
)
def root():
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/health",
    }