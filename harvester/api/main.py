from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from harvester.api.routes import router
from harvester.config import get_settings
from harvester.db import init_db
from harvester.utils.logging import configure_logging, get_logger

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Harvester API", version=settings.app_version)
    init_db()
    yield
    logger.info("Shutting down Harvester API")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Recursive Crawling Queue Builder — 8-stage document harvesting pipeline",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")


@app.get("/", tags=["root"])
def root():
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "api": "/api/v1",
    }
