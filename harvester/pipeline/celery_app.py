from celery import Celery

from harvester.config import get_settings
from harvester.utils.logging import configure_logging

settings = get_settings()
configure_logging(settings.celery_log_level)

celery_app = Celery(
    "harvester",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["harvester.pipeline.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3300,
    worker_prefetch_multiplier=1,
    worker_concurrency=settings.celery_concurrency,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=86400,
    broker_connection_retry_on_startup=True,
    task_routes={
        "harvester.pipeline.tasks.discover_pages_task": {"queue": "discovery"},
        "harvester.pipeline.tasks.index_pages_task": {"queue": "indexing"},
        "harvester.pipeline.tasks.extract_page_task": {"queue": "extraction"},
        "harvester.pipeline.tasks.download_document_task": {"queue": "download"},
        "harvester.pipeline.tasks.ocr_document_task": {"queue": "ocr"},
        "harvester.pipeline.tasks.transform_document_task": {"queue": "transform"},
        "harvester.pipeline.tasks.embed_document_task": {"queue": "embedding"},
        "harvester.pipeline.tasks.store_document_task": {"queue": "storage"},
        "harvester.pipeline.tasks.validate_job_task": {"queue": "validation"},
        "harvester.pipeline.tasks.run_full_pipeline_task": {"queue": "orchestration"},
    },
)
