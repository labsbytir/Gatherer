from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from harvester.api.schemas import (
    HealthResponse,
    JobRunCreate,
    JobRunResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    QueueItemListResponse,
    QueueItemResponse,
    StageRunRequest,
)
from harvester.config import get_settings
from harvester.db import get_db
from harvester.models import JobRun, QueueItem, Stage
from harvester.queue import QueueManager
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter()

STAGE_MAP = {
    "discover": "discover",
    "index": "index",
    "extract": "extract",
    "ocr": "ocr",
    "transform": "transform",
    "store": "store",
    "validate": "validate",
}


@router.get("/health", response_model=HealthResponse, tags=["health"])
async def health():
    services = {}
    try:
        from harvester.crawler import CrawlbergQueue
        q = CrawlbergQueue()
        import asyncio
        r = await q.health()
        services["crawlberg"] = r.get("status", "ok")
    except Exception as e:
        services["crawlberg"] = f"error: {str(e)}"

    try:
        from harvester.storage import StorageClient
        StorageClient()
        services["storage"] = "ok"
    except Exception as e:
        services["storage"] = f"error: {str(e)}"

    return HealthResponse(status="ok", version=settings.app_version, services=services)


@router.post("/jobs", response_model=JobRunResponse, tags=["jobs"], status_code=201)
def create_job(job_data: JobRunCreate, db: Session = Depends(get_db)):
    existing = db.query(JobRun).filter(JobRun.job_run_id == job_data.job_run_id).first()
    if existing:
        raise HTTPException(409, f"Job run {job_data.job_run_id} already exists")

    job = JobRun(
        job_run_id=job_data.job_run_id,
        name=job_data.name,
        seed_urls=job_data.seed_urls,
        max_depth=job_data.max_depth,
        max_pages=job_data.max_pages,
        config=job_data.config,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return JobRunResponse.model_validate(job)


@router.get("/jobs", response_model=list[JobRunResponse], tags=["jobs"])
def list_jobs(
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = select(JobRun).order_by(JobRun.created_at.desc())
    if status:
        q = q.where(JobRun.status == status)
    jobs = db.execute(q.limit(limit)).scalars().all()
    return [JobRunResponse.model_validate(j) for j in jobs]


@router.get("/jobs/{job_run_id}", response_model=JobRunResponse, tags=["jobs"])
def get_job(job_run_id: int, db: Session = Depends(get_db)):
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(404, f"Job run {job_run_id} not found")
    return JobRunResponse.model_validate(job)


@router.post("/jobs/{job_run_id}/run", response_model=PipelineRunResponse, tags=["jobs"])
def run_pipeline(
    job_run_id: int,
    request: PipelineRunRequest,
    db: Session = Depends(get_db),
):
    """
    Run the full 8-stage pipeline for a job run.
    Creates the job run if it doesn't exist (default ID 10).
    """
    with QueueManager(db) as qm:
        result = qm.run_full_pipeline(
            job_run_id=request.job_run_id,
            seed_urls=request.seed_urls,
            name=request.name,
        )
    return PipelineRunResponse(
        job_run_id=result["job_run"]["job_run_id"],
        status=result["job_run"]["status"],
        discovered=result["discovered"],
        indexed=result["indexed"],
        extracted=result["extracted"],
        ocr=result["ocr"],
        transformed=result["transformed"],
        stored=result["stored"],
        validation=result["validation"],
    )


@router.post("/jobs/{job_run_id}/stage", tags=["jobs"])
def run_stage(
    job_run_id: int,
    request: StageRunRequest,
    db: Session = Depends(get_db),
):
    """
    Run a single stage of the pipeline.
    """
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(404, f"Job run {job_run_id} not found")

    stage_name = request.stage.lower()
    if stage_name not in STAGE_MAP:
        raise HTTPException(400, f"Unknown stage: {request.stage}. Valid: {list(STAGE_MAP.keys())}")

    with QueueManager(db) as qm:
        if stage_name == "discover":
            count = qm.stage_1_discover(job)
        elif stage_name == "index":
            count = qm.stage_2_3_index_and_create_jobs(job)
        elif stage_name == "extract":
            count = qm.stage_4_extract(job)
        elif stage_name == "ocr":
            count = qm.stage_5_ocr(job)
        elif stage_name == "transform":
            count = qm.stage_6_transform(job)
        elif stage_name == "store":
            count = qm.stage_7_store(job)
        elif stage_name == "validate":
            result = qm.stage_8_validate(job)
            return result
        else:
            raise HTTPException(400, f"Unknown stage: {stage_name}")

    return {"job_run_id": job_run_id, "stage": stage_name, "processed": count}


@router.get("/jobs/{job_run_id}/items", response_model=QueueItemListResponse, tags=["jobs"])
def get_job_items(
    job_run_id: int,
    stage: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    is_document: Optional[bool] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(404, f"Job run {job_run_id} not found")

    q = select(QueueItem).where(QueueItem.job_run_id == job.id)
    if stage:
        q = q.where(QueueItem.current_stage == stage)
    if status:
        q = q.where(QueueItem.overall_status == status)
    if is_document is not None:
        q = q.where(QueueItem.is_document == is_document)

    total = db.execute(select(func.count(QueueItem.id)).where(QueueItem.job_run_id == job.id)).scalar() or 0

    items = db.execute(q.order_by(QueueItem.created_at.desc()).offset(offset).limit(limit)).scalars().all()
    return QueueItemListResponse(
        total=total,
        items=[QueueItemResponse.model_validate(i) for i in items],
    )


@router.get("/items/{item_id}", response_model=QueueItemResponse, tags=["items"])
def get_item(item_id: str, db: Session = Depends(get_db)):
    item = db.query(QueueItem).filter(QueueItem.id == item_id).first()
    if not item:
        raise HTTPException(404, f"Item {item_id} not found")
    return QueueItemResponse.model_validate(item)


@router.get("/items/{item_id}/download", tags=["items"])
def download_item(item_id: str, db: Session = Depends(get_db)):
    from fastapi.responses import RedirectResponse

    item = db.query(QueueItem).filter(QueueItem.id == item_id).first()
    if not item:
        raise HTTPException(404, f"Item {item_id} not found")
    if not item.s3_original_key:
        raise HTTPException(404, "File not stored")

    from harvester.storage import StorageClient
    storage = StorageClient()
    url = storage.presigned_url(item.s3_original_key)
    return RedirectResponse(url=url)
