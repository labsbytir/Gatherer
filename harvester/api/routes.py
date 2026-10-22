from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from harvester.api.schemas import (
    DocumentListResponse,
    DocumentResponse,
    HealthResponse,
    JobRunCreate,
    JobRunListResponse,
    JobRunResponse,
    PageResponse,
    PipelineStatusResponse,
    PipelineTriggerRequest,
    SearchRequest,
    SearchResponse,
)
from harvester.config import get_settings
from harvester.db import get_db, init_db
from harvester.models import Document, DocumentStatus, JobRun, JobStatus, Page
from harvester.pipeline.tasks import run_full_pipeline_task
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter()


@router.on_event("startup")
def on_startup():
    init_db()


@router.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    services = {}

    try:
        import redis
        r = redis.from_url(settings.redis_url)
        r.ping()
        services["redis"] = "ok"
    except Exception as e:
        services["redis"] = f"error: {str(e)}"

    try:
        from harvester.crawler import CrawlbergClient
        client = CrawlbergClient()
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(client.health_check())
        services["crawlberg"] = result.get("status", "ok")
    except Exception as e:
        services["crawlberg"] = f"error: {str(e)}"

    try:
        from harvester.storage import StorageClient
        StorageClient()
        services["storage"] = "ok"
    except Exception as e:
        services["storage"] = f"error: {str(e)}"

    return HealthResponse(
        status="ok",
        version=settings.app_version,
        services=services,
    )


@router.post("/jobs", response_model=JobRunResponse, tags=["jobs"], status_code=201)
def create_job(
    job_data: JobRunCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new harvest job run with seed URLs.
    """
    if job_data.job_run_id is not None:
        existing = db.query(JobRun).filter(JobRun.job_run_id == job_data.job_run_id).first()
        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"Job run ID {job_data.job_run_id} already exists",
            )
        job_run_id = job_data.job_run_id
    else:
        last_job = db.query(func.max(JobRun.job_run_id)).scalar() or 0
        job_run_id = last_job + 1

    job = JobRun(
        job_run_id=job_run_id,
        name=job_data.name,
        seed_urls=job_data.seed_urls,
        max_depth=job_data.max_depth,
        max_pages=job_data.max_pages,
        config=job_data.config,
        status=JobStatus.PENDING,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    logger.info("Job created", job_run_id=job_run_id, name=job_data.name)

    return JobRunResponse.model_validate(job)


@router.get("/jobs", response_model=JobRunListResponse, tags=["jobs"])
def list_jobs(
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all job runs with optional status filter.
    """
    query = select(JobRun).order_by(JobRun.created_at.desc())
    if status:
        query = query.where(JobRun.status == status)

    total = db.query(func.count(JobRun.id)).scalar()
    if status:
        total = db.query(func.count(JobRun.id)).where(JobRun.status == status).scalar()

    jobs = db.execute(query.offset(offset).limit(limit)).scalars().all()

    return JobRunListResponse(
        total=total or 0,
        items=[JobRunResponse.model_validate(j) for j in jobs],
    )


@router.get("/jobs/{job_run_id}", response_model=JobRunResponse, tags=["jobs"])
def get_job(job_run_id: int, db: Session = Depends(get_db)):
    """
    Get a specific job run by its run ID.
    """
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job run {job_run_id} not found")
    return JobRunResponse.model_validate(job)


@router.post("/jobs/{job_run_id}/start", response_model=PipelineStatusResponse, tags=["jobs"])
def start_job(job_run_id: int, db: Session = Depends(get_db)):
    """
    Start the full 8-stage pipeline for a job run.
    """
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job run {job_run_id} not found")

    if job.status not in (JobStatus.PENDING, JobStatus.FAILED, JobStatus.CANCELLED):
        raise HTTPException(
            status_code=400,
            detail=f"Job is already in status: {job.status.value}",
        )

    result = run_full_pipeline_task.delay(job_run_id, job.seed_urls)

    logger.info("Pipeline started", job_run_id=job_run_id, task_id=result.id)

    return PipelineStatusResponse(
        job_run_id=job_run_id,
        celery_task_id=result.id,
        status="dispatched",
    )


@router.get("/jobs/{job_run_id}/pages", response_model=list[PageResponse], tags=["jobs"])
def get_job_pages(
    job_run_id: int,
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get all pages discovered/indexed for a job run.
    """
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job run {job_run_id} not found")

    query = select(Page).where(Page.job_run_id == job.id).order_by(Page.created_at.desc())
    if status:
        query = query.where(Page.status == status)

    pages = db.execute(query.offset(offset).limit(limit)).scalars().all()
    return [PageResponse.model_validate(p) for p in pages]


@router.get("/jobs/{job_run_id}/documents", response_model=DocumentListResponse, tags=["jobs"])
def get_job_documents(
    job_run_id: int,
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get all documents for a job run.
    """
    job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job run {job_run_id} not found")

    query = select(Document).where(Document.job_run_id == job.id).order_by(Document.created_at.desc())
    if status:
        query = query.where(Document.status == status)

    total_query = select(func.count(Document.id)).where(Document.job_run_id == job.id)
    if status:
        total_query = total_query.where(Document.status == status)
    total = db.execute(total_query).scalar() or 0

    docs = db.execute(query.offset(offset).limit(limit)).scalars().all()

    return DocumentListResponse(
        total=total,
        items=[DocumentResponse.model_validate(d) for d in docs],
    )


@router.get("/documents/{document_id}", response_model=DocumentResponse, tags=["documents"])
def get_document(document_id: str, db: Session = Depends(get_db)):
    """
    Get a specific document by ID.
    """
    doc = db.query(Document).filter(Document.id == document_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail=f"Document {document_id} not found")
    return DocumentResponse.model_validate(doc)


@router.get("/documents/{document_id}/download", tags=["documents"])
def download_document_file(document_id: str, db: Session = Depends(get_db)):
    """
    Get a presigned URL to download the original document file.
    """
    from fastapi.responses import RedirectResponse

    doc = db.query(Document).filter(Document.id == document_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail=f"Document {document_id} not found")

    if not doc.s3_original_key:
        raise HTTPException(status_code=404, detail="Document file not stored")

    from harvester.storage import StorageClient
    storage = StorageClient()
    url = storage.generate_presigned_url(doc.s3_original_key, expires_in=3600)
    return RedirectResponse(url=url)


@router.post("/search", response_model=SearchResponse, tags=["search"])
def search_documents(
    request: SearchRequest,
    db: Session = Depends(get_db),
):
    """
    Semantic search across document chunks using local embeddings.
    """
    from harvester.embeddings import EmbeddingService

    embedder = EmbeddingService()
    query_embedding = embedder._get_model().encode(
        [request.query], convert_to_numpy=True
    )[0]

    query = select(Document).where(
        Document.status == DocumentStatus.STORED,
    )
    if request.job_run_id:
        job = db.query(JobRun).filter(JobRun.job_run_id == request.job_run_id).first()
        if job:
            query = query.where(Document.job_run_id == job.id)

    docs = db.execute(query.limit(1000)).scalars().all()

    results = []
    for doc in docs:
        if not doc.s3_embeddings_key:
            continue
        try:
            from harvester.storage import StorageClient
            storage = StorageClient()
            embeddings_json = storage.download_text(doc.s3_embeddings_key)
            embeddings_data = json.loads(embeddings_json)

            import numpy as np
            for chunk in embeddings_data.get("chunks", []):
                chunk_vec = np.array(chunk["embedding"])
                score = float(np.dot(query_embedding, chunk_vec) / (
                    np.linalg.norm(query_embedding) * np.linalg.norm(chunk_vec) + 1e-10
                ))
                results.append({
                    "document_id": doc.id,
                    "source_url": doc.source_url,
                    "chunk_text": chunk["text"][:500],
                    "score": score,
                })
        except Exception as e:
            logger.warning("Failed to load embeddings for search", doc_id=doc.id, error=str(e))

    results.sort(key=lambda x: x["score"], reverse=True)
    results = results[:request.limit]

    return SearchResponse(
        query=request.query,
        results=[
            {"document_id": r["document_id"], "source_url": r["source_url"],
             "chunk_text": r["chunk_text"], "score": r["score"]}
            for r in results
        ],
    )


import json
