from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class JobRunCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    seed_urls: list[str] = Field(..., min_length=1)
    job_run_id: Optional[int] = Field(None, description="Custom job run ID (auto-assigned if not provided)")
    max_depth: int = Field(3, ge=0, le=10)
    max_pages: int = Field(1000, ge=1, le=100000)
    config: dict = Field(default_factory=dict)


class JobRunResponse(BaseModel):
    id: str
    job_run_id: int
    name: str
    status: str
    seed_urls: list[str]
    max_depth: int
    max_pages: int
    total_pages_discovered: int
    total_pages_processed: int
    total_documents_downloaded: int
    total_documents_ocr: int
    total_documents_stored: int
    crawlberg_crawl_job_id: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobRunListResponse(BaseModel):
    total: int
    items: list[JobRunResponse]


class PageResponse(BaseModel):
    id: str
    url: str
    normalized_url: str
    depth: int
    status: str
    title: Optional[str]
    content_type: Optional[str]
    http_status: Optional[int]
    discovered_document_urls: list
    created_at: datetime

    model_config = {"from_attributes": True}


class DocumentResponse(BaseModel):
    id: str
    source_url: str
    filename: Optional[str]
    content_type: Optional[str]
    file_size: Optional[int]
    file_hash: Optional[str]
    status: str
    chunk_count: int
    embedding_count: int
    s3_original_key: Optional[str]
    s3_parquet_key: Optional[str]
    s3_json_key: Optional[str]
    s3_markdown_key: Optional[str]
    s3_embeddings_key: Optional[str]
    error_message: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class DocumentListResponse(BaseModel):
    total: int
    items: list[DocumentResponse]


class PipelineTriggerRequest(BaseModel):
    job_run_id: int
    seed_urls: list[str]


class PipelineStatusResponse(BaseModel):
    job_run_id: int
    celery_task_id: Optional[str]
    status: str


class HealthResponse(BaseModel):
    status: str
    version: str
    services: dict


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    job_run_id: Optional[int] = None
    limit: int = Field(10, ge=1, le=100)


class SearchResultItem(BaseModel):
    document_id: str
    source_url: str
    chunk_text: str
    score: float


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResultItem]
