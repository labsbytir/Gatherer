from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class JobRunCreate(BaseModel):
    job_run_id: int = Field(10, description="Job run ID (default 10)")
    name: str = Field("Harvest Run", max_length=255)
    seed_urls: list[str] = Field(..., min_length=1)
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
    total_items: int
    items_discovered: int
    items_indexed: int
    items_extracted: int
    items_ocr_complete: int
    items_transformed: int
    items_stored: int
    items_validated: int
    items_failed: int
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class QueueItemResponse(BaseModel):
    id: str
    url: str
    normalized_url: str
    depth: int
    item_type: str
    current_stage: str
    overall_status: str
    crawlberg_job_id: Optional[str]
    title: Optional[str]
    content_type: Optional[str]
    http_status: Optional[int]
    is_document: bool
    document_filename: Optional[str]
    document_size: Optional[int]
    parquet_rows: int
    s3_original_key: Optional[str]
    s3_parquet_key: Optional[str]
    s3_json_key: Optional[str]
    s3_markdown_key: Optional[str]
    error_message: Optional[str]
    stage_statuses: dict
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class QueueItemListResponse(BaseModel):
    total: int
    items: list[QueueItemResponse]


class PipelineRunRequest(BaseModel):
    job_run_id: int = Field(10)
    seed_urls: list[str] = Field(..., min_length=1)
    name: str = Field("Harvest Run", max_length=255)
    max_depth: int = Field(3, ge=0, le=10)
    max_pages: int = Field(1000, ge=1, le=100000)


class PipelineRunResponse(BaseModel):
    job_run_id: int
    status: str
    discovered: int
    indexed: int
    extracted: int
    ocr: int
    transformed: int
    stored: int
    validation: dict


class StageRunRequest(BaseModel):
    stage: str = Field(..., description="Stage to run: discover, index, extract, ocr, transform, store, validate")


class HealthResponse(BaseModel):
    status: str
    version: str
    services: dict
