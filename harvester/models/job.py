import enum
from typing import Optional

from sqlalchemy import JSON, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from harvester.models.base import Base


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    DISCOVERING = "discovering"
    INDEXING = "indexing"
    QUEUED = "queued"
    EXTRACTING = "extracting"
    OCR_PROCESSING = "ocr_processing"
    TRANSFORMING = "transforming"
    STORING = "storing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobRun(Base):
    __tablename__ = "job_runs"

    job_run_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), default=JobStatus.PENDING, index=True
    )
    seed_urls: Mapped[list] = mapped_column(JSON, default=list)
    config: Mapped[dict] = mapped_column(JSON, default=dict)

    max_depth: Mapped[int] = mapped_column(Integer, default=3)
    max_pages: Mapped[int] = mapped_column(Integer, default=1000)

    total_pages_discovered: Mapped[int] = mapped_column(Integer, default=0)
    total_pages_processed: Mapped[int] = mapped_column(Integer, default=0)
    total_documents_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    total_documents_ocr: Mapped[int] = mapped_column(Integer, default=0)
    total_documents_stored: Mapped[int] = mapped_column(Integer, default=0)

    crawlberg_crawl_job_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    pages: Mapped[list["Page"]] = relationship(
        "Page", back_populates="job_run", cascade="all, delete-orphan"
    )
    documents: Mapped[list["Document"]] = relationship(
        "Document", back_populates="job_run", cascade="all, delete-orphan"
    )
