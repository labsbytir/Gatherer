import enum
from typing import Optional

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from harvester.models.base import Base


class DocumentStatus(str, enum.Enum):
    DISCOVERED = "discovered"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    OCR_PROCESSING = "ocr_processing"
    OCR_COMPLETE = "ocr_complete"
    TRANSFORMING = "transforming"
    TRANSFORMED = "transformed"
    EMBEDDING = "embedding"
    EMBEDDED = "embedded"
    STORING = "storing"
    STORED = "stored"
    FAILED = "failed"


class Document(Base):
    __tablename__ = "documents"
    __table_args__ = (
        UniqueConstraint("job_run_id", "source_url", name="uq_documents_job_source_url"),
    )

    job_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("job_runs.id", ondelete="CASCADE"), index=True
    )
    page_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("pages.id", ondelete="SET NULL"), nullable=True, index=True
    )

    source_url: Mapped[str] = mapped_column(Text, index=True)
    filename: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    file_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    file_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    local_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    status: Mapped[DocumentStatus] = mapped_column(
        Enum(DocumentStatus), default=DocumentStatus.DISCOVERED, index=True
    )

    extracted_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    docling_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    markdown_output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ocr_metadata: Mapped[dict] = mapped_column(JSON, default=dict)

    s3_original_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    s3_parquet_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    s3_json_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    s3_markdown_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    s3_embeddings_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    embedding_count: Mapped[int] = mapped_column(Integer, default=0)
    chunk_count: Mapped[int] = mapped_column(Integer, default=0)

    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

    job_run: Mapped["JobRun"] = relationship("JobRun", back_populates="documents")
