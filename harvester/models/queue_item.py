import enum

from sqlalchemy import JSON, Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from harvester.models.base import Base


class Stage(str, enum.Enum):
    """
    The 8 stages of the pipeline. Each queue item flows through all stages.
    """
    DISCOVERED = "1_discovered"
    INDEXED = "2_indexed"
    JOB_CREATED = "3_job_created"
    EXTRACTED = "4_extracted"
    OCR = "5_ocr"
    TRANSFORM = "6_transform"
    STORED = "7_stored"
    VALIDATED = "8_validated"


STAGE_ORDER = [
    Stage.DISCOVERED,
    Stage.INDEXED,
    Stage.JOB_CREATED,
    Stage.EXTRACTED,
    Stage.OCR,
    Stage.TRANSFORM,
    Stage.STORED,
    Stage.VALIDATED,
]


class StageStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class QueueItem(Base):
    """
    Each page/record in the queue. Tracks the item through all 8 stages.

    The Crawlberg API server acts as the crawl queue:
    - Stage 3 (JOB_CREATED): A crawl job is submitted to Crawlberg for this URL
    - Crawlberg processes the crawl asynchronously
    - Harvester polls Crawlberg job status and advances the item through stages
    """

    __tablename__ = "queue_items"
    __table_args__ = (
        UniqueConstraint("job_run_id", "url", name="uq_queue_items_job_url"),
    )

    job_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("job_runs.id", ondelete="CASCADE"), index=True
    )

    url: Mapped[str] = mapped_column(Text, index=True)
    normalized_url: Mapped[str] = mapped_column(Text)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    item_type: Mapped[str] = mapped_column(String(50), default="page")

    current_stage: Mapped[Stage] = mapped_column(
        Enum(Stage), default=Stage.DISCOVERED, index=True
    )
    overall_status: Mapped[StageStatus] = mapped_column(
        Enum(StageStatus), default=StageStatus.PENDING, index=True
    )

    crawlberg_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)

    stage_statuses: Mapped[dict] = mapped_column(JSON, default=dict)

    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    html_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    markdown_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    page_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    links: Mapped[list] = mapped_column(JSON, default=list)
    images: Mapped[list] = mapped_column(JSON, default=list)

    is_document: Mapped[bool] = mapped_column(default=False)
    document_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    document_content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    document_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    document_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    local_file_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    ocr_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_markdown: Mapped[str | None] = mapped_column(Text, nullable=True)
    docling_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_metadata: Mapped[dict] = mapped_column(JSON, default=dict)

    parquet_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    parquet_rows: Mapped[int] = mapped_column(Integer, default=0)

    s3_original_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    s3_parquet_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    s3_json_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    s3_markdown_key: Mapped[str | None] = mapped_column(String(512), nullable=True)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

    job_run: Mapped["JobRun"] = relationship("JobRun", back_populates="queue_items")

    def advance_stage(self) -> Stage | None:
        current_idx = STAGE_ORDER.index(self.current_stage)
        if current_idx < len(STAGE_ORDER) - 1:
            self.current_stage = STAGE_ORDER[current_idx + 1]
            return self.current_stage
        return None

    def set_stage_status(self, stage: Stage, status: StageStatus, error: str | None = None):
        if self.stage_statuses is None:
            self.stage_statuses = {}
        self.stage_statuses[stage.value] = {
            "status": status.value,
            "error": error,
        }
