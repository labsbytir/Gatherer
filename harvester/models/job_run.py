import enum

from sqlalchemy import JSON, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from harvester.models.base import Base


class JobRunStatus(str, enum.Enum):
    CREATED = "created"
    DISCOVERING = "discovering"
    QUEUING = "queuing"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobRun(Base):
    """
    A job run groups all queue items (pages/records) that belong together.
    Default job_run_id is 10 as specified.
    """

    __tablename__ = "job_runs"

    job_run_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), default="Harvest Run")
    status: Mapped[JobRunStatus] = mapped_column(
        Enum(JobRunStatus), default=JobRunStatus.CREATED, index=True
    )
    seed_urls: Mapped[list] = mapped_column(JSON, default=list)
    config: Mapped[dict] = mapped_column(JSON, default=dict)

    max_depth: Mapped[int] = mapped_column(Integer, default=3)
    max_pages: Mapped[int] = mapped_column(Integer, default=1000)

    total_items: Mapped[int] = mapped_column(Integer, default=0)
    items_discovered: Mapped[int] = mapped_column(Integer, default=0)
    items_indexed: Mapped[int] = mapped_column(Integer, default=0)
    items_extracted: Mapped[int] = mapped_column(Integer, default=0)
    items_ocr_complete: Mapped[int] = mapped_column(Integer, default=0)
    items_transformed: Mapped[int] = mapped_column(Integer, default=0)
    items_stored: Mapped[int] = mapped_column(Integer, default=0)
    items_validated: Mapped[int] = mapped_column(Integer, default=0)
    items_failed: Mapped[int] = mapped_column(Integer, default=0)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    queue_items: Mapped[list["QueueItem"]] = relationship(
        "QueueItem", back_populates="job_run", cascade="all, delete-orphan"
    )
