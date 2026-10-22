import enum
from typing import Optional

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from harvester.models.base import Base


class PageStatus(str, enum.Enum):
    DISCOVERED = "discovered"
    QUEUED = "queued"
    EXTRACTED = "extracted"
    FAILED = "failed"


class Page(Base):
    __tablename__ = "pages"
    __table_args__ = (
        UniqueConstraint("job_run_id", "url", name="uq_pages_job_url"),
    )

    job_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("job_runs.id", ondelete="CASCADE"), index=True
    )
    url: Mapped[str] = mapped_column(Text, index=True)
    normalized_url: Mapped[str] = mapped_column(Text)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[PageStatus] = mapped_column(
        Enum(PageStatus), default=PageStatus.DISCOVERED, index=True
    )

    title: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    markdown_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    html_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    links: Mapped[list] = mapped_column(JSON, default=list)
    images: Mapped[list] = mapped_column(JSON, default=list)

    discovered_document_urls: Mapped[list] = mapped_column(JSON, default=list)

    crawlberg_page_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    s3_html_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    s3_markdown_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    job_run: Mapped["JobRun"] = relationship("JobRun", back_populates="pages")
