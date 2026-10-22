import asyncio
import json
from pathlib import Path
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from harvester.config import get_settings
from harvester.crawler import CrawlbergQueue
from harvester.db import SessionLocal
from harvester.models import JobRun, JobRunStatus, QueueItem, Stage, StageStatus
from harvester.ocr import DoclingProcessor
from harvester.storage import StorageClient
from harvester.transform import ParquetTransformer
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

DOCUMENT_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".txt", ".csv", ".rtf", ".odt", ".ods", ".odp", ".epub",
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".gif", ".bmp", ".webp",
    ".html", ".htm", ".md", ".xml", ".json",
}


def _run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("loop closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


class QueueManager:
    """
    Manages the recursive crawling queue for a job run.

    The queue system communicates with the Crawlberg API server:
    1. DISCOVERED  → Crawlberg /v1/map discovers all URLs
    2. INDEXED     → Each URL is submitted to Crawlberg /v1/crawl as a job
    3. JOB_CREATED → Crawlberg job ID is stored, job is queued in Crawlberg
    4. EXTRACTED   → Crawlberg /v1/download fetches the document
    5. OCR         → Docling extracts text, markdown, JSON from the document
    6. TRANSFORM   → Docling JSON → Parquet via parquet-converter
    7. STORED      → Upload to R2/MinIO + save metadata to PostgreSQL
    8. VALIDATED   → Validate job completion

    Each page/record is a QueueItem that flows through all 8 stages.
    """

    def __init__(self, db: Session | None = None):
        self._db = db
        self._crawlberg = CrawlbergQueue()
        self._docling = DoclingProcessor()
        self._transformer = ParquetTransformer()
        self._storage = StorageClient()

    @property
    def db(self) -> Session:
        if self._db is None:
            self._db = SessionLocal()
        return self._db

    def close(self):
        if self._db is not None:
            self._db.close()
            self._db = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def create_job_run(
        self,
        job_run_id: int,
        seed_urls: list[str],
        name: str = "Harvest Run",
        max_depth: int | None = None,
        max_pages: int | None = None,
        config: dict | None = None,
    ) -> JobRun:
        """
        Create a new job run with the specified ID (default 10).
        """
        existing = self.db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if existing:
            logger.info("Job run already exists, returning existing", job_run_id=job_run_id)
            return existing

        job = JobRun(
            job_run_id=job_run_id,
            name=name,
            seed_urls=seed_urls,
            max_depth=max_depth or settings.max_depth,
            max_pages=max_pages or settings.max_pages,
            config=config or {},
            status=JobRunStatus.CREATED,
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        logger.info("Job run created", job_run_id=job_run_id, seeds=seed_urls)
        return job

    def get_or_create_job_run(self, job_run_id: int | None = None) -> JobRun:
        jid = job_run_id or settings.default_job_run_id
        job = self.db.query(JobRun).filter(JobRun.job_run_id == jid).first()
        if not job:
            raise ValueError(f"Job run {jid} not found. Create it first.")
        return job

    def stage_1_discover(self, job_run: JobRun) -> int:
        """
        Stage 1: DISCOVERED
        Use Crawlberg /v1/map to discover all URLs from seed URLs.
        Each discovered URL becomes a QueueItem with stage=DISCOVERED.
        """
        job_run.status = JobRunStatus.DISCOVERING
        self.db.commit()

        discovered_urls: set[str] = set()
        count = 0

        for seed_url in job_run.seed_urls:
            logger.info("Discovering URLs from seed", seed_url=seed_url)
            try:
                result = _run_async(
                    self._crawlberg.map_urls(seed_url, limit=job_run.max_pages)
                )
                for entry in result.urls:
                    url = entry.get("url", "")
                    if url and url not in discovered_urls:
                        discovered_urls.add(url)
                        count += 1
            except Exception as e:
                logger.error("Discovery failed for seed", seed_url=seed_url, error=str(e))

        for url in discovered_urls:
            existing = self.db.query(QueueItem).filter(
                QueueItem.job_run_id == job_run.id,
                QueueItem.url == url,
            ).first()
            if existing:
                continue

            item = QueueItem(
                job_run_id=job_run.id,
                url=url,
                normalized_url=url,
                depth=0,
                current_stage=Stage.DISCOVERED,
                overall_status=StageStatus.PENDING,
                is_document=self._is_document_url(url),
            )
            item.set_stage_status(Stage.DISCOVERED, StageStatus.COMPLETED)
            self.db.add(item)

        job_run.items_discovered = count
        job_run.total_items = count
        self.db.commit()

        logger.info("Stage 1 complete: discovered", count=count, job_run_id=job_run.job_run_id)
        return count

    def stage_2_3_index_and_create_jobs(self, job_run: JobRun) -> int:
        """
        Stage 2-3: INDEXED + JOB_CREATED
        For each discovered QueueItem, submit a crawl job to Crawlberg's queue.
        Store the Crawlberg job ID on the QueueItem.
        """
        job_run.status = JobRunStatus.QUEUING
        self.db.commit()

        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
            QueueItem.current_stage == Stage.DISCOVERED,
        ).all()

        queued = 0
        for item in items:
            try:
                item.overall_status = StageStatus.IN_PROGRESS
                item.set_stage_status(Stage.INDEXED, StageStatus.IN_PROGRESS)
                self.db.commit()

                crawl_job_id = _run_async(
                    self._crawlberg.enqueue_crawl(
                        url=item.url,
                        max_depth=job_run.max_depth,
                        max_pages=1,
                    )
                )

                item.crawlberg_job_id = crawl_job_id
                item.current_stage = Stage.JOB_CREATED
                item.set_stage_status(Stage.INDEXED, StageStatus.COMPLETED)
                item.set_stage_status(Stage.JOB_CREATED, StageStatus.IN_PROGRESS)
                self.db.commit()

                queued += 1
                logger.info(
                    "Crawl job created",
                    item_id=item.id,
                    url=item.url,
                    crawlberg_job_id=crawl_job_id,
                )
            except Exception as e:
                logger.error("Failed to create crawl job", item_id=item.id, error=str(e))
                item.set_stage_status(Stage.JOB_CREATED, StageStatus.FAILED, str(e))
                item.overall_status = StageStatus.FAILED
                item.error_message = str(e)
                self.db.commit()

        job_run.items_indexed = queued
        self.db.commit()
        logger.info("Stage 2-3 complete: jobs created", queued=queued)
        return queued

    def stage_4_extract(self, job_run: JobRun) -> int:
        """
        Stage 4: EXTRACTED
        Poll Crawlberg job status. When complete, extract page data.
        For document URLs, download the document via Crawlberg /v1/download.
        """
        job_run.status = JobRunStatus.PROCESSING
        self.db.commit()

        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
            QueueItem.current_stage == Stage.JOB_CREATED,
            QueueItem.overall_status == StageStatus.IN_PROGRESS,
        ).all()

        extracted = 0
        for item in items:
            if not item.crawlberg_job_id:
                continue

            try:
                result = _run_async(
                    self._crawlberg.wait_for_job(item.crawlberg_job_id)
                )

                if result.status == "completed" and result.data:
                    page = result.data[0]
                    item.title = (page.get("metadata") or {}).get("title")
                    item.content_type = page.get("content_type", "")
                    item.http_status = page.get("status_code", 0)
                    item.html_content = (page.get("html") or "")[:500000]
                    md = page.get("markdown") or {}
                    item.markdown_content = md.get("content") if isinstance(md, dict) else None
                    item.page_metadata = page.get("metadata") or {}
                    item.links = page.get("links") or []
                    item.images = page.get("images") or []

                    doc = page.get("downloaded_document")
                    if doc:
                        item.is_document = True
                        item.document_filename = doc.get("filename")
                        item.document_content_type = doc.get("content_type")
                        item.document_size = doc.get("size")
                        item.document_hash = doc.get("sha256")

                    item.set_stage_status(Stage.JOB_CREATED, StageStatus.COMPLETED)

                elif result.status == "failed":
                    item.set_stage_status(Stage.JOB_CREATED, StageStatus.FAILED, result.error)
                    item.overall_status = StageStatus.FAILED
                    item.error_message = result.error
                    self.db.commit()
                    continue

                if item.is_document:
                    dl_result = _run_async(
                        self._crawlberg.download(
                            url=item.url, max_size=settings.max_file_size
                        )
                    )
                    if dl_result.downloaded_document:
                        doc_info = dl_result.downloaded_document
                        item.document_filename = doc_info.get("filename", item.document_filename)
                        item.document_content_type = doc_info.get("content_type")
                        item.document_size = doc_info.get("size", dl_result.body_size)
                        item.document_hash = doc_info.get("sha256")

                        content = doc_info.get("content", "")
                        if isinstance(content, str):
                            import base64
                            try:
                                content_bytes = base64.b64decode(content)
                            except Exception:
                                content_bytes = content.encode("utf-8")
                        else:
                            content_bytes = dl_result.html.encode("utf-8")

                        temp_dir = Path(settings.temp_dir) / f"job-{job_run.job_run_id}" / item.id
                        temp_dir.mkdir(parents=True, exist_ok=True)
                        local_path = temp_dir / (item.document_filename or "document.bin")
                        local_path.write_bytes(content_bytes)
                        item.local_file_path = str(local_path)

                item.current_stage = Stage.EXTRACTED
                item.set_stage_status(Stage.EXTRACTED, StageStatus.COMPLETED)
                item.overall_status = StageStatus.COMPLETED
                self.db.commit()
                extracted += 1

                logger.info("Stage 4 complete: extracted", item_id=item.id, url=item.url)

            except Exception as e:
                logger.error("Extraction failed", item_id=item.id, error=str(e))
                item.set_stage_status(Stage.EXTRACTED, StageStatus.FAILED, str(e))
                item.overall_status = StageStatus.FAILED
                item.error_message = str(e)
                self.db.commit()

        job_run.items_extracted = extracted
        self.db.commit()
        logger.info("Stage 4 complete: extracted", count=extracted)
        return extracted

    def stage_5_ocr(self, job_run: JobRun) -> int:
        """
        Stage 5: OCR
        Process each extracted document with Docling.
        Extracts: text, markdown, structured JSON, metadata.
        Prepares data for vectors, AI, and storage.
        """
        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
            QueueItem.current_stage == Stage.EXTRACTED,
            QueueItem.is_document.is_(True),
        ).all()

        ocr_count = 0
        for item in items:
            try:
                if not item.local_file_path or not Path(item.local_file_path).exists():
                    logger.warning("No local file for OCR", item_id=item.id)
                    item.current_stage = Stage.OCR
                    item.set_stage_status(Stage.OCR, StageStatus.SKIPPED, "No file")
                    self.db.commit()
                    continue

                item.set_stage_status(Stage.OCR, StageStatus.IN_PROGRESS)
                self.db.commit()

                result = self._docling.process_file(item.local_file_path, item.url)

                item.ocr_text = result.text[:1000000] if result.text else None
                item.ocr_markdown = result.markdown
                item.docling_json = json.dumps(result.docling_json, ensure_ascii=False)
                item.ocr_metadata = result.metadata

                item.current_stage = Stage.OCR
                item.set_stage_status(Stage.OCR, StageStatus.COMPLETED)
                self.db.commit()
                ocr_count += 1

                logger.info(
                    "Stage 5 complete: OCR",
                    item_id=item.id,
                    text_len=len(result.text),
                    pages=result.num_pages,
                )
            except Exception as e:
                logger.error("OCR failed", item_id=item.id, error=str(e))
                item.set_stage_status(Stage.OCR, StageStatus.FAILED, str(e))
                item.overall_status = StageStatus.FAILED
                item.error_message = str(e)
                self.db.commit()

        job_run.items_ocr_complete = ocr_count
        self.db.commit()
        logger.info("Stage 5 complete: OCR", count=ocr_count)
        return ocr_count

    def stage_6_transform(self, job_run: JobRun) -> int:
        """
        Stage 6: TRANSFORM
        Convert Docling JSON → Apache Parquet.
        Uses parquet-converter (Go) if available, falls back to PyArrow.
        """
        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
            QueueItem.current_stage.in_([Stage.EXTRACTED, Stage.OCR]),
            QueueItem.docling_json.isnot(None),
        ).all()

        transformed = 0
        for item in items:
            try:
                item.set_stage_status(Stage.TRANSFORM, StageStatus.IN_PROGRESS)
                self.db.commit()

                docling_data = json.loads(item.docling_json)
                result = self._transformer.transform(
                    docling_json=docling_data,
                    document_id=item.id,
                    source_url=item.url,
                    output_filename=f"{item.id}.parquet",
                )

                item.parquet_path = str(result.parquet_path)
                item.parquet_rows = result.row_count

                item.current_stage = Stage.TRANSFORM
                item.set_stage_status(Stage.TRANSFORM, StageStatus.COMPLETED)
                self.db.commit()
                transformed += 1

                logger.info(
                    "Stage 6 complete: transform",
                    item_id=item.id,
                    rows=result.row_count,
                )
            except Exception as e:
                logger.error("Transform failed", item_id=item.id, error=str(e))
                item.set_stage_status(Stage.TRANSFORM, StageStatus.FAILED, str(e))
                item.overall_status = StageStatus.FAILED
                item.error_message = str(e)
                self.db.commit()

        job_run.items_transformed = transformed
        self.db.commit()
        logger.info("Stage 6 complete: transform", count=transformed)
        return transformed

    def stage_7_store(self, job_run: JobRun) -> int:
        """
        Stage 7: STORED
        Transfer all artifacts to R2/MinIO object storage.
        Also saves metadata to PostgreSQL (already done incrementally).
        """
        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
            QueueItem.current_stage.in_([Stage.EXTRACTED, Stage.OCR, Stage.TRANSFORM]),
        ).all()

        stored = 0
        for item in items:
            try:
                item.set_stage_status(Stage.STORED, StageStatus.IN_PROGRESS)
                self.db.commit()

                base_key = f"job-{job_run.job_run_id}/{item.id}"

                if item.local_file_path and Path(item.local_file_path).exists():
                    is_parquet = item.local_file_path.endswith(".parquet")
                    if is_parquet:
                        item.s3_parquet_key = f"{base_key}/parquet/{Path(item.local_file_path).name}"
                        self._storage.upload_file(
                            item.local_file_path,
                            item.s3_parquet_key,
                            content_type="application/octet-stream",
                        )
                    else:
                        item.s3_original_key = f"{base_key}/original/{Path(item.local_file_path).name}"
                        self._storage.upload_file(
                            item.local_file_path,
                            item.s3_original_key,
                            content_type=item.document_content_type or "application/octet-stream",
                        )

                if item.docling_json:
                    item.s3_json_key = f"{base_key}/json/document.json"
                    self._storage.upload_text(
                        item.docling_json, item.s3_json_key, "application/json"
                    )

                if item.ocr_markdown:
                    item.s3_markdown_key = f"{base_key}/markdown/document.md"
                    self._storage.upload_text(
                        item.ocr_markdown, item.s3_markdown_key, "text/markdown"
                    )

                item.current_stage = Stage.STORED
                item.set_stage_status(Stage.STORED, StageStatus.COMPLETED)
                self.db.commit()
                stored += 1

                logger.info("Stage 7 complete: stored", item_id=item.id)
            except Exception as e:
                logger.error("Storage failed", item_id=item.id, error=str(e))
                item.set_stage_status(Stage.STORED, StageStatus.FAILED, str(e))
                item.overall_status = StageStatus.FAILED
                item.error_message = str(e)
                self.db.commit()

        job_run.items_stored = stored
        self.db.commit()
        logger.info("Stage 7 complete: stored", count=stored)
        return stored

    def stage_8_validate(self, job_run: JobRun) -> dict:
        """
        Stage 8: VALIDATED
        Validate that all queue items completed successfully.
        Update job run status.
        """
        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
        ).all()

        validated = 0
        failed = 0

        for item in items:
            if item.overall_status == StageStatus.COMPLETED or item.current_stage in (
                Stage.STORED, Stage.TRANSFORM, Stage.OCR, Stage.EXTRACTED,
            ):
                item.current_stage = Stage.VALIDATED
                item.set_stage_status(Stage.VALIDATED, StageStatus.COMPLETED)
                item.overall_status = StageStatus.COMPLETED
                validated += 1
            else:
                item.set_stage_status(Stage.VALIDATED, StageStatus.FAILED)
                failed += 1

        job_run.items_validated = validated
        job_run.items_failed = failed

        if failed == 0 and validated > 0:
            job_run.status = JobRunStatus.COMPLETED
        elif validated > 0:
            job_run.status = JobRunStatus.COMPLETED
            job_run.error_message = f"Partial: {validated} ok, {failed} failed"
        else:
            job_run.status = JobRunStatus.FAILED
            job_run.error_message = f"All {failed} items failed"

        self.db.commit()

        logger.info(
            "Stage 8 complete: validated",
            job_run_id=job_run.job_run_id,
            validated=validated,
            failed=failed,
            status=job_run.status.value,
        )

        return {
            "job_run_id": job_run.job_run_id,
            "status": job_run.status.value,
            "validated": validated,
            "failed": failed,
            "total": len(items),
        }

    def run_full_pipeline(
        self,
        job_run_id: int,
        seed_urls: list[str],
        name: str = "Harvest Run",
    ) -> dict:
        """
        Run the complete 8-stage pipeline for a job run.
        """
        job_run = self.create_job_run(job_run_id, seed_urls, name=name)

        results = {}

        logger.info("=== Stage 1: DISCOVERED ===", job_run_id=job_run_id)
        results["discovered"] = self.stage_1_discover(job_run)

        logger.info("=== Stage 2-3: INDEXED + JOB_CREATED ===", job_run_id=job_run_id)
        results["indexed"] = self.stage_2_3_index_and_create_jobs(job_run)

        logger.info("=== Stage 4: EXTRACTED ===", job_run_id=job_run_id)
        results["extracted"] = self.stage_4_extract(job_run)

        logger.info("=== Stage 5: OCR ===", job_run_id=job_run_id)
        results["ocr"] = self.stage_5_ocr(job_run)

        logger.info("=== Stage 6: TRANSFORM ===", job_run_id=job_run_id)
        results["transformed"] = self.stage_6_transform(job_run)

        logger.info("=== Stage 7: STORED ===", job_run_id=job_run_id)
        results["stored"] = self.stage_7_store(job_run)

        logger.info("=== Stage 8: VALIDATED ===", job_run_id=job_run_id)
        results["validation"] = self.stage_8_validate(job_run)

        self.db.refresh(job_run)
        results["job_run"] = {
            "job_run_id": job_run.job_run_id,
            "status": job_run.status.value,
            "total_items": job_run.total_items,
        }

        logger.info("Pipeline complete", job_run_id=job_run_id, results=results)
        return results

    def process_pending_items(self, job_run: JobRun, stage: Stage) -> int:
        """
        Process all pending items at a specific stage.
        Useful for resuming interrupted pipelines.
        """
        items = self.db.query(QueueItem).filter(
            QueueItem.job_run_id == job_run.id,
            QueueItem.current_stage == stage,
            QueueItem.overall_status.in_([StageStatus.PENDING, StageStatus.IN_PROGRESS]),
        ).count()

        logger.info("Processing pending items", stage=stage.value, count=items)

        if stage == Stage.DISCOVERED:
            return self.stage_2_3_index_and_create_jobs(job_run)
        elif stage == Stage.JOB_CREATED:
            return self.stage_4_extract(job_run)
        elif stage == Stage.EXTRACTED:
            return self.stage_5_ocr(job_run)
        elif stage == Stage.OCR:
            return self.stage_6_transform(job_run)
        elif stage in (Stage.TRANSFORM, Stage.EXTRACTED):
            return self.stage_7_store(job_run)

        return 0

    @staticmethod
    def _is_document_url(url: str) -> bool:
        path = url.split("?")[0].lower()
        return any(path.endswith(ext) for ext in DOCUMENT_EXTENSIONS)
