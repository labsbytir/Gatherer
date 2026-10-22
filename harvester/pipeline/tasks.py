import asyncio
import json
import os
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from celery import chain, group, shared_task
from sqlalchemy import select
from sqlalchemy.orm import Session

from harvester.config import get_settings
from harvester.crawler import CrawlbergClient
from harvester.db import SessionLocal
from harvester.embeddings import EmbeddingService
from harvester.models import Document, DocumentStatus, JobRun, JobStatus, Page, PageStatus
from harvester.ocr import DoclingProcessor
from harvester.pipeline.celery_app import celery_app
from harvester.proxy import get_proxy_manager
from harvester.storage import StorageClient
from harvester.transform import ParquetTransformer
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def _get_db() -> Session:
    return SessionLocal()


def _run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def discover_pages_task(self, job_run_id: int, seed_urls: list[str]) -> dict:
    """
    Stage 1: DISCOVERED
    Use Crawlberg to map/discover all pages from seed URLs.
    Each discovered page is added to the database with status DISCOVERED.
    """
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        job.status = JobStatus.DISCOVERING
        db.commit()

        client = CrawlbergClient()
        proxy_manager = get_proxy_manager()

        all_pages: list[dict] = []
        discovered_urls: set[str] = set()

        for seed_url in seed_urls:
            logger.info("Mapping URLs from seed", seed_url=seed_url, job_run_id=job_run_id)

            try:
                if proxy_manager.enabled:
                    proxy_cfg = proxy_manager.get_crawlberg_proxy_config()
                    logger.info("Using proxy for discovery", proxy_enabled=True)

                map_result = _run_async(
                    client.map_urls(seed_url, limit=job.max_pages)
                )

                for url_entry in map_result.urls:
                    url = url_entry.get("url", "")
                    if url and url not in discovered_urls:
                        discovered_urls.add(url)
                        all_pages.append({
                            "url": url,
                            "normalized_url": url_entry.get("url", url),
                            "depth": 0,
                            "metadata_json": {
                                "lastmod": url_entry.get("lastmod"),
                                "changefreq": url_entry.get("changefreq"),
                                "priority": url_entry.get("priority"),
                            },
                        })

                logger.info(
                    "Mapped URLs from seed",
                    seed_url=seed_url,
                    count=len(map_result.urls),
                )
            except Exception as e:
                logger.error("Failed to map seed URL", seed_url=seed_url, error=str(e))
                continue

        for page_data in all_pages:
            existing = db.query(Page).filter(
                Page.job_run_id == job.id,
                Page.url == page_data["url"],
            ).first()
            if existing:
                continue

            page = Page(
                job_run_id=job.id,
                url=page_data["url"],
                normalized_url=page_data["normalized_url"],
                depth=page_data["depth"],
                status=PageStatus.DISCOVERED,
                metadata_json=page_data.get("metadata_json", {}),
            )
            db.add(page)

        job.total_pages_discovered = len(all_pages)
        db.commit()

        logger.info(
            "Discovery complete",
            job_run_id=job_run_id,
            total_discovered=job.total_pages_discovered,
        )

        return {
            "job_run_id": job_run_id,
            "discovered_count": len(all_pages),
            "status": "discovered",
        }

    except Exception as exc:
        db.rollback()
        logger.error("Discovery failed", job_run_id=job_run_id, error=str(exc))
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(exc)
            db.commit()
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def index_pages_task(self, discover_result: dict) -> dict:
    """
    Stage 2: INDEXED (Creates Job)
    Start a Crawlberg crawl job to index/extract all discovered pages.
    Pages are updated with their extracted content (HTML, markdown, metadata).
    """
    job_run_id = discover_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        job.status = JobStatus.INDEXING
        db.commit()

        client = CrawlbergClient()

        crawl_job_ids = []
        for seed_url in job.seed_urls:
            try:
                crawl_job_id = _run_async(
                    client.start_crawl(
                        url=seed_url,
                        max_depth=job.max_depth,
                        max_pages=job.max_pages,
                    )
                )
                crawl_job_ids.append(crawl_job_id)
                logger.info(
                    "Started crawl job",
                    seed_url=seed_url,
                    crawl_job_id=crawl_job_id,
                )
            except Exception as e:
                logger.error("Failed to start crawl", seed_url=seed_url, error=str(e))

        if crawl_job_ids:
            job.crawlberg_crawl_job_id = crawl_job_ids[0] if len(crawl_job_ids) == 1 else json.dumps(crawl_job_ids)
        db.commit()

        all_crawl_results = []
        for crawl_job_id in crawl_job_ids:
            try:
                result = _run_async(
                    client.wait_for_crawl(
                        crawl_job_id,
                        poll_interval=settings.job_poll_interval,
                        max_attempts=settings.job_poll_max_attempts,
                    )
                )
                all_crawl_results.extend(result.data)
                logger.info(
                    "Crawl completed",
                    crawl_job_id=crawl_job_id,
                    status=result.status,
                    pages=len(result.data),
                )
            except Exception as e:
                logger.error("Crawl failed", crawl_job_id=crawl_job_id, error=str(e))

        pages_updated = 0
        document_urls_found: set[str] = set()

        for crawl_page in all_crawl_results:
            page = db.query(Page).filter(
                Page.job_run_id == job.id,
                Page.normalized_url == crawl_page.normalized_url,
            ).first()

            if not page:
                page = db.query(Page).filter(
                    Page.job_run_id == job.id,
                    Page.url == crawl_page.url,
                ).first()

            if not page:
                page = Page(
                    job_run_id=job.id,
                    url=crawl_page.url,
                    normalized_url=crawl_page.normalized_url,
                    depth=crawl_page.depth,
                )
                db.add(page)

            page.status = PageStatus.EXTRACTED
            page.title = crawl_page.metadata.get("title") if crawl_page.metadata else None
            page.content_type = crawl_page.content_type
            page.http_status = crawl_page.status_code
            page.html_content = crawl_page.html[:500000] if crawl_page.html else None
            page.markdown_content = crawl_page.markdown_content
            page.metadata_json = crawl_page.metadata or {}
            page.links = crawl_page.links or []
            page.images = crawl_page.images or []

            doc_urls = []
            for link in crawl_page.links or []:
                link_url = link.get("url", "") if isinstance(link, dict) else str(link)
                link_type = link.get("link_type", "") if isinstance(link, dict) else ""
                if link_type == "document" or self._is_document_url(link_url):
                    if link_url not in document_urls_found:
                        document_urls_found.add(link_url)
                        doc_urls.append(link_url)

            if crawl_page.downloaded_document:
                doc_info = crawl_page.downloaded_document
                doc_url = doc_info.get("url", crawl_page.url)
                if doc_url not in document_urls_found:
                    document_urls_found.add(doc_url)
                    doc_urls.append(doc_url)

            page.discovered_document_urls = doc_urls
            pages_updated += 1

        job.total_pages_processed = pages_updated
        job.status = JobStatus.QUEUED
        db.commit()

        for doc_url in document_urls_found:
            existing = db.query(Document).filter(
                Document.job_run_id == job.id,
                Document.source_url == doc_url,
            ).first()
            if existing:
                continue

            doc = Document(
                job_run_id=job.id,
                source_url=doc_url,
                filename=self._extract_filename(doc_url),
                status=DocumentStatus.QUEUED,
            )
            db.add(doc)

        db.commit()

        logger.info(
            "Indexing complete",
            job_run_id=job_run_id,
            pages_updated=pages_updated,
            documents_queued=len(document_urls_found),
        )

        return {
            "job_run_id": job_run_id,
            "pages_indexed": pages_updated,
            "documents_queued": len(document_urls_found),
            "status": "indexed",
        }

    except Exception as exc:
        db.rollback()
        logger.error("Indexing failed", job_run_id=job_run_id, error=str(exc))
        raise self.retry(exc=exc)
    finally:
        db.close()

    @staticmethod
    def _is_document_url(url: str) -> bool:
        doc_extensions = {
            ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
            ".txt", ".csv", ".rtf", ".odt", ".ods", ".odp",
            ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".gif", ".bmp", ".webp",
            ".epub", ".md", ".html", ".htm", ".xml", ".json",
        }
        path = url.split("?")[0].lower()
        return any(path.endswith(ext) for ext in doc_extensions)

    @staticmethod
    def _extract_filename(url: str) -> str:
        path = url.split("?")[0].rstrip("/")
        return path.split("/")[-1] if "/" in path else path


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def extract_page_task(self, index_result: dict) -> dict:
    """
    Stage 3-4: EXTRACTED (Downloaded and transferred to OCR)
    Download each discovered document and prepare it for OCR processing.
    """
    job_run_id = index_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        job.status = JobStatus.EXTRACTING
        db.commit()

        client = CrawlbergClient()
        documents = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status == DocumentStatus.QUEUED,
        ).all()

        downloaded_count = 0
        for doc in documents:
            try:
                doc.status = DocumentStatus.DOWNLOADING
                db.commit()

                result = _run_async(
                    client.download_document(
                        url=doc.source_url,
                        max_size=settings.crawler_max_file_size,
                    )
                )

                if result.downloaded_document:
                    doc_info = result.downloaded_document
                    doc.filename = doc_info.get("filename", doc.filename)
                    doc.content_type = doc_info.get("content_type", result.content_type)
                    doc.file_size = doc_info.get("size", result.body_size)
                    doc.file_hash = doc_info.get("sha256")

                    doc_content = doc_info.get("content")
                    if doc_content and isinstance(doc_content, str):
                        import base64
                        try:
                            content_bytes = base64.b64decode(doc_content)
                        except Exception:
                            content_bytes = doc_content.encode("utf-8")
                    else:
                        content_bytes = result.html.encode("utf-8") if result.html else b""

                    temp_dir = Path(settings.temp_dir) / f"job-{job_run_id}" / doc.id
                    temp_dir.mkdir(parents=True, exist_ok=True)
                    local_path = temp_dir / (doc.filename or "document.bin")
                    local_path.write_bytes(content_bytes)
                    doc.local_path = str(local_path)

                    doc.status = DocumentStatus.DOWNLOADED
                    downloaded_count += 1
                    logger.info(
                        "Document downloaded",
                        doc_id=doc.id,
                        url=doc.source_url,
                        size=doc.file_size,
                    )
                else:
                    doc.status = DocumentStatus.FAILED
                    doc.error_message = "No document content returned from Crawlberg"

                db.commit()

            except Exception as e:
                logger.error("Failed to download document", doc_id=doc.id, error=str(e))
                doc.status = DocumentStatus.FAILED
                doc.error_message = str(e)
                doc.retry_count += 1
                db.commit()

        job.total_documents_downloaded = downloaded_count
        db.commit()

        logger.info(
            "Extraction complete",
            job_run_id=job_run_id,
            downloaded=downloaded_count,
            total=len(documents),
        )

        return {
            "job_run_id": job_run_id,
            "downloaded_count": downloaded_count,
            "status": "extracted",
        }

    except Exception as exc:
        db.rollback()
        logger.error("Extraction failed", job_run_id=job_run_id, error=str(exc))
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def download_document_task(self, job_run_id: int, document_id: str) -> dict:
    """
    Download a single document (used for per-document pipeline).
    """
    db = _get_db()
    try:
        doc = db.query(Document).filter(Document.id == document_id).first()
        if not doc:
            raise ValueError(f"Document {document_id} not found")

        job = db.query(JobRun).filter(JobRun.id == doc.job_run_id).first()

        doc.status = DocumentStatus.DOWNLOADING
        db.commit()

        client = CrawlbergClient()
        result = _run_async(
            client.download_document(
                url=doc.source_url,
                max_size=settings.crawler_max_file_size,
            )
        )

        if result.downloaded_document:
            doc_info = result.downloaded_document
            doc.filename = doc_info.get("filename", doc.filename)
            doc.content_type = doc_info.get("content_type", result.content_type)
            doc.file_size = doc_info.get("size", result.body_size)
            doc.file_hash = doc_info.get("sha256")

            doc_content = doc_info.get("content")
            if doc_content and isinstance(doc_content, str):
                import base64
                try:
                    content_bytes = base64.b64decode(doc_content)
                except Exception:
                    content_bytes = doc_content.encode("utf-8")
            else:
                content_bytes = result.html.encode("utf-8") if result.html else b""

            temp_dir = Path(settings.temp_dir) / f"job-{job.job_run_id}" / doc.id
            temp_dir.mkdir(parents=True, exist_ok=True)
            local_path = temp_dir / (doc.filename or "document.bin")
            local_path.write_bytes(content_bytes)
            doc.local_path = str(local_path)
            doc.status = DocumentStatus.DOWNLOADED
        else:
            doc.status = DocumentStatus.FAILED
            doc.error_message = "No document content returned"

        db.commit()

        return {
            "job_run_id": job.job_run_id,
            "document_id": doc.id,
            "local_path": doc.local_path,
            "status": doc.status.value,
        }

    except Exception as exc:
        db.rollback()
        doc = db.query(Document).filter(Document.id == document_id).first()
        if doc:
            doc.status = DocumentStatus.FAILED
            doc.error_message = str(exc)
            doc.retry_count += 1
            db.commit()
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def ocr_document_task(self, extract_result: dict) -> dict:
    """
    Stage 5: OCR
    Process each downloaded document with Docling to extract:
    - Text content
    - Markdown
    - Structured JSON (DoclingDocument)
    - Metadata (page count, tables, etc.)
    """
    job_run_id = extract_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        job.status = JobStatus.OCR_PROCESSING
        db.commit()

        processor = DoclingProcessor()
        documents = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status == DocumentStatus.DOWNLOADED,
        ).all()

        ocr_count = 0
        for doc in documents:
            try:
                if not doc.local_path or not Path(doc.local_path).exists():
                    doc.status = DocumentStatus.FAILED
                    doc.error_message = "Local file not found"
                    db.commit()
                    continue

                doc.status = DocumentStatus.OCR_PROCESSING
                db.commit()

                logger.info("Starting OCR", doc_id=doc.id, path=doc.local_path)
                ocr_result = processor.process_file(doc.local_path, doc.source_url)

                doc.extracted_text = ocr_result.text[:1000000] if ocr_result.text else None
                doc.markdown_output = ocr_result.markdown
                doc.docling_json = processor.to_json_string(ocr_result)
                doc.ocr_metadata = ocr_result.metadata
                doc.status = DocumentStatus.OCR_COMPLETE
                ocr_count += 1

                logger.info(
                    "OCR complete",
                    doc_id=doc.id,
                    text_length=len(ocr_result.text),
                    pages=ocr_result.num_pages,
                    ocr_applied=ocr_result.ocr_applied,
                )
                db.commit()

            except Exception as e:
                logger.error("OCR failed", doc_id=doc.id, error=str(e))
                doc.status = DocumentStatus.FAILED
                doc.error_message = f"OCR failed: {str(e)}"
                doc.retry_count += 1
                db.commit()

        job.total_documents_ocr = ocr_count
        db.commit()

        logger.info(
            "OCR processing complete",
            job_run_id=job_run_id,
            ocr_count=ocr_count,
            total=len(documents),
        )

        return {
            "job_run_id": job_run_id,
            "ocr_count": ocr_count,
            "status": "ocr_complete",
        }

    except Exception as exc:
        db.rollback()
        logger.error("OCR stage failed", job_run_id=job_run_id, error=str(exc))
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def transform_document_task(self, ocr_result: dict) -> dict:
    """
    Stage 6: TRANSFORM
    Transform Docling JSON output into Apache Parquet format.
    """
    job_run_id = ocr_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        job.status = JobStatus.TRANSFORMING
        db.commit()

        transformer = ParquetTransformer()
        documents = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status == DocumentStatus.OCR_COMPLETE,
        ).all()

        transform_count = 0
        for doc in documents:
            try:
                if not doc.docling_json:
                    doc.status = DocumentStatus.FAILED
                    doc.error_message = "No Docling JSON to transform"
                    db.commit()
                    continue

                doc.status = DocumentStatus.TRANSFORMING
                db.commit()

                docling_data = json.loads(doc.docling_json)
                result = transformer.transform_docling_json(
                    docling_json=docling_data,
                    document_id=doc.id,
                    source_url=doc.source_url,
                    output_filename=f"{doc.id}.parquet",
                )

                doc.local_path = str(result.parquet_path)
                doc.chunk_count = result.row_count
                doc.status = DocumentStatus.TRANSFORMED
                transform_count += 1

                logger.info(
                    "Transform complete",
                    doc_id=doc.id,
                    rows=result.row_count,
                    columns=len(result.columns),
                )
                db.commit()

            except Exception as e:
                logger.error("Transform failed", doc_id=doc.id, error=str(e))
                doc.status = DocumentStatus.FAILED
                doc.error_message = f"Transform failed: {str(e)}"
                doc.retry_count += 1
                db.commit()

        db.commit()

        logger.info(
            "Transform stage complete",
            job_run_id=job_run_id,
            transformed=transform_count,
            total=len(documents),
        )

        return {
            "job_run_id": job_run_id,
            "transformed_count": transform_count,
            "status": "transformed",
        }

    except Exception as exc:
        db.rollback()
        logger.error("Transform stage failed", job_run_id=job_run_id, error=str(exc))
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def embed_document_task(self, transform_result: dict) -> dict:
    """
    Stage 6b: EMBED
    Generate local embeddings for each transformed document's text chunks.
    """
    job_run_id = transform_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        embedder = EmbeddingService()
        documents = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status == DocumentStatus.TRANSFORMED,
        ).all()

        embed_count = 0
        for doc in documents:
            try:
                if not doc.extracted_text:
                    logger.warning("No text to embed", doc_id=doc.id)
                    doc.embedding_count = 0
                    db.commit()
                    continue

                doc.status = DocumentStatus.EMBEDDING
                db.commit()

                result = embedder.embed_text(doc.extracted_text, document_id=doc.id)

                embeddings_dir = Path(settings.temp_dir) / f"job-{job_run_id}" / doc.id
                embeddings_dir.mkdir(parents=True, exist_ok=True)
                embeddings_path = embeddings_dir / "embeddings.json"
                embedder.save_embeddings(result, embeddings_path)

                doc.embedding_count = len(result.chunks)
                doc.s3_embeddings_key = None
                doc._embeddings_path = str(embeddings_path)
                doc._embeddings_result = result
                doc.status = DocumentStatus.EMBEDDED
                embed_count += 1

                logger.info(
                    "Embeddings generated",
                    doc_id=doc.id,
                    chunks=len(result.chunks),
                    dimension=result.embedding_dimension,
                )
                db.commit()

            except Exception as e:
                logger.error("Embedding failed", doc_id=doc.id, error=str(e))
                doc.status = DocumentStatus.FAILED
                doc.error_message = f"Embedding failed: {str(e)}"
                doc.retry_count += 1
                db.commit()

        logger.info(
            "Embedding stage complete",
            job_run_id=job_run_id,
            embedded=embed_count,
            total=len(documents),
        )

        return {
            "job_run_id": job_run_id,
            "embedded_count": embed_count,
            "status": "embedded",
        }

    except Exception as exc:
        db.rollback()
        logger.error("Embedding stage failed", job_run_id=job_run_id, error=str(exc))
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def store_document_task(self, embed_result: dict) -> dict:
    """
    Stage 7: STORE
    Transfer all processed artifacts to S3-compatible object storage (R2/MinIO):
    - Original document file
    - Parquet transformed data
    - Docling JSON
    - Markdown output
    - Embeddings
    """
    job_run_id = embed_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        job.status = JobStatus.STORING
        db.commit()

        storage = StorageClient()
        documents = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status.in_([DocumentStatus.EMBEDDED, DocumentStatus.TRANSFORMED]),
        ).all()

        stored_count = 0
        for doc in documents:
            try:
                doc.status = DocumentStatus.STORING
                db.commit()

                base_key = f"job-{job_run_id}/{doc.id}"

                if doc.local_path and Path(doc.local_path).exists():
                    is_parquet = doc.local_path.endswith(".parquet")
                    category = "parquet" if is_parquet else "original"
                    filename = Path(doc.local_path).name
                    key = f"{base_key}/{category}/{filename}"
                    content_type = (
                        "application/octet-stream"
                        if is_parquet
                        else doc.content_type or "application/octet-stream"
                    )
                    storage.upload_file(doc.local_path, key, content_type=content_type)
                    if is_parquet:
                        doc.s3_parquet_key = key
                    else:
                        doc.s3_original_key = key

                if doc.docling_json:
                    key = f"{base_key}/json/document.json"
                    storage.upload_text(doc.docling_json, key, content_type="application/json")
                    doc.s3_json_key = key

                if doc.markdown_output:
                    key = f"{base_key}/markdown/document.md"
                    storage.upload_text(doc.markdown_output, key, content_type="text/markdown")
                    doc.s3_markdown_key = key

                embeddings_path = getattr(doc, "_embeddings_path", None)
                if embeddings_path and Path(embeddings_path).exists():
                    key = f"{base_key}/embeddings/embeddings.json"
                    storage.upload_file(
                        embeddings_path, key, content_type="application/json"
                    )
                    doc.s3_embeddings_key = key

                doc.status = DocumentStatus.STORED
                stored_count += 1

                logger.info("Document stored", doc_id=doc.id, job_run_id=job_run_id)
                db.commit()

            except Exception as e:
                logger.error("Storage failed", doc_id=doc.id, error=str(e))
                doc.status = DocumentStatus.FAILED
                doc.error_message = f"Storage failed: {str(e)}"
                doc.retry_count += 1
                db.commit()

        job.total_documents_stored = stored_count
        db.commit()

        logger.info(
            "Storage stage complete",
            job_run_id=job_run_id,
            stored=stored_count,
            total=len(documents),
        )

        return {
            "job_run_id": job_run_id,
            "stored_count": stored_count,
            "status": "stored",
        }

    except Exception as exc:
        db.rollback()
        logger.error("Storage stage failed", job_run_id=job_run_id, error=str(exc))
        raise self.retry(exc=exc)
    finally:
        db.close()


@shared_task(bind=True)
def validate_job_task(self, store_result: dict) -> dict:
    """
    Stage 8: VALIDATE
    Validate job completion. Check all documents are stored, update job status.
    """
    job_run_id = store_result["job_run_id"]
    db = _get_db()
    try:
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if not job:
            raise ValueError(f"Job run {job_run_id} not found")

        total_docs = db.query(Document).filter(Document.job_run_id == job.id).count()
        stored_docs = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status == DocumentStatus.STORED,
        ).count()
        failed_docs = db.query(Document).filter(
            Document.job_run_id == job.id,
            Document.status == DocumentStatus.FAILED,
        ).count()

        if stored_docs == total_docs and total_docs > 0:
            job.status = JobStatus.COMPLETED
        elif stored_docs > 0:
            job.status = JobStatus.COMPLETED
            job.error_message = f"Partial completion: {stored_docs}/{total_docs} stored, {failed_docs} failed"
        else:
            job.status = JobStatus.FAILED
            job.error_message = f"No documents stored: {failed_docs} failed out of {total_docs}"

        db.commit()

        logger.info(
            "Job validation complete",
            job_run_id=job_run_id,
            status=job.status.value,
            total=total_docs,
            stored=stored_docs,
            failed=failed_docs,
        )

        return {
            "job_run_id": job_run_id,
            "status": job.status.value,
            "total_documents": total_docs,
            "stored_documents": stored_docs,
            "failed_documents": failed_docs,
        }

    except Exception as exc:
        db.rollback()
        logger.error("Validation failed", job_run_id=job_run_id, error=str(exc))
        job = db.query(JobRun).filter(JobRun.job_run_id == job_run_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = f"Validation failed: {str(exc)}"
            db.commit()
        raise
    finally:
        db.close()


@shared_task(bind=True)
def run_full_pipeline_task(self, job_run_id: int, seed_urls: list[str]) -> dict:
    """
    Orchestrator: Run the full 8-stage pipeline as a Celery chain.

    Stages:
    1. Discovered  → discover_pages_task
    2. Indexed     → index_pages_task
    3. Job created → (part of index_pages_task)
    4. Extracted   → extract_page_task
    5. OCR         → ocr_document_task
    6. Transform   → transform_document_task (+ embed_document_task)
    7. Transfer    → store_document_task
    8. Validate    → validate_job_task
    """
    logger.info("Starting full pipeline", job_run_id=job_run_id, seeds=seed_urls)

    pipeline = chain(
        discover_pages_task.s(job_run_id, seed_urls),
        index_pages_task.s(),
        extract_page_task.s(),
        ocr_document_task.s(),
        transform_document_task.s(),
        embed_document_task.s(),
        store_document_task.s(),
        validate_job_task.s(),
    )

    result = pipeline.apply_async()
    logger.info("Pipeline dispatched", job_run_id=job_run_id, task_id=result.id)

    return {
        "job_run_id": job_run_id,
        "celery_task_id": result.id,
        "status": "dispatched",
    }
