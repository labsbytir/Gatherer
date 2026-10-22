import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class OCRResult:
    text: str
    markdown: str
    docling_json: dict
    metadata: dict = field(default_factory=dict)
    num_pages: int = 0
    ocr_applied: bool = False
    file_type: str = ""


class DoclingProcessor:
    """
    Document processor using Docling for OCR and structured extraction.

    Converts documents (PDF, DOCX, PPTX, images, HTML, etc.) into:
    - Plain text
    - Markdown
    - Structured JSON (DoclingDocument format)
    - Metadata (page count, tables, etc.)
    """

    def __init__(
        self,
        ocr_languages: str | None = None,
        temp_dir: str | None = None,
    ):
        self.ocr_languages = ocr_languages or settings.ocr_languages
        self.temp_dir = Path(temp_dir or settings.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self._converter = None

    def _get_converter(self):
        if self._converter is None:
            from docling.document_converter import DocumentConverter

            self._converter = DocumentConverter()
        return self._converter

    def process_file(
        self,
        file_path: str | Path,
        source_url: str | None = None,
    ) -> OCRResult:
        """
        Process a local file using Docling.

        Returns OCRResult with extracted text, markdown, JSON, and metadata.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info("Processing document with Docling", file=str(file_path))

        converter = self._get_converter()
        result = converter.convert(str(file_path))
        doc = result.document

        markdown = doc.export_to_markdown()
        text = doc.export_to_text() if hasattr(doc, "export_to_text") else markdown
        docling_json = doc.export_to_dict()

        metadata = self._extract_metadata(doc, result)

        file_type = file_path.suffix.lower().lstrip(".")
        num_pages = self._get_page_count(doc, metadata)
        ocr_applied = self._check_ocr_applied(result)

        return OCRResult(
            text=text,
            markdown=markdown,
            docling_json=docling_json,
            metadata=metadata,
            num_pages=num_pages,
            ocr_applied=ocr_applied,
            file_type=file_type,
        )

    def process_url(
        self,
        url: str,
        download_dir: str | Path | None = None,
    ) -> tuple[OCRResult, Path]:
        """
        Download and process a document from a URL using Docling.

        Returns (OCRResult, local_file_path).
        """
        converter = self._get_converter()
        logger.info("Processing document from URL with Docling", url=url)

        result = converter.convert(url)
        doc = result.document

        markdown = doc.export_to_markdown()
        text = doc.export_to_text() if hasattr(doc, "export_to_text") else markdown
        docling_json = doc.export_to_dict()
        metadata = self._extract_metadata(doc, result)

        local_path = self._save_temp_copy(doc, url, download_dir)

        file_type = self._guess_file_type(url, metadata)
        num_pages = self._get_page_count(doc, metadata)
        ocr_applied = self._check_ocr_applied(result)

        return OCRResult(
            text=text,
            markdown=markdown,
            docling_json=docling_json,
            metadata=metadata,
            num_pages=num_pages,
            ocr_applied=ocr_applied,
            file_type=file_type,
        ), local_path

    def process_bytes(
        self,
        content: bytes,
        filename: str,
        content_type: str | None = None,
    ) -> OCRResult:
        """
        Process raw bytes as a document.
        """
        temp_file = self.temp_dir / filename
        temp_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file.write_bytes(content)

        try:
            return self.process_file(temp_file)
        finally:
            pass

    def _extract_metadata(self, doc, result) -> dict:
        metadata: dict[str, Any] = {}

        if hasattr(doc, "pages") and doc.pages:
            metadata["page_count"] = len(doc.pages)

        if hasattr(doc, "tables") and doc.tables:
            metadata["table_count"] = len(doc.tables)

        if hasattr(doc, "pictures") and doc.pictures:
            metadata["picture_count"] = len(doc.pictures)

        if hasattr(result, "status"):
            metadata["conversion_status"] = str(result.status)

        if hasattr(doc, "name"):
            metadata["document_name"] = doc.name

        if hasattr(doc, "origin") and doc.origin:
            origin = doc.origin
            if hasattr(origin, "filename"):
                metadata["origin_filename"] = origin.filename
            if hasattr(origin, "mimetype"):
                metadata["mimetype"] = origin.mimetype

        return metadata

    def _get_page_count(self, doc, metadata: dict) -> int:
        if "page_count" in metadata:
            return metadata["page_count"]
        if hasattr(doc, "pages") and doc.pages:
            return len(doc.pages)
        return 0

    def _check_ocr_applied(self, result) -> bool:
        if hasattr(result, "document") and hasattr(result.document, "pages"):
            for page in result.document.pages.values() if hasattr(result.document.pages, "values") else []:
                if hasattr(page, "ocr_applied") and page.ocr_applied:
                    return True
        return False

    def _guess_file_type(self, url: str, metadata: dict) -> str:
        if "mimetype" in metadata:
            mimetype = metadata["mimetype"]
            type_map = {
                "application/pdf": "pdf",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
                "text/html": "html",
                "image/png": "png",
                "image/jpeg": "jpg",
                "image/tiff": "tiff",
            }
            return type_map.get(mimetype, mimetype.split("/")[-1])
        return url.split(".")[-1].split("?")[0].lower() if "." in url else "unknown"

    def _save_temp_copy(self, doc, url: str, download_dir: str | Path | None) -> Path:
        base_dir = Path(download_dir) if download_dir else self.temp_dir
        base_dir.mkdir(parents=True, exist_ok=True)

        filename = os.path.basename(url.split("?")[0]) or "document"
        if "." not in filename:
            filename += ".bin"

        temp_path = base_dir / filename
        return temp_path

    def to_json_string(self, ocr_result: OCRResult) -> str:
        """
        Serialize the Docling JSON output to a string.
        """
        return json.dumps(ocr_result.docling_json, ensure_ascii=False, indent=2)
