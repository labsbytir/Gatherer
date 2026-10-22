import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

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

    Used after downloading to extract vectors and data from a file
    for storage in parquet and R2.

    Reference: https://github.com/docling-project/docling
    """

    def __init__(self, temp_dir: str | None = None):
        self.temp_dir = Path(temp_dir or settings.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self._converter = None

    def _get_converter(self):
        if self._converter is None:
            from docling.document_converter import DocumentConverter
            self._converter = DocumentConverter()
        return self._converter

    def process_file(
        self, file_path: str | Path, source_url: str | None = None
    ) -> OCRResult:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info("Processing with Docling", file=str(file_path))
        converter = self._get_converter()
        result = converter.convert(str(file_path))
        doc = result.document

        markdown = doc.export_to_markdown()
        text = doc.export_to_text() if hasattr(doc, "export_to_text") else markdown
        docling_json = doc.export_to_dict()
        metadata = self._extract_metadata(doc, result)

        return OCRResult(
            text=text,
            markdown=markdown,
            docling_json=docling_json,
            metadata=metadata,
            num_pages=self._get_page_count(doc, metadata),
            ocr_applied=self._check_ocr(result),
            file_type=file_path.suffix.lower().lstrip("."),
        )

    def _extract_metadata(self, doc, result) -> dict:
        meta = {}
        if hasattr(doc, "pages") and doc.pages:
            meta["page_count"] = len(doc.pages)
        if hasattr(doc, "tables") and doc.tables:
            meta["table_count"] = len(doc.tables)
        if hasattr(doc, "pictures") and doc.pictures:
            meta["picture_count"] = len(doc.pictures)
        if hasattr(result, "status"):
            meta["conversion_status"] = str(result.status)
        return meta

    def _get_page_count(self, doc, metadata: dict) -> int:
        if "page_count" in metadata:
            return metadata["page_count"]
        if hasattr(doc, "pages") and doc.pages:
            return len(doc.pages)
        return 0

    def _check_ocr(self, result) -> bool:
        return False
