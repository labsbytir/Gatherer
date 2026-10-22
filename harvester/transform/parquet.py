import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class TransformResult:
    parquet_path: Path
    row_count: int
    columns: list[str]
    method: str = "pyarrow"


class ParquetTransformer:
    """
    Transforms Docling JSON output into Apache Parquet format.

    Primary method: parquet-converter (Go binary) as specified
    Fallback: PyArrow (pure Python)

    Reference: https://github.com/iwillwen/parquet-converter
    """

    def __init__(self, temp_dir: str | None = None):
        self.temp_dir = Path(temp_dir or settings.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.use_go_converter = settings.use_parquet_converter
        self.go_binary = settings.parquet_converter_path

    def transform(
        self,
        docling_json: dict,
        document_id: str,
        source_url: str,
        output_filename: str,
    ) -> TransformResult:
        rows = self._flatten_docling(docling_json, document_id, source_url)
        output_path = self.temp_dir / output_filename

        if self.use_go_converter and self._go_binary_exists():
            try:
                return self._transform_with_go(rows, output_path)
            except Exception as e:
                logger.warning("parquet-converter failed, falling back to PyArrow", error=str(e))

        return self._transform_with_pyarrow(rows, output_path)

    def _go_binary_exists(self) -> bool:
        return Path(self.go_binary).exists() or self._which(self.go_binary) is not None

    @staticmethod
    def _which(binary: str) -> str | None:
        for path in os.environ.get("PATH", "").split(os.pathsep):
            candidate = Path(path) / binary
            if candidate.exists() and os.access(candidate, os.X_OK):
                return str(candidate)
        if "/" in binary and Path(binary).exists():
            return binary
        return None

    def _transform_with_go(self, rows: list[dict], output_path: Path) -> TransformResult:
        """
        Use parquet-converter (Go) to convert JSON to Parquet.

        parquet-converter expects a JSON file with:
        {
          "columns": [{"name": "col1", "val_type": "UTF8", "repetition_type": "REQUIRED"}, ...],
          "rows": [[val1, val2, ...], ...]
        }
        """
        columns = self._infer_columns(rows)
        column_defs = [
            {"name": c["name"], "val_type": c["val_type"], "repetition_type": "OPTIONAL"}
            for c in columns
        ]
        row_values = [[row.get(c["name"]) for c in columns] for row in rows]

        input_json = {
            "columns": column_defs,
            "rows": row_values,
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, dir=str(self.temp_dir)
        ) as f:
            json.dump(input_json, f)
            input_file = f.name

        try:
            binary = self._which(self.go_binary) or self.go_binary
            cmd = [binary, "json2parquet", "-file", input_file, "-out", str(output_path)]
            logger.info("Running parquet-converter", cmd=" ".join(cmd))
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                raise RuntimeError(f"parquet-converter failed: {result.stderr}")

            table = pq.read_table(output_path)
            return TransformResult(
                parquet_path=output_path,
                row_count=table.num_rows,
                columns=table.column_names,
                method="parquet-converter",
            )
        finally:
            Path(input_file).unlink(missing_ok=True)

    def _transform_with_pyarrow(self, rows: list[dict], output_path: Path) -> TransformResult:
        if not rows:
            rows = [{}]
        table = pa.Table.from_pylist(rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, output_path, compression="snappy", version="2.6")

        logger.info("Wrote Parquet via PyArrow", path=str(output_path), rows=table.num_rows)
        return TransformResult(
            parquet_path=output_path,
            row_count=table.num_rows,
            columns=table.column_names,
            method="pyarrow",
        )

    def _flatten_docling(self, doc: dict, document_id: str, source_url: str) -> list[dict]:
        rows = []
        doc_name = doc.get("name", "")
        origin = doc.get("origin", {})
        mimetype = origin.get("mimetype", "")

        for i, text_item in enumerate(doc.get("texts", [])):
            rows.append({
                "document_id": document_id,
                "source_url": source_url,
                "document_name": doc_name,
                "mimetype": mimetype,
                "chunk_index": i,
                "text": text_item.get("text", ""),
                "label": text_item.get("label", ""),
                "page_no": text_item.get("page_no"),
            })

        if not rows:
            rows.append({
                "document_id": document_id,
                "source_url": source_url,
                "document_name": doc_name,
                "mimetype": mimetype,
                "chunk_index": 0,
                "text": doc.get("markdown", "") or "",
                "label": "full_document",
                "page_no": None,
            })

        return rows

    def _infer_columns(self, rows: list[dict]) -> list[dict]:
        if not rows:
            return []
        type_map = {str: "UTF8", int: "INT64", float: "DOUBLE", bool: "BOOLEAN"}
        columns = []
        seen = set()
        for row in rows:
            for key, value in row.items():
                if key not in seen:
                    seen.add(key)
                    val_type = type_map.get(type(value), "UTF8")
                    columns.append({"name": key, "val_type": val_type})
        return columns
