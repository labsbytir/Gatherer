import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

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
    schema: dict = field(default_factory=dict)


class ParquetTransformer:
    """
    Transforms Docling JSON output into Apache Parquet format.

    Flattens the nested DoclingDocument JSON into a columnar format
    suitable for analytics, vector search, and data warehousing.
    """

    def __init__(self, temp_dir: str | None = None):
        self.temp_dir = Path(temp_dir or settings.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def transform_docling_json(
        self,
        docling_json: dict,
        document_id: str,
        source_url: str,
        output_filename: str | None = None,
    ) -> TransformResult:
        """
        Transform a DoclingDocument JSON into a Parquet file.

        Each row represents a text chunk/section from the document.
        """
        rows = self._flatten_docling_document(docling_json, document_id, source_url)
        return self._write_parquet(rows, output_filename or f"{document_id}.parquet")

    def transform_records(
        self,
        records: list[dict],
        output_filename: str,
        schema: pa.Schema | None = None,
    ) -> TransformResult:
        """
        Transform a list of records (dicts) into a Parquet file.
        """
        return self._write_parquet(records, output_filename, schema)

    def transform_json_file(
        self,
        json_path: str | Path,
        output_filename: str | None = None,
    ) -> TransformResult:
        """
        Transform a JSON file (array of objects or single object) into Parquet.
        """
        json_path = Path(json_path)
        data = json.loads(json_path.read_text(encoding="utf-8"))

        if isinstance(data, dict):
            if "rows" in data and "columns" in data:
                records = self._convert_columnar_format(data)
            else:
                records = [data]
        elif isinstance(data, list):
            records = data
        else:
            raise ValueError(f"Unsupported JSON structure in {json_path}")

        output_name = output_filename or json_path.stem + ".parquet"
        return self._write_parquet(records, output_name)

    def _flatten_docling_document(
        self,
        doc: dict,
        document_id: str,
        source_url: str,
    ) -> list[dict]:
        """
        Flatten a DoclingDocument into rows for Parquet.

        Extracts text chunks with their metadata, hierarchy, and page info.
        """
        rows: list[dict] = []

        doc_name = doc.get("name", "")
        origin = doc.get("origin", {})
        mimetype = origin.get("mimetype", "")

        texts = doc.get("texts", [])
        for i, text_item in enumerate(texts):
            row = {
                "document_id": document_id,
                "source_url": source_url,
                "document_name": doc_name,
                "mimetype": mimetype,
                "chunk_index": i,
                "text": text_item.get("text", ""),
                "label": text_item.get("label", ""),
                "page_no": text_item.get("page_no"),
                "prov": json.dumps(text_item.get("prov", []), ensure_ascii=False),
            }
            rows.append(row)

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
                "prov": "[]",
            })

        tables = doc.get("tables", [])
        for t_idx, table in enumerate(tables):
            table_data = table.get("data", {})
            num_rows = table_data.get("num_rows", 0)
            num_cols = table_data.get("num_cols", 0)
            cells = table_data.get("table_cells", [])

            rows.append({
                "document_id": document_id,
                "source_url": source_url,
                "document_name": doc_name,
                "mimetype": mimetype,
                "chunk_index": len(rows),
                "text": json.dumps(table_data, ensure_ascii=False),
                "label": "table",
                "page_no": table.get("prov", [{}])[0].get("page_no") if table.get("prov") else None,
                "prov": json.dumps(table.get("prov", []), ensure_ascii=False),
                "table_index": t_idx,
                "table_rows": num_rows,
                "table_cols": num_cols,
                "table_cells": len(cells),
            })

        pictures = doc.get("pictures", [])
        for p_idx, picture in enumerate(pictures):
            caption = ""
            for caption_ref in picture.get("captions", []):
                caption_ref_idx = caption_ref.get("cref", "")
                for text_item in texts:
                    if text_item.get("self_ref") == caption_ref_idx:
                        caption = text_item.get("text", "")
                        break

            rows.append({
                "document_id": document_id,
                "source_url": source_url,
                "document_name": doc_name,
                "mimetype": mimetype,
                "chunk_index": len(rows),
                "text": caption,
                "label": "picture",
                "page_no": picture.get("prov", [{}])[0].get("page_no") if picture.get("prov") else None,
                "prov": json.dumps(picture.get("prov", []), ensure_ascii=False),
                "picture_index": p_idx,
                "picture_caption": caption,
            })

        metadata_row = {
            "document_id": document_id,
            "source_url": source_url,
            "document_name": doc_name,
            "mimetype": mimetype,
            "chunk_index": -1,
            "text": json.dumps(doc.get("metadata", {}), ensure_ascii=False) if doc.get("metadata") else "",
            "label": "metadata",
            "page_no": None,
            "prov": "[]",
        }
        rows.append(metadata_row)

        return rows

    def _write_parquet(
        self,
        records: list[dict],
        output_filename: str,
        schema: pa.Schema | None = None,
    ) -> TransformResult:
        """
        Write records to a Parquet file.
        """
        if not records:
            records = [{}]

        table = pa.Table.from_pylist(records, schema=schema)
        output_path = self.temp_dir / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pq.write_table(
            table,
            output_path,
            compression="snappy",
            version="2.6",
        )

        logger.info(
            "Wrote Parquet file",
            path=str(output_path),
            rows=table.num_rows,
            columns=table.num_columns,
        )

        return TransformResult(
            parquet_path=output_path,
            row_count=table.num_rows,
            columns=table.column_names,
            schema={col: str(table.schema.field(col).type) for col in table.column_names},
        )

    def _convert_columnar_format(self, data: dict) -> list[dict]:
        """
        Convert the parquet-converter's columnar JSON format to records.
        """
        columns = data.get("columns", [])
        rows = data.get("rows", [])
        col_names = [c["name"] for c in columns]
        records = []
        for row in rows:
            record = {}
            for i, name in enumerate(col_names):
                record[name] = row[i] if i < len(row) else None
            records.append(record)
        return records

    def read_parquet(self, parquet_path: str | Path) -> list[dict]:
        """
        Read a Parquet file back into a list of dicts.
        """
        table = pq.read_table(parquet_path)
        return table.to_pylist()
