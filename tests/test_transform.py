import json

import pytest

from harvester.transform import ParquetTransformer


class TestParquetTransformer:
    def test_transform_docling_json(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        transformer.use_go_converter = False

        docling_json = {
            "name": "test.pdf",
            "origin": {"mimetype": "application/pdf", "filename": "test.pdf"},
            "texts": [
                {"text": "First paragraph.", "label": "paragraph", "page_no": 1},
                {"text": "Second paragraph.", "label": "paragraph", "page_no": 1},
            ],
        }

        result = transformer.transform(
            docling_json=docling_json,
            document_id="doc-1",
            source_url="https://example.com/test.pdf",
            output_filename="test.parquet",
        )

        assert result.parquet_path.exists()
        assert result.row_count == 2
        assert "text" in result.columns
        assert "document_id" in result.columns

    def test_transform_empty_docling(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        transformer.use_go_converter = False

        result = transformer.transform(
            docling_json={"name": "empty", "texts": []},
            document_id="doc-2",
            source_url="https://example.com/empty.pdf",
            output_filename="empty.parquet",
        )

        assert result.parquet_path.exists()
        assert result.row_count >= 1

    def test_infer_columns(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        rows = [
            {"name": "Alice", "age": 30, "score": 9.5, "active": True},
            {"name": "Bob", "age": 25, "score": 7.0, "active": False},
        ]
        cols = transformer._infer_columns(rows)
        names = [c["name"] for c in cols]
        assert "name" in names
        assert "age" in names
        assert "score" in names
        assert "active" in names

        type_map = {c["name"]: c["val_type"] for c in cols}
        assert type_map["name"] == "UTF8"
        assert type_map["age"] == "INT64"
        assert type_map["score"] == "DOUBLE"
        assert type_map["active"] == "BOOLEAN"
