import json

import pytest

from harvester.transform import ParquetTransformer


class TestParquetTransformer:
    def test_transform_simple_records(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        records = [
            {"id": "1", "text": "Hello world", "label": "greeting"},
            {"id": "2", "text": "Goodbye world", "label": "farewell"},
        ]
        result = transformer.transform_records(records, "test.parquet")

        assert result.parquet_path.exists()
        assert result.row_count == 2
        assert "id" in result.columns
        assert "text" in result.columns
        assert "label" in result.columns

        read_back = transformer.read_parquet(result.parquet_path)
        assert len(read_back) == 2
        assert read_back[0]["text"] == "Hello world"

    def test_transform_docling_json(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        docling_json = {
            "name": "test_doc",
            "origin": {"mimetype": "application/pdf", "filename": "test.pdf"},
            "texts": [
                {
                    "text": "This is the first paragraph.",
                    "label": "paragraph",
                    "page_no": 1,
                    "prov": [{"page_no": 1, "bbox": {"l": 0, "t": 0, "r": 100, "b": 50}}],
                    "self_ref": "#/texts/0",
                },
                {
                    "text": "This is the second paragraph.",
                    "label": "paragraph",
                    "page_no": 1,
                    "prov": [{"page_no": 1, "bbox": {"l": 0, "t": 60, "r": 100, "b": 100}}],
                    "self_ref": "#/texts/1",
                },
            ],
            "tables": [],
            "pictures": [],
            "metadata": {"title": "Test Document"},
        }

        result = transformer.transform_docling_json(
            docling_json=docling_json,
            document_id="doc-123",
            source_url="https://example.com/test.pdf",
        )

        assert result.parquet_path.exists()
        assert result.row_count >= 2

        read_back = transformer.read_parquet(result.parquet_path)
        text_rows = [r for r in read_back if r.get("label") == "paragraph"]
        assert len(text_rows) == 2
        assert text_rows[0]["text"] == "This is the first paragraph."
        assert text_rows[0]["document_id"] == "doc-123"

    def test_transform_json_file(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        json_file = tmp_path / "input.json"
        json_file.write_text(json.dumps([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]))

        result = transformer.transform_json_file(json_file)
        assert result.parquet_path.exists()
        assert result.row_count == 2

        read_back = transformer.read_parquet(result.parquet_path)
        assert read_back[0]["name"] == "Alice"
        assert read_back[1]["age"] == 25

    def test_transform_empty_records(self, tmp_path):
        transformer = ParquetTransformer(temp_dir=str(tmp_path))
        result = transformer.transform_records([], "empty.parquet")
        assert result.parquet_path.exists()
        assert result.row_count == 1
