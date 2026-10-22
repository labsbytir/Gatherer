import pytest

from harvester.embeddings import EmbeddingService


class TestEmbeddingService:
    def test_chunk_text_short(self):
        service = EmbeddingService.__new__(EmbeddingService)
        service.chunk_size = 512
        service.chunk_overlap = 50

        chunks = service.chunk_text("Hello world")
        assert len(chunks) == 1
        assert chunks[0][1] == "Hello world"

    def test_chunk_text_empty(self):
        service = EmbeddingService.__new__(EmbeddingService)
        service.chunk_size = 512
        service.chunk_overlap = 50

        chunks = service.chunk_text("")
        assert len(chunks) == 0

    def test_chunk_text_long(self):
        service = EmbeddingService.__new__(EmbeddingService)
        service.chunk_size = 100
        service.chunk_overlap = 20

        text = "Sentence one. Sentence two. Sentence three. Sentence four. Sentence five. " * 10
        chunks = service.chunk_text(text)

        assert len(chunks) > 1
        for idx, chunk_text, start, end in chunks:
            assert len(chunk_text) > 0
            assert start < end
            assert end <= len(text)

    def test_chunk_text_overlap(self):
        service = EmbeddingService.__new__(EmbeddingService)
        service.chunk_size = 50
        service.chunk_overlap = 10

        text = "A" * 200
        chunks = service.chunk_text(text)

        assert len(chunks) > 1
        for i in range(len(chunks) - 1):
            current_end = chunks[i][3]
            next_start = chunks[i + 1][2]
            assert next_start < current_end

    def test_embedding_result_to_dict(self):
        from harvester.embeddings.embedder import ChunkEmbedding, EmbeddingResult

        result = EmbeddingResult(
            document_id="doc-1",
            chunks=[
                ChunkEmbedding(chunk_index=0, text="Hello", embedding=[0.1, 0.2, 0.3]),
            ],
            model_name="test-model",
            embedding_dimension=3,
        )

        d = result.to_dict()
        assert d["document_id"] == "doc-1"
        assert len(d["chunks"]) == 1
        assert d["chunks"][0]["text"] == "Hello"
        assert d["chunks"][0]["embedding"] == [0.1, 0.2, 0.3]

    def test_embedding_result_to_json(self):
        import json

        from harvester.embeddings.embedder import ChunkEmbedding, EmbeddingResult

        result = EmbeddingResult(
            document_id="doc-1",
            chunks=[
                ChunkEmbedding(chunk_index=0, text="Hello", embedding=[0.1]),
            ],
            model_name="test-model",
            embedding_dimension=1,
        )

        json_str = result.to_json()
        parsed = json.loads(json_str)
        assert parsed["document_id"] == "doc-1"
