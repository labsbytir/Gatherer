import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class ChunkEmbedding:
    chunk_index: int
    text: str
    embedding: list[float]
    start_char: int = 0
    end_char: int = 0


@dataclass
class EmbeddingResult:
    document_id: str
    chunks: list[ChunkEmbedding] = field(default_factory=list)
    model_name: str = ""
    embedding_dimension: int = 0

    def to_dict(self) -> dict:
        return {
            "document_id": self.document_id,
            "model_name": self.model_name,
            "embedding_dimension": self.embedding_dimension,
            "chunks": [
                {
                    "chunk_index": c.chunk_index,
                    "text": c.text,
                    "embedding": c.embedding,
                    "start_char": c.start_char,
                    "end_char": c.end_char,
                }
                for c in self.chunks
            ],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


class EmbeddingService:
    """
    Local embedding service using sentence-transformers.

    Generates vector embeddings for document chunks entirely locally,
    without any external API calls.
    """

    def __init__(
        self,
        model_name: str | None = None,
        device: str | None = None,
        batch_size: int | None = None,
        chunk_size: int | None = None,
        chunk_overlap: int | None = None,
    ):
        self.model_name = model_name or settings.embedding_model
        self.device = device or settings.embedding_device
        self.batch_size = batch_size or settings.embedding_batch_size
        self.chunk_size = chunk_size or settings.embedding_chunk_size
        self.chunk_overlap = chunk_overlap or settings.embedding_chunk_overlap
        self._model = None

    def _get_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            logger.info("Loading embedding model", model=self.model_name, device=self.device)
            self._model = SentenceTransformer(self.model_name, device=self.device)
            logger.info(
                "Embedding model loaded",
                model=self.model_name,
                dimension=self._model.get_sentence_embedding_dimension(),
            )
        return self._model

    def chunk_text(
        self,
        text: str,
        chunk_size: int | None = None,
        chunk_overlap: int | None = None,
    ) -> list[tuple[int, str, int, int]]:
        """
        Split text into overlapping chunks.

        Returns list of (index, chunk_text, start_char, end_char) tuples.
        """
        chunk_size = chunk_size or self.chunk_size
        chunk_overlap = chunk_overlap or self.chunk_overlap

        if not text or len(text) <= chunk_size:
            return [(0, text, 0, len(text))] if text else []

        chunks = []
        start = 0
        index = 0

        while start < len(text):
            end = start + chunk_size
            if end >= len(text):
                chunk_text = text[start:]
                chunks.append((index, chunk_text, start, len(text)))
                break

            chunk_end = end
            for sep in ["\n\n", "\n", ". ", "! ", "? ", " ", ""]:
                if sep:
                    last_sep = text.rfind(sep, start + chunk_size - chunk_overlap, end)
                    if last_sep > start:
                        chunk_end = last_sep + len(sep)
                        break

            chunk_text = text[start:chunk_end].strip()
            if chunk_text:
                chunks.append((index, chunk_text, start, chunk_end))
                index += 1

            start = chunk_end - chunk_overlap if chunk_end > chunk_overlap else chunk_end
            if start >= len(text):
                break

        return chunks

    def embed_text(self, text: str, document_id: str = "") -> EmbeddingResult:
        """
        Generate embeddings for a single text document.

        Chunks the text and generates embeddings for each chunk.
        """
        model = self._get_model()
        chunks = self.chunk_text(text)

        if not chunks:
            return EmbeddingResult(
                document_id=document_id,
                model_name=self.model_name,
                embedding_dimension=model.get_sentence_embedding_dimension(),
            )

        chunk_texts = [c[1] for c in chunks]
        embeddings = model.encode(
            chunk_texts,
            batch_size=self.batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
        )

        chunk_embeddings = []
        for i, (idx, chunk_text, start_char, end_char) in enumerate(chunks):
            chunk_embeddings.append(
                ChunkEmbedding(
                    chunk_index=idx,
                    text=chunk_text,
                    embedding=embeddings[i].tolist(),
                    start_char=start_char,
                    end_char=end_char,
                )
            )

        return EmbeddingResult(
            document_id=document_id,
            chunks=chunk_embeddings,
            model_name=self.model_name,
            embedding_dimension=int(embeddings.shape[1]),
        )

    def embed_chunks(self, chunks: list[str], document_id: str = "") -> EmbeddingResult:
        """
        Generate embeddings for pre-chunked text.
        """
        model = self._get_model()

        if not chunks:
            return EmbeddingResult(
                document_id=document_id,
                model_name=self.model_name,
                embedding_dimension=model.get_sentence_embedding_dimension(),
            )

        embeddings = model.encode(
            chunks,
            batch_size=self.batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
        )

        chunk_embeddings = [
            ChunkEmbedding(
                chunk_index=i,
                text=chunk,
                embedding=embeddings[i].tolist(),
            )
            for i, chunk in enumerate(chunks)
        ]

        return EmbeddingResult(
            document_id=document_id,
            chunks=chunk_embeddings,
            model_name=self.model_name,
            embedding_dimension=int(embeddings.shape[1]),
        )

    def save_embeddings(self, result: EmbeddingResult, output_path: str | Path) -> Path:
        """
        Save embeddings to a JSON file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result.to_json(), encoding="utf-8")
        return output_path

    def save_embeddings_parquet(
        self, result: EmbeddingResult, output_path: str | Path
    ) -> Path:
        """
        Save embeddings to a Parquet file with one row per chunk.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        records = [
            {
                "document_id": result.document_id,
                "chunk_index": c.chunk_index,
                "text": c.text,
                "embedding": c.embedding,
                "start_char": c.start_char,
                "end_char": c.end_char,
                "model_name": result.model_name,
                "embedding_dimension": result.embedding_dimension,
            }
            for c in result.chunks
        ]

        table = pa.Table.from_pylist(records)
        pq.write_table(table, output_path, compression="snappy")
        return output_path


_embedding_service: EmbeddingService | None = None


def get_embedding_service() -> EmbeddingService:
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service
