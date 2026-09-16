"""
Thin wrapper around the embedding model (sentence-transformers) shared by
embed_file, semantic search, and RAG retrieval, so the model is loaded
once per worker process.
"""
from sentence_transformers import SentenceTransformer

_model = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model
