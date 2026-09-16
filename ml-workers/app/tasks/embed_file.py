"""
Generates a semantic embedding for a newly-uploaded file (text extraction
for docs, captioning for images, etc.) and upserts it into Qdrant, keyed
by file_id, for use by semantic search and RAG chat.
"""
from app.celery_app import celery_app


@celery_app.task(name="tasks.embed_file")
def embed_file(file_id: int, storage_key: str, mime_type: str) -> dict:
    # TODO: download object from MinIO, extract text/generate embedding,
    # upsert into Qdrant collection "file_embeddings".
    return {"file_id": file_id, "status": "not_implemented"}
