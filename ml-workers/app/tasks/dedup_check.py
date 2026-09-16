"""
Checks a newly-uploaded file against existing files (same owner/workspace)
for near-duplicates using embedding cosine similarity (documents) or
perceptual hashing (images), and flags likely duplicates.
"""
from app.celery_app import celery_app


@celery_app.task(name="tasks.dedup_check")
def dedup_check(file_id: int) -> dict:
    # TODO: query Qdrant for nearest neighbors above a similarity threshold.
    return {"file_id": file_id, "duplicates": []}
