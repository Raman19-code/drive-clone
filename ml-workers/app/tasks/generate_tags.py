"""
Auto-tags a file based on its content/embedding (e.g. "invoice",
"screenshot", "resume") and writes tags back via the API/DB.
"""
from app.celery_app import celery_app


@celery_app.task(name="tasks.generate_tags")
def generate_tags(file_id: int) -> dict:
    # TODO: classify against a fixed or embedding-derived tag taxonomy.
    return {"file_id": file_id, "tags": []}
