"""
Semantic search endpoint: embeds the query, queries Qdrant, returns
ranked file_ids for the C++ API to hydrate with metadata.
"""
from fastapi import APIRouter

router = APIRouter()


@router.get("/search")
async def search(q: str, owner_id: int):
    # TODO: embed query, search Qdrant filtered by owner_id, return hits.
    return {"query": q, "results": []}
