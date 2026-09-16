"""
RAG "ask your drive" chat: retrieves relevant file chunks from Qdrant
for the user's query, then generates a grounded answer via an LLM call.
"""
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class ChatRequest(BaseModel):
    owner_id: int
    message: str


@router.post("/chat")
async def chat(req: ChatRequest):
    # TODO: retrieve top-k chunks from Qdrant, build prompt, call LLM.
    return {"answer": "not implemented", "sources": []}
