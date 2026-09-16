"""
FastAPI control plane for the ML workers: exposes semantic search and
RAG chat endpoints, proxied to by the C++ API's /search and /chat routes.
"""
from fastapi import FastAPI
from app.api import search, chat

app = FastAPI(title="DriveX ML Workers")
app.include_router(search.router)
app.include_router(chat.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
