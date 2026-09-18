
# DriveX

Self-hosted, production-grade cloud file storage platform (Google Drive analogue) built to demonstrate full-stack systems engineering: relational data modeling, object storage at scale, asynchronous AI/ML pipelines, and horizontally-scalable backend architecture.

DriveX differentiates itself from a typical file-manager CRUD clone with an AI layer — semantic search, auto-tagging, duplicate detection, and a "chat with your drive" RAG assistant.

See [`docs/architecture.md`](docs/architecture.md) for the full system design and [`docs/schema-design.md`](docs/schema-design.md) for the database schema.

## Stack

| Layer | Technology |
|---|---|
| Backend API | C++ (Drogon) |
| Frontend | HTMX + Tailwind CSS |
| RDBMS | MySQL 8 |
| Cache / Sessions | Redis |
| Object Storage | MinIO (S3-compatible) |
| Vector DB | Qdrant |
| Message Queue | RabbitMQ |
| AI/ML Workers | Python (FastAPI + Celery) |
| Auth | JWT (RS256) + argon2id |
| Reverse Proxy | Nginx |
| Observability | OpenTelemetry + Prometheus + Grafana |
| CI/CD | GitHub Actions + Docker + Kubernetes |

## Getting started (dev)

```bash
# Bring up MySQL, Redis, MinIO, RabbitMQ, Qdrant
docker compose -f infra/docker/docker-compose.dev.yml up -d

# Build & run the C++ API
cd api
mkdir build && cd build
cmake .. && make -j$(nproc)
./drivex_api

# Run ML workers
cd ml-workers
pip install -r requirements.txt
celery -A app.celery_app worker --loglevel=info
uvicorn app.main:app --reload --port 8001
```

## Repository layout

```
drive-clone/
├── api/            # C++ backend (Drogon)
├── ml-workers/     # Python FastAPI + Celery AI pipeline
├── frontend/       # HTMX + Tailwind server-rendered UI
├── db/             # SQL migrations & schema
├── infra/          # Docker, Kubernetes, Terraform
├── load-tests/     # Staged benchmark suite
└── docs/           # Architecture, API spec, schema docs
```

## Build plan

1. **Core CRUD** — auth, folder tree, file upload/download via pre-signed MinIO URLs
2. **Sharing & Permissions** — RBAC, expiring share links
3. **Async ML Pipeline** — RabbitMQ + Celery embeddings, semantic search
4. **RAG Chat & Dedup** — "ask your drive" chat, duplicate detection
5. **Scale & Harden** — Redis caching, read replicas, load testing, observability

See `docs/architecture.md` for full phase breakdown and load-test targets.

## License

MIT (or update as needed).
