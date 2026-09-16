# DriveX Architecture

## Overview

DriveX is a self-hosted, production-grade cloud file storage platform (a Google
Drive analogue) built to demonstrate full-stack systems engineering: relational
data modeling, object storage at scale, asynchronous AI/ML pipelines, and
horizontally-scalable backend architecture. It differentiates itself from a
typical CRUD file-manager clone with an AI layer: semantic search, auto-tagging,
duplicate detection, and a "chat with your drive" RAG assistant.

## Goals

- Production-level reliability: 100% upload/download success under sustained concurrent load
- Horizontal scalability at every layer (API, DB, storage, ML workers)
- A staged load-test benchmark suite
- A resume/portfolio-grade systems project

## System diagram

```
Client (HTMX) -> CDN -> Nginx (TLS, LB) -> C++ API instances (stateless, Drogon)
                                                |
        +---------------+---------------+------+------+---------------+
        v               v               v             v               v
     Redis           MySQL          MinIO         RabbitMQ         Qdrant
   (cache/session) (primary+repl) (blobs, presigned)  |          (vectors)
                                                        v
                                          Python ML Workers (Celery:
                                          embed, tag, dedup, RAG chat)
```

## Key design decisions

- **Pre-signed URLs**: clients upload/download directly to/from MinIO,
  bypassing the API server for file bytes. This is the single biggest
  scalability lever — the API only ever handles metadata.
- **Stateless API tier**: no server-side session or file-handle state,
  enabling trivial horizontal scaling behind the load balancer.
- **Async ML pipeline**: embedding/tagging/dedup work never blocks the
  upload request path — it's triggered via RabbitMQ after upload completion.
- **Sharding-ready schema**: folder/file tables are designed so
  `owner_id`/`workspace_id` can become a shard key later.

## Phased build plan

1. **Core CRUD** (Weeks 1-2) — auth, folder tree, upload/download, basic UI
2. **Sharing & Permissions** (Week 3) — RBAC, expiring share links
3. **Async ML Pipeline** (Weeks 4-5) — RabbitMQ, Celery embeddings, semantic search
4. **RAG Chat & Dedup** (Week 6) — "ask your drive" chat, duplicate detection
5. **Scale & Harden** (Weeks 7-8) — Redis caching, read replicas, load testing, observability

## Load testing

Modeled on a prior staged-sweep benchmark methodology, adapted for
file-transfer workloads (bytes-in-flight, not just request count).

| Stage | Workers | Workload | Target |
|---|---|---|---|
| 1 | 50 | Metadata ops | Baseline latency |
| 2 | 500 | Mixed + small files (<=1MB) | p99 < 100ms metadata |
| 3 | 2,000 | Mixed + medium files (10-50MB) | Sustained MB/s |
| 4 | 5,000 | Large files (100MB+), resume-on-failure | 100% success incl. resumes |
| 5 | 10,000 | Full mixed workload | Replica lag, cache hit rate, queue depth |

## Production hardening

- Idempotent uploads via client-generated upload IDs
- Quota enforcement before pre-signed URL issuance, reconciled post-upload
- Soft delete + 30-day trash retention
- Full audit logging
- Per-user/IP rate limiting at Nginx + API middleware
- Graceful degradation if the ML pipeline is down
- MySQL backups + MinIO versioning/replication
- TLS everywhere, including internal service traffic where feasible
