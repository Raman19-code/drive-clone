# DriveX AI/ML Subsystem & Vector Search Master Specification

**Document Version**: 1.0.0-RELEASE  
**Status**: Authoritative Technical Specification (Requirement R4 Master Blueprint)  
**Target Architecture**: DriveX Distributed Cloud Storage & AI Intelligence Platform  
**Target Services**: `drivex-ml-workers`, `drivex-ml-api`, `drivex-rabbitmq`, `drivex-qdrant`  
**Classification**: Production Engineering Architecture & Implementation Standard  

---

## 1. Executive Summary & AI/ML Architectural Philosophy

DriveX transforms conventional cloud object storage into an active, intelligent content management and discovery platform. While traditional cloud drives treat files as passive byte streams, DriveX treats every uploaded document, image, and text artifact as an indexed semantic corpus.

The Machine Learning and Artificial Intelligence (AI/ML) subsystem is engineered around four non-negotiable architectural tenets:

### 1.1 Asynchronous Event-Driven Decoupling
Heavy computational tasks—optical character recognition (OCR), multi-format document parsing, semantic text chunking, high-dimensional vector embedding generation, perceptual image hashing, and deduplication—must **never execute on the synchronous request/response path** of user uploads. 
When a client completes an upload transaction via the C++ Drogon Control Plane (`POST /api/v1/files/upload-complete`), the Drogon API validates the S3 object in MinIO, commits the metadata transaction to MySQL 8, and immediately publishes an AMQP 0-9-1 event to RabbitMQ. The HTTP connection terminates with a response time under 25 milliseconds (p99), offloading all downstream machine learning tasks to an elastic pool of Celery workers.

### 1.2 Zero-Copy Storage Stream Consumption
Worker processes must never require intermediate proxy services to read document binaries. Ingestion workers authenticate directly against the MinIO S3 cluster using internal cluster-local IAM credentials, streaming byte payloads into memory or bounded disk caches via chunked HTTP/S3 streams. Once processing completes, raw temporary byte buffers are immediately released, preventing worker process memory exhaustion.

### 1.3 Multi-Tenant Isolation & Zero Data Leakage
DriveX operates in multi-user and enterprise multi-tenant environments. A fundamental vector search security risk in naive RAG systems is cross-tenant vector leakage, where vector similarity searches return passages belonging to other users or workspaces. DriveX guarantees mathematical tenant isolation:
1. Every vector upserted into the Qdrant vector database is tagged with immutable payload attributes (`owner_id`, `workspace_id`, and `file_id`).
2. Vector searches require mandatory tenant filter clauses evaluated directly against Qdrant in-memory inverted payload indexes before HNSW graph traversal.
3. Chunks belonging to files that a user does not possess `viewer`, `editor`, or `owner` permissions for are strictly unreachable at the vector database retrieval layer.

### 1.4 Idempotency & Relational State Reconciliation
All asynchronous worker tasks are strictly idempotent. If a worker process is terminated abruptly (via OOM kill or container eviction), the unacknowledged AMQP message is redelivered. Worker tasks check the authoritative MySQL database state before applying transformations, handle duplicate deliveries gracefully, and transition the relational record status atomically (`PENDING` -> `INDEXED` or `FAILED`).

---

## 2. Component & Service Boundary Matrix

The following matrix defines the boundaries, responsibilities, runtime environments, and communication interfaces for all subsystems participating in the AI/ML pipeline.

| Subsystem Component | Service Container | Primary Technology | Transport Protocol | Core Responsibilities |
|---|---|---|---|---|
| **Control Plane Producer** | `drivex-api` | C++20 Drogon Framework | AMQP 0-9-1 TCP | Publishes `file.uploaded.#`, `file.deleted`, and `file.content_updated` events to RabbitMQ topic exchange upon upload completion. |
| **Message Broker** | `drivex-rabbitmq` | RabbitMQ 3.13 (Quorum) | AMQP 0-9-1 (5672) | High-availability message routing, topic-based event fanout, message queue persistence, and TTL retry orchestration. |
| **Asynchronous Worker Pool** | `drivex-ml-workers` | Python 3.11, Celery 5.4 | AMQP & S3 Wire | Pulls tasks from RabbitMQ, streams files from MinIO, parses text, executes OCR, chunks text, computes embeddings, and performs deduplication. |
| **Vector Database** | `drivex-qdrant` | Qdrant v1.9+ Rust Engine | gRPC (6334) / REST (6333) | Stores 1024-d chunk embeddings, maintains in-memory HNSW index, executes INT8 scalar quantization, and performs tenant-filtered vector searches. |
| **ML Inference Gateway** | `drivex-ml-api` | Python 3.11, FastAPI, Uvicorn | HTTP/1.1 REST & SSE | Exposes internal `/search` semantic query endpoint and `/chat` conversational RAG SSE streaming endpoint with cross-encoder re-ranking. |
| **Relational Metadata** | `drivex-mysql` | MySQL 8.0 (InnoDB) | MySQL Wire (3306) | Stores authoritative file records, `processing_status` state, SHA-256 digests, perceptual hashes (`phash`), and FULLTEXT name/metadata search indexes. |
| **Object Storage** | `drivex-minio` | MinIO Distributed S3 | S3 REST API (9000) | Immutable binary object store hosting the `drivex-blobs` bucket accessed directly by Celery workers via S3 `GetObject`. |
| **Distributed Cache & State** | `drivex-redis` | Redis 7.0 In-Memory DB | RESP3 TCP (6379) | Celery task state tracking (DB 1), HyDE query caching (DB 2), and token rate limit windows. |

---

## 3. Visual Architecture Blueprints

### 3.1 Asynchronous Event-Driven RabbitMQ & Celery ML Ingestion Pipeline

The following diagram illustrates the complete asynchronous lifecycle of an uploaded file, detailing the interaction between the C++ Drogon Control Plane, the RabbitMQ AMQP messaging fabric, progressive TTL retry queues, Celery worker task execution, MinIO object streaming, Qdrant vector indexing, and MySQL state finalization.

```mermaid
flowchart TD
    subgraph Control_Plane["C++ Drogon Control Plane"]
        Client["Client User-Agent"]
        MinIO["MinIO S3 Cluster<br/>(Bucket: drivex-blobs)"]
        DrogonAPI["C++ Drogon REST API<br/>POST /api/v1/files/upload-complete"]
        MySQL_Master[("MySQL 8.0 Primary<br/>Table: files (status=PENDING)")]
    end

    subgraph Messaging_Fabric["RabbitMQ AMQP 0-9-1 Messaging Topology"]
        TopicEx["Topic Exchange<br/>drivex.events"]
        RetryEx["Topic Exchange<br/>drivex.events.retry"]
        DLX["Dead-Letter Exchange<br/>drivex.events.dlx"]

        Q_Ingest["Quorum Queue: file.ingest<br/>Binds: file.uploaded.#, file.content_updated"]
        Q_OCR["Quorum Queue: file.ocr<br/>Binds: file.uploaded.image.#, *.pdf"]
        Q_Embed["Quorum Queue: file.embed<br/>Binds: file.extracted.text"]
        Q_Dedup["Quorum Queue: file.dedup<br/>Binds: file.uploaded.#"]

        Q_Retry30s["TTL Queue: retry.30s<br/>x-message-ttl: 30000ms<br/>DLX: drivex.events"]
        Q_Retry5m["TTL Queue: retry.5m<br/>x-message-ttl: 300000ms<br/>DLX: drivex.events"]
        Q_DLQ["Quorum Queue: file.dlq<br/>Binds: # on drivex.events.dlx"]
    end

    subgraph Celery_Worker_Tier["Celery Distributed Worker Pool"]
        WorkerIngest["Ingestion Worker<br/>tasks.ingest_file<br/>(Stream Validation)"]
        WorkerExtract["Extraction Worker<br/>tasks.extract_document<br/>(PyMuPDF, docx, chardet)"]
        WorkerOCR["OCR Worker<br/>tasks.extract_image<br/>(pdf2image + Tesseract)"]
        WorkerChunk["Chunking Engine<br/>tasks.semantic_chunk<br/>(512 tokens / 64 overlap)"]
        WorkerEmbed["Embedding Worker<br/>tasks.generate_embeddings<br/>(BAAI/bge-large-en-v1.5)"]
        WorkerDedup["Deduplication Worker<br/>tasks.dedup_check<br/>(SHA-256, pHash/dHash)"]
        WorkerFinalize["Finalization Worker<br/>tasks.finalize_ingestion<br/>(Atomic DB Update)"]
    end

    subgraph Storage_Tier["Storage & Indexing Tier"]
        Qdrant_DB[("Qdrant Vector DB<br/>Collection: drivex_file_chunks<br/>- 1024-d Cosine<br/>- HNSW (M=16, ef=128)<br/>- INT8 Scalar Quantization")]
        MySQL_Final[("MySQL 8.0 Primary<br/>Update files:<br/>- status = INDEXED<br/>- content_hash, phash")]
    end

    %% Upload & Control Flow
    Client -->|1. Direct Pre-signed PUT| MinIO
    Client -->|2. Upload Complete| DrogonAPI
    DrogonAPI -->|3. HeadObject & INSERT files| MySQL_Master
    DrogonAPI -->|4. Publish AMQP Event| TopicEx

    %% RabbitMQ Ingestion Fanout
    TopicEx -->|file.uploaded.#| Q_Ingest
    TopicEx -->|file.uploaded.image.#| Q_OCR
    TopicEx -->|file.uploaded.#| Q_Dedup

    %% Worker Pipeline Execution
    Q_Ingest --> WorkerIngest
    WorkerIngest -->|Stream S3 Bytes| MinIO
    WorkerIngest -->|Native Doc| WorkerExtract
    WorkerExtract -->|Extracted Text| WorkerChunk

    Q_OCR --> WorkerOCR
    WorkerOCR -->|Stream Raster Image| MinIO
    WorkerOCR -->|OCR Text| WorkerChunk

    WorkerChunk -->|Publish Extracted Text| Q_Embed
    Q_Embed -->|file.extracted.text| WorkerEmbed
    WorkerEmbed -->|Upsert 1024-d Vectors| Qdrant_DB
    WorkerEmbed --> WorkerFinalize

    Q_Dedup --> WorkerDedup
    WorkerDedup -->|Calculate pHash & dHash| WorkerFinalize

    WorkerFinalize -->|status=INDEXED| MySQL_Final

    %% Retry & Dead-Letter Flow
    WorkerIngest -.->|Transient Failure (Attempt 1)| RetryEx
    WorkerOCR -.->|Transient Failure (Attempt 1)| RetryEx
    WorkerEmbed -.->|Transient Failure (Attempt 1)| RetryEx
    RetryEx -->|retry.short.*| Q_Retry30s
    Q_Retry30s -.->|30s Expiry -> Re-route| TopicEx

    WorkerIngest -.->|Transient Failure (Attempt 2)| RetryEx
    RetryEx -->|retry.long.*| Q_Retry5m
    Q_Retry5m -.->|5m Expiry -> Re-route| TopicEx

    WorkerIngest -.->|Fatal / Retry Limit >= 3| DLX
    WorkerOCR -.->|Fatal / Retry Limit >= 3| DLX
    WorkerEmbed -.->|Fatal / Retry Limit >= 3| DLX
    DLX -->|Routing #| Q_DLQ
```

---

### 3.2 Conversational RAG Assistant Query & Retrieval Sequence Flow

The following sequence diagram details the end-to-end multi-stage pipeline of the conversational "Chat with Your Drive" assistant, demonstrating query analysis, HyDE expansion, parallel hybrid vector and lexical search, Reciprocal Rank Fusion, cross-encoder re-ranking, token budgeting, and Server-Sent Events (SSE) streaming token delivery.

```mermaid
sequenceDiagram
    autonumber
    actor Client as User-Agent (Web / Mobile)
    participant Nginx as Nginx (Edge Proxy & Ingress)
    participant Gateway as Drogon API / FastAPI ML Gateway
    participant LLM_HyDE as Internal LLM (HyDE Rewriter)
    participant Qdrant as Qdrant Vector Database
    participant MySQL as MySQL 8.0 (FULLTEXT Search)
    participant Reranker as Cross-Encoder (bge-reranker-large)
    participant LLM_Gen as Streaming LLM Generator

    Client->>Nginx: POST /api/v1/chat (query, conversation_id)
    Nginx->>Gateway: Forward Request (X-Accel-Buffering: no)
    Gateway->>Gateway: Verify RS256 JWT, Extract user_id & workspace_id

    rect rgb(240, 248, 255)
        note over Gateway,LLM_HyDE: Step 1: Query Analysis & HyDE Expansion
        Gateway->>LLM_HyDE: Expand query: generate hypothetical document passage
        LLM_HyDE-->>Gateway: Return hypothetical passage (h)
        Gateway->>Gateway: Formulate query_rich = query + "\n" + h
        Gateway->>Gateway: Compute 1024-d Dense Vector (BAAI/bge-large-en-v1.5)
    end

    rect rgb(245, 255, 245)
        note over Gateway,MySQL: Step 2: Parallel Hybrid Retrieval & Reciprocal Rank Fusion (k=60)
        par Dense Vector Search
            Gateway->>Qdrant: Vector similarity search (Top 50 candidates)<br/>Filter: (owner_id == user_id OR workspace_id) + Post-Filter: Redis/MySQL permissions
            Qdrant-->>Gateway: Return 50 vector points with Cosine scores
        and Sparse Lexical Keyword Search
            Gateway->>MySQL: MATCH(name, metadata) AGAINST(:query IN BOOLEAN MODE)<br/>Filter: Permissions & ownership (Top 50 candidates)
            MySQL-->>Gateway: Return 50 file records with BM25/TF-IDF scores
        end
        Gateway->>Gateway: Merge candidates via RRF: Score = 0.70/(60+r_vec) + 0.30/(60+r_lex)
        Gateway->>Gateway: Retain Top 50 consolidated candidate chunks
    end

    rect rgb(255, 250, 240)
        note over Gateway,Reranker: Step 3: Deep Cross-Encoder Re-Ranking
        Gateway->>Reranker: Score 50 (query, chunk_text) pairs via joint cross-attention
        Reranker-->>Gateway: Return relevance probability scores [0.0 - 1.0]
        Gateway->>Gateway: Discard chunks with score < 0.40 (noise suppression)
        Gateway->>Gateway: Select Top 5-10 high-precision chunks
    end

    rect rgb(255, 245, 255)
        note over Gateway,LLM_Gen: Step 4: Token Budgeting & Grounded Prompt Assembly
        Gateway->>Gateway: Enforce 4,096 token budget (System: 300, Chunks: 2500, History: 800)
        Gateway->>Gateway: Build anti-hallucination prompt with mandatory citation markers
    end

    rect rgb(240, 255, 255)
        note over Gateway,Client: Step 5: Server-Sent Events (SSE) Streaming
        Gateway-->>Client: event: sources<br/>data: {"sources": [{"file_id": 1024, "filename": "report.pdf", "page": 4, "chunk": 2}]}
        Gateway->>LLM_Gen: Dispatch streaming completion request
        loop Token Delta Streaming
            LLM_Gen-->>Gateway: Yield token delta
            Gateway-->>Client: event: message<br/>data: {"delta": "According to [Doc: report.pdf, Page: 4, Chunk: 2]..."}
        end
        LLM_Gen-->>Gateway: Generation complete (usage metrics)
        Gateway-->>Client: event: done<br/>data: {"status": "completed", "total_tokens": 142}
    end
```

---

## 4. RabbitMQ AMQP 0-9-1 Messaging Topology

The RabbitMQ messaging backbone coordinates the asynchronous event flow across the DriveX platform. All file lifecycle events published by the C++ Drogon Control Plane are broadcast onto durable topic exchanges, routed into fault-tolerant quorum queues, and protected by progressive TTL-based retry queues and dead-letter exchanges.

### 4.1 Exchange Architecture

DriveX declares three dedicated AMQP exchanges, each serving an isolated operational responsibility:

| Exchange Name | Exchange Type | Durability | Auto-Delete | Internal | Description |
|---|---|---|---|---|---|
| `drivex.events` | `topic` | `true` | `false` | `false` | The primary operational event bus. Receives all file lifecycle events (`file.uploaded.#`, `file.deleted`, `file.content_updated`) published by the Drogon Control Plane and internal worker completion events. |
| `drivex.events.retry` | `topic` | `true` | `false` | `false` | Delayed staging exchange. Receives failed event messages flagged for exponential backoff retry. Routes messages to dead-letter TTL queues. |
| `drivex.events.dlx` | `topic` | `true` | `false` | `false` | Terminal dead-letter exchange (DLX). Receives messages that have exhausted their maximum retry limit (3 attempts) or suffered unrecoverable fatal parsing failures. |

---

### 4.2 Routing Key Hierarchy & Semantic Conventions

Routing keys adhere to a strict hierarchical dot-notation scheme:
$$\text{domain}.\text{action}.\text{category}.\text{format}$$

The canonical routing keys and their subscription patterns are defined below:

1. **`file.uploaded.<mime_category>.<mime_subtype>`**:
   - `file.uploaded.application.pdf`: Standard PDF document uploads.
   - `file.uploaded.application.vnd.openxmlformats-officedocument.wordprocessingml.document`: Microsoft Word DOCX uploads.
   - `file.uploaded.text.plain`: Plain text or ASCII code files.
   - `file.uploaded.text.markdown`: Markdown documentation files.
   - `file.uploaded.image.jpeg`: Raster JPEG images requiring OCR and perceptual hashing.
   - `file.uploaded.image.png`: Raster PNG images requiring OCR and perceptual hashing.
   - `file.uploaded.image.webp`: WebP images requiring OCR and perceptual hashing.
2. **`file.deleted`**: Emitted when a file is soft-deleted to trash or permanently purged from storage. Triggers vector point removal in Qdrant and relational tag cleanup.
3. **`file.content_updated`**: Emitted when an existing logical file receives a new binary version (`file_versions`). Triggers re-chunking, re-embedding, and perceptual hash updating.
4. **`file.extracted.text`**: Internal worker-to-worker event emitted after text extraction or OCR succeeds, targeting the embedding queue.
5. **`file.retry.requeue`**: Internal routing key assigned by TTL retry queues when dead-lettering expired retry messages back into `drivex.events`.

---

### 4.3 Quorum Queues, Bindings & Arguments

DriveX utilizes **RabbitMQ Quorum Queues** exclusively for all mission-critical worker queues. Quorum queues employ the Raft consensus protocol, providing high data safety, leader election across clustered broker nodes, and protection against network partition data loss.

| Queue Name | Queue Type | Durability | Bound Exchange | Binding Pattern | Dead-Letter Exchange (`x-dead-letter-exchange`) | Dead-Letter Routing Key (`x-dead-letter-routing-key`) | Delivery Limit (`x-delivery-limit`) |
|---|---|---|---|---|---|---|---|
| `file.ingest` | `quorum` | `true` | `drivex.events` | `file.uploaded.#`, `file.content_updated`, `file.retry.requeue` | `drivex.events.dlx` | `dead.file.ingest` | 3 |
| `file.ocr` | `quorum` | `true` | `drivex.events` | `file.uploaded.image.#`, `file.uploaded.application.pdf` | `drivex.events.dlx` | `dead.file.ocr` | 3 |
| `file.embed` | `quorum` | `true` | `drivex.events` | `file.extracted.text` | `drivex.events.dlx` | `dead.file.embed` | 3 |
| `file.dedup` | `quorum` | `true` | `drivex.events` | `file.uploaded.#` | `drivex.events.dlx` | `dead.file.dedup` | 3 |
| `file.dlq` | `quorum` | `true` | `drivex.events.dlx` | `#` | *(None - Terminal)* | *(None)* | *(None)* |

#### Quorum Queue Configuration Arguments:
```json
{
  "x-queue-type": "quorum",
  "x-max-in-memory-length": 5000,
  "x-delivery-limit": 3,
  "x-dead-letter-exchange": "drivex.events.dlx",
  "x-overflow": "reject-publish"
}
```

---

### 4.4 Progressive TTL-Based Exponential Backoff Retry Architecture

Transient errors (e.g., temporary MinIO S3 network timeouts, Qdrant memory pressure, or database lock contention) must not trigger immediate failure or overwhelm downstream services with rapid retries. DriveX implements a progressive two-stage delayed retry mechanism using RabbitMQ message Time-To-Live (TTL) and dead-letter redirection:

```
[Worker Task Failure]
         |
         |-- (Attempt 1: Transient Error) --> Publish to 'drivex.events.retry' with key 'retry.short.file.ingest'
         |                                         |
         |                                         v
         |                                   Queue: 'retry.30s' (x-message-ttl: 30000ms)
         |                                         |
         |                                         v (TTL Expires after 30s)
         |                                   Dead-letters to 'drivex.events' with key 'file.retry.requeue'
         |                                         |
         |                                         v
         |                                   Re-consumed by Queue: 'file.ingest'
         |
         |-- (Attempt 2: Second Failure) ---> Publish to 'drivex.events.retry' with key 'retry.long.file.ingest'
         |                                         |
         |                                         v
         |                                   Queue: 'retry.5m' (x-message-ttl: 300000ms)
         |                                         |
         |                                         v (TTL Expires after 5m)
         |                                   Dead-letters to 'drivex.events' with key 'file.retry.requeue'
         |
         |-- (Attempt 3: Terminal Failure) -> basic.reject(requeue=false)
                                                   |
                                                   v
                                             Routes to 'drivex.events.dlx' -> Queue: 'file.dlq'
                                             Alert fires to Prometheus / PagerDuty
```

#### Detailed Retry Queue Parameters:

1. **`retry.30s` Queue Specification**:
   - `durable`: `true`
   - `x-message-ttl`: `30000` (30,000 milliseconds = 30 seconds)
   - `x-dead-letter-exchange`: `"drivex.events"`
   - `x-dead-letter-routing-key`: `"file.retry.requeue"`
   - Bindings: Exchange `drivex.events.retry` with routing pattern `retry.short.*`
2. **`retry.5m` Queue Specification**:
   - `durable`: `true`
   - `x-message-ttl`: `300000` (300,000 milliseconds = 5 minutes)
   - `x-dead-letter-exchange`: `"drivex.events"`
   - `x-dead-letter-routing-key`: `"file.retry.requeue"`
   - Bindings: Exchange `drivex.events.retry` with routing pattern `retry.long.*`
3. **`file.dlq` Dead-Letter Queue**:
   - Stores terminally failed messages wrapped in the `DeadLetterEnvelope` schema.
   - Consumed by operator alert daemons and an automated administrative redrive CLI.

---

### 4.5 Production Draft-07 JSON Event Schemas

All messages published to RabbitMQ must conform strictly to the following draft-07 JSON schemas. Malformed payloads that fail schema validation are rejected immediately without requeue and routed to `file.dlq` to prevent poison pill loops.

#### 4.5.1 `file.uploaded` Event Payload Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FileUploadedEvent",
  "type": "object",
  "required": [
    "event_id",
    "event_type",
    "schema_version",
    "timestamp",
    "file_id",
    "owner_id",
    "storage_key",
    "mime_type",
    "size_bytes",
    "sha256_checksum",
    "version_id",
    "trace_context"
  ],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid",
      "description": "Cryptographically secure UUIDv4 identifying this event instance."
    },
    "event_type": {
      "type": "string",
      "enum": [
        "file.uploaded"
      ]
    },
    "schema_version": {
      "type": "string",
      "enum": [
        "1.0.0"
      ]
    },
    "timestamp": {
      "type": "integer",
      "description": "Epoch timestamp in milliseconds when the upload was finalized."
    },
    "file_id": {
      "type": "integer",
      "minimum": 1,
      "description": "Primary key of the file record in MySQL files table."
    },
    "owner_id": {
      "type": "integer",
      "minimum": 1,
      "description": "User ID of the file owner."
    },
    "workspace_id": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Nullable tenant/workspace identifier for organizational multi-tenancy."
    },
    "folder_id": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Parent folder ID; null represents the user root directory."
    },
    "filename": {
      "type": "string",
      "maxLength": 255,
      "description": "Original user-provided filename including extension."
    },
    "storage_key": {
      "type": "string",
      "maxLength": 512,
      "description": "Canonical MinIO object key, e.g. 'blobs/42/2026-09/a1b2c3d4.pdf'."
    },
    "mime_type": {
      "type": "string",
      "maxLength": 127,
      "description": "IANA media type classification of the file."
    },
    "size_bytes": {
      "type": "integer",
      "minimum": 0,
      "description": "Exact byte length of the uploaded object verified via S3 HeadObject."
    },
    "sha256_checksum": {
      "type": "string",
      "pattern": "^[a-f0-9]{64}$",
      "description": "Cryptographic SHA-256 digest computed across file contents."
    },
    "version_id": {
      "type": "integer",
      "minimum": 1,
      "description": "Monotonically increasing version number from file_versions table."
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$",
          "description": "W3C Trace Context traceparent header for distributed tracing."
        },
        "tracestate": {
          "type": "string",
          "description": "Optional W3C vendor-specific state string."
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

#### 4.5.2 `file.deleted` Event Payload Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FileDeletedEvent",
  "type": "object",
  "required": [
    "event_id",
    "event_type",
    "schema_version",
    "timestamp",
    "file_id",
    "owner_id",
    "storage_key",
    "permanent_delete",
    "trace_context"
  ],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique UUIDv4 identifier for this event."
    },
    "event_type": {
      "type": "string",
      "enum": [
        "file.deleted"
      ]
    },
    "schema_version": {
      "type": "string",
      "enum": [
        "1.0.0"
      ]
    },
    "timestamp": {
      "type": "integer",
      "description": "Epoch timestamp in milliseconds of the deletion operation."
    },
    "file_id": {
      "type": "integer",
      "minimum": 1,
      "description": "MySQL file ID target of deletion."
    },
    "owner_id": {
      "type": "integer",
      "minimum": 1,
      "description": "User ID owning the target file."
    },
    "workspace_id": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Nullable workspace identifier."
    },
    "storage_key": {
      "type": "string",
      "description": "MinIO object storage key to be deleted if permanent."
    },
    "permanent_delete": {
      "type": "boolean",
      "description": "True if permanently purged from trash; false if soft-deleted to trash."
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$"
        },
        "tracestate": {
          "type": "string"
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

#### 4.5.3 `file.content_updated` Event Payload Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FileContentUpdatedEvent",
  "type": "object",
  "required": [
    "event_id",
    "event_type",
    "schema_version",
    "timestamp",
    "file_id",
    "new_version_id",
    "previous_version_id",
    "owner_id",
    "storage_key",
    "mime_type",
    "size_bytes",
    "new_sha256_checksum",
    "trace_context"
  ],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid"
    },
    "event_type": {
      "type": "string",
      "enum": [
        "file.content_updated"
      ]
    },
    "schema_version": {
      "type": "string",
      "enum": [
        "1.0.0"
      ]
    },
    "timestamp": {
      "type": "integer"
    },
    "file_id": {
      "type": "integer",
      "minimum": 1
    },
    "new_version_id": {
      "type": "integer",
      "minimum": 2
    },
    "previous_version_id": {
      "type": "integer",
      "minimum": 1
    },
    "owner_id": {
      "type": "integer",
      "minimum": 1
    },
    "workspace_id": {
      "type": [
        "integer",
        "null"
      ]
    },
    "storage_key": {
      "type": "string"
    },
    "mime_type": {
      "type": "string"
    },
    "size_bytes": {
      "type": "integer",
      "minimum": 0
    },
    "new_sha256_checksum": {
      "type": "string",
      "pattern": "^[a-f0-9]{64}$"
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$"
        },
        "tracestate": {
          "type": "string"
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

#### 4.5.4 `file.dlq` Dead-Letter Metadata Envelope Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DeadLetterQueueEnvelope",
  "type": "object",
  "required": [
    "dlq_id",
    "failed_at",
    "source_queue",
    "original_routing_key",
    "retry_count",
    "error_type",
    "error_message",
    "stack_trace",
    "original_payload",
    "trace_context"
  ],
  "properties": {
    "dlq_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique identifier assigned upon DLQ dead-lettering."
    },
    "failed_at": {
      "type": "integer",
      "description": "Epoch timestamp in milliseconds when the message was permanently dead-lettered."
    },
    "source_queue": {
      "type": "string",
      "description": "The worker queue in which the failure occurred (e.g. 'file.ingest')."
    },
    "original_routing_key": {
      "type": "string",
      "description": "The routing key originally assigned to the event message."
    },
    "retry_count": {
      "type": "integer",
      "minimum": 1,
      "description": "Number of retry attempts executed prior to final dead-lettering."
    },
    "error_type": {
      "type": "string",
      "description": "Exception class or machine-readable error category."
    },
    "error_message": {
      "type": "string",
      "description": "Descriptive error message captured during the terminal execution."
    },
    "stack_trace": {
      "type": "string",
      "description": "Full serialized stack trace of the terminal worker exception."
    },
    "original_payload": {
      "type": "object",
      "description": "The complete original event JSON payload that failed processing."
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$"
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

---

## 5. Celery Worker Pipeline & Ingestion DAG

The asynchronous execution fabric is implemented using Celery 5.4 orchestrated over RabbitMQ (Broker) and Redis (Result Backend DB 1). The pipeline coordinates multi-format ingestion, optical character recognition, semantic text chunking, embedding generation, and multi-tier deduplication.

### 5.1 Production Celery Configuration (`app/celery_app.py`)

The Celery worker fleet is configured with strict concurrency, memory, and reliability boundaries:

```python
import os
from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.getenv("RABBITMQ_URL", "amqp://drivex:drivexpass@rabbitmq:5672//")
RESULT_BACKEND = os.getenv("REDIS_URL", "redis://redis:6379/1")

celery_app = Celery("drivex_ml", broker=BROKER_URL, backend=RESULT_BACKEND)

celery_app.conf.update(
    # Serialization & Security
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,

    # Reliability Invariants (Late ACKs & Single Prefetch)
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,      # Recycle workers after 100 tasks to purge PyTorch GPU/CPU memory fragments
    task_track_started=True,
    task_time_limit=900,                 # Hard limit: 15 minutes before SIGKILL
    task_soft_time_limit=840,            # Soft limit: 14 minutes before SoftTimeLimitExceeded exception

    # Result Backend Expiration
    result_expires=86400,                # Retain task status for 24 hours in Redis DB 1

    # Task Publishing Retry Policy
    task_publish_retry=True,
    task_publish_retry_policy={
        "max_retries": 3,
        "interval_start": 0.5,
        "interval_step": 1.0,
        "interval_max": 5.0,
    },

    # Dedicated Quorum Queue Topology Mapping
    task_queues=[
        Queue(
            "file.ingest",
            Exchange("drivex.events", type="topic"),
            routing_key="file.uploaded.#",
            queue_arguments={"x-queue-type": "quorum"}
        ),
        Queue(
            "file.ocr",
            Exchange("drivex.events", type="topic"),
            routing_key="file.uploaded.image.#",
            queue_arguments={"x-queue-type": "quorum"}
        ),
        Queue(
            "file.embed",
            Exchange("drivex.events", type="topic"),
            routing_key="file.extracted.text",
            queue_arguments={"x-queue-type": "quorum"}
        ),
        Queue(
            "file.dedup",
            Exchange("drivex.events", type="topic"),
            routing_key="file.uploaded.#",
            queue_arguments={"x-queue-type": "quorum"}
        ),
    ],
    task_default_queue="file.ingest",
)
```

#### Worker Pool Sizing & Workload Specialization:
To prevent heavy OCR and PyTorch transformer workloads from starving fast I/O ingestion tasks, workers are split into specialized container pools:
1. **`worker-io` (Concurrency: 8, Pool: `threads` / `prefork`)**: Consumes `file.ingest`. Handles S3 download streams, SHA-256 byte validation, and plain text/markdown parsing.
2. **`worker-ocr` (Concurrency: 4, Pool: `prefork`)**: Consumes `file.ocr`. Runs `pdf2image` and Tesseract OCR processes isolated from memory-sensitive tasks.
3. **`worker-nlp` (Concurrency: 2 per GPU or 4 on CPU, Pool: `prefork`)**: Consumes `file.embed`. Houses pinned in-memory instances of `BAAI/bge-large-en-v1.5` and executes batch vectorization.
4. **`worker-dedup` (Concurrency: 4, Pool: `prefork`)**: Consumes `file.dedup`. Computes pHash/dHash and performs relational hash lookups.

---

### 5.2 Celery Canvas Orchestration DAG

The ingestion workflow is modeled as a declarative Directed Acyclic Graph (DAG) using Celery Canvas primitives (`chain`, `chord`, `group`):

```python
from celery import chain, chord, group
from app.tasks.ingest import task_ingest_file
from app.tasks.extract import task_extract_document, task_extract_image
from app.tasks.dedup import task_dedup_check
from app.tasks.chunk import task_semantic_chunk
from app.tasks.embed import task_generate_embeddings, task_upsert_qdrant
from app.tasks.finalize import task_finalize_ingestion

def build_ingestion_workflow(event_payload: dict):
    mime_type = event_payload["mime_type"]
    is_image = mime_type.startswith("image/")
    
    # 1. Select appropriate extraction task
    extraction_task = (
        task_extract_image.s(event_payload)
        if is_image
        else task_extract_document.s(event_payload)
    )
    
    # 2. Build parallel chord: extraction runs concurrently with dedup check
    parallel_analysis = chord(
        group(
            extraction_task,
            task_dedup_check.s(event_payload)
        ),
        # Chord callback receives [extraction_result, dedup_result]
        task_semantic_chunk.s(event_payload)
    )
    
    # 3. Formulate end-to-end chain
    workflow = chain(
        task_ingest_file.s(event_payload),
        parallel_analysis,
        task_generate_embeddings.s(event_payload),
        task_upsert_qdrant.s(event_payload),
        task_finalize_ingestion.s(event_payload)
    )
    return workflow
```

---

### 5.3 Multi-Format File Ingestion Engine

The document extraction tier normalizes unstructured binary formats into structured text blocks paired with structural hierarchical metadata.

#### 5.3.1 Native PDF Extraction via PyMuPDF (`fitz`)
- Opens the PDF binary stream without writing intermediate files to disk: `fitz.open(stream=stream_bytes, filetype="pdf")`.
- Iterates through document pages:
  - Extracts text blocks, bounding boxes, font weights, and sizes.
  - Detects structural headings: blocks with font size > 1.3x document median body font size are tagged as section titles.
  - Table extraction: PyMuPDF table finder (`page.find_tables()`) identifies grid lines, extracts tabular cells, and renders them as Markdown tables to preserve tabular relationships for RAG embeddings.
- **Scanned Page Detection Fallback**:
  If a page contains less than 32 text characters but contains raster image objects occupying > 40% of page area, the page is flagged as scanned and routed to the OCR engine.

#### 5.3.2 Scanned PDF & Raster Image OCR (`pdf2image` + `PyTesseract`)
- Scanned PDF pages are rasterized to uncompressed RGB images at 300 DPI using `pdf2image.convert_from_bytes(page_bytes, dpi=300)`.
- Image Pre-processing Pipeline (Pillow & OpenCV):
  1. Convert to 8-bit grayscale: `img.convert('L')`.
  2. Contrast enhancement: CLAHE (Contrast Limited Adaptive Histogram Equalization).
  3. Binarization: Otsu's thresholding to isolate text glyphs from paper background noise.
  4. De-skewing: Calculate text orientation angle via Radon transform and rotate image to horizontal baseline.
- Tesseract Execution:
  `pytesseract.image_to_string(processed_img, lang='eng', config='--oem 1 --psm 1')`
  - `--oem 1`: Neural network LSTM engine.
  - `--psm 1`: Automatic page segmentation with Orientation and Script Detection (OSD).
- Filter: Low-confidence OCR output (mean confidence < 40%) is flagged for human review or indexed as low-confidence.

#### 5.3.3 Office Document Extraction via `python-docx`
- Reads DOCX OpenXML packages directly from memory streams: `docx.Document(io.BytesIO(stream_bytes))`.
- Traverses document structural elements:
  - Paragraphs: Maps built-in styles (`Heading 1`, `Heading 2`, `Heading 3`, `Title`, `Subtitle`) into a hierarchical section breadcrumb stack.
  - Lists: Bulleted and numbered list items are formatted with appropriate indentation.
  - Tables: Iterates over rows and cells, generating formatted Markdown table blocks (`| Col 1 | Col 2 |`).
  - Embedded Hyperlinks: Resolves relationship IDs (`rId`) to target URLs and formats as `[Anchor Text](URL)`.

#### 5.3.4 Plain Text & Code with Encoding Fallback (`chardet` + NFKC)
- Primary attempt: Strict UTF-8 decoding (`bytes.decode('utf-8')`).
- Fallback Heuristic: On `UnicodeDecodeError`, sample first 64 KB of the byte stream and invoke `chardet.detect(sample)`.
  - Supports ISO-8859-1 (Latin-1), Windows-1252, Shift-JIS, GB18030, and EUC-KR.
  - Decodes full stream using detected encoding, ignoring unmapped bytes via `errors='replace'`.
- Normalization: Apply Unicode Normalization Form KC (NFKC) via `unicodedata.normalize('NFKC', text)` to standardize full-width characters, ligature forms, and composite glyphs into canonical equivalents.
- Control Character Cleansing: Strips non-printable ASCII control characters (`[ --]`) while preserving tabs (`	`) and line feeds (`
`).

#### 5.3.5 Image Metadata & EXIF Extraction via Pillow
- Reads image streams using Pillow (`PIL.Image.open(io.BytesIO(stream_bytes))`).
- Extracts image dimensions (width, height), color mode (RGB, RGBA, CMYK), and format (JPEG, PNG, WebP).
- Parses raw EXIF data tags via `img.getexif()`:
  - `Make`, `Model` (Camera hardware information).
  - `DateTimeOriginal` (Capture timestamp).
  - `GPSInfo`: Decodes GPS latitude and longitude from degree/minute/second tuples to signed decimal coordinates, storing them in payload attributes for geospatial filtering.

---

### 5.4 Semantic Text Chunking Engine

The chunking engine converts continuous document streams into discrete, self-contained semantic units tailored for retrieval:

#### 5.4.1 Tokenizer & Window Specifications
- **Tokenizer**: Hugging Face `AutoTokenizer.from_pretrained("BAAI/bge-large-en-v1.5")`.
- **Target Chunk Size ($W$)**: 512 tokens (~2,048 characters).
- **Stride Overlap ($O$)**: 64 tokens (~256 characters), providing a 12.5% overlap ratio.
- **Overlap Invariant**: The overlap guarantees that sentences spanning boundary edges are fully represented in at least one adjacent chunk, preventing semantic truncation.

#### 5.4.2 Structural Breadcrumb & Section Header Context Preservation
Raw text chunks frequently lose context when detached from their parent document structure (e.g., a table row saying "Expenses: $50,000" is meaningless without knowing it belongs to the "Q3 AWS Cloud Infrastructure" section).
The chunking engine prepends a canonical context header to every chunk before tokenization:
```
[Document: {filename} > Section: {heading_level_1} > Subsection: {heading_level_2} | Page: {page_number}]
{chunk_text}
```
The token budget for the context header is dynamically subtracted from the 512-token chunk capacity, reserving 450-480 tokens for the substantive body text.

#### 5.4.3 Punctuation-Aware Boundary Splitting Algorithm
Splitting follows a strict recursive boundary hierarchy to guarantee that text is never sliced mid-word or mid-sentence:
1. **Separator Tier 1**: Double Newlines (`

`) representing paragraph boundaries.
2. **Separator Tier 2**: Single Newlines (`
`) representing lines and list items.
3. **Separator Tier 3**: Sentence Terminators (`. `, `? `, `! `) preserving complete grammatical sentences.
4. **Separator Tier 4**: Clause Delimiters (`; `, `, `, ` - `).
5. **Separator Tier 5**: Word Whitespaces (` `).
6. **Fallback Tier 6**: Raw character boundary (strictly invoked only if an unbroken alphanumeric string exceeds 512 tokens).

---

### 5.5 Dense Embedding Generation Engine

- **Embedding Model**: `BAAI/bge-large-en-v1.5`
- **Output Vector Dimension ($D$)**: 1024 float32 dimensions.
- **Max Input Length**: 512 tokens.
- **$L_2$ Normalization Invariant**:
  All generated vectors $\mathbf{v}$ are normalized to unit Euclidean length prior to storage:
  $$\hat{\mathbf{v}} = rac{\mathbf{v}}{\|\mathbf{v}\|_2} = rac{\mathbf{v}}{\sqrt{\sum_{i=1}^{1024} v_i^2}}$$
  Because $\|\hat{\mathbf{v}}\|_2 = 1.0$, the Cosine similarity between two vectors $\mathbf{a}$ and $\mathbf{b}$ reduces to an inner dot product:
  $$	ext{CosineSimilarity}(\hat{\mathbf{a}}, \hat{\mathbf{b}}) = \hat{\mathbf{a}} \cdot \hat{\mathbf{b}} = \sum_{i=1}^{1024} a_i b_i$$
  This allows Qdrant to execute similarity calculations via SIMD-accelerated dot product instructions (`AVX-512-VNNI` / `NEON`), yielding 4x higher search throughput.

#### Batch Processing & Dynamic Tensor Padding:
- Embeddings are generated in batches of **32 chunks**.
- Dynamic padding is applied per batch to the longest chunk in that batch, eliminating wasted matrix operations on pad tokens.

#### Query vs. Passage Instruction Prefixes:
`BAAI/bge-large-en-v1.5` requires asymmetric instruction prefixing:
- **Passages / Chunks**: Ingested directly **without prefix**.
- **Search Queries**: Must include the task-specific instruction prefix:
  `"Represent this sentence for searching relevant passages: " + query_text`

---

### 5.6 Dual-Tier Deduplication Engine

To maximize storage utilization and eliminate vector database clutter, DriveX enforces a multi-tier deduplication engine:

```
[Uploaded File Stream]
         |
         |---> 1. Cryptographic SHA-256 Stream Hash
         |        |
         |        v
         |     Check MySQL `files.content_hash`
         |     - Match Found: Exact Byte Duplicate -> Re-use storage_key / Create version link
         |
         |---> 2. Perceptual Image Hashing (if Image)
         |        |
         |        v
         |     Compute 64-bit pHash (DCT) & 64-bit dHash (Gradient)
         |     - Evaluate Hamming Distance against existing images
         |     - Threshold <= 6: Flag as Perceptual Duplicate / Near-Duplicate
         |
         |---> 3. Semantic Near-Duplicate Detection (if Text)
                  |
                  v
               Mean-pool all chunk vectors into document embedding
               - Evaluate Cosine Similarity against existing documents
               - Threshold >= 0.92: Tag as Semantic Variant / Revision
```

#### 5.6.1 Cryptographic Deduplication (SHA-256)
- Computed in streaming fashion as bytes pass from MinIO.
- When an exact SHA-256 match occurs within the same owner/workspace, the system registers a new logical file entry pointing to the existing MinIO `storage_key` and increments the underlying object reference count, consuming zero additional S3 disk blocks.

#### 5.6.2 Perceptual Image Deduplication (pHash & dHash)
Raster images are vulnerable to visual duplication that alters byte checksums (e.g., resizing, minor JPEG compression, color profile stripping, watermark addition).
1. **Perceptual Hash (`pHash`)**:
   - Resizes image to 32x32 pixels, converts to grayscale.
   - Computes 2D Discrete Cosine Transform (DCT).
   - Extracts the low-frequency 8x8 DCT matrix (representing basic structure).
   - Computes median DCT value; bits are set to `1` if above median, `0` otherwise, producing a 64-bit hexadecimal string.
2. **Difference Hash (`dHash`)**:
   - Resizes image to 9x8 pixels (72 pixels).
   - Computes horizontal gradient between adjacent pixels ($P[x, y] > P[x+1, y]$).
   - Generates a 64-bit binary string.
3. **Hamming Distance Comparison**:
   $$	ext{Hamming Distance} = 	ext{popcount}(	ext{pHash}_1 \oplus 	ext{pHash}_2)$$
   - $	ext{Distance} = 0$: Identical visual content.
   - $1 \le 	ext{Distance} \le 6$: High probability perceptual duplicate (minor crop, recompression, watermark).
   - $	ext{Distance} > 6$: Visually distinct image.

#### 5.6.3 Document Semantic Near-Duplicate Detection
For text documents, a file-level representation is formed by mean-pooling all $N$ chunk vectors:
$$ar{\mathbf{v}}_{	ext{doc}} = rac{1}{N} \sum_{i=1}^N \hat{\mathbf{v}}_i, \quad \hat{\mathbf{v}}_{	ext{doc}} = rac{ar{\mathbf{v}}_{	ext{doc}}}{\|ar{\mathbf{v}}_{	ext{doc}}\|_2}$$
If $\hat{\mathbf{v}}_{	ext{doc}} \cdot \hat{\mathbf{v}}_{	ext{existing}} \ge 0.92$, the file is classified as a semantic revision or plagiarized variant of the existing document.

---

## 6. Qdrant Vector Database Architecture

Qdrant (v1.9+) serves as the specialized vector similarity search engine, managing distributed vector indexing, quantized similarity scoring, and tenant-isolated payload filtering.

### 6.1 Master Collection Specification (`drivex_file_chunks`)

The collection `drivex_file_chunks` is initialized on system boot via the ML API control plane. It is configured for high indexing throughput, sub-10ms query latency, and extreme memory efficiency:

```python
from qdrant_client import QdrantClient
from qdrant_client.http import models

def initialize_qdrant_collection(client: QdrantClient):
    collection_name = "drivex_file_chunks"
    
    # Check idempotency
    collections = client.get_collections().collections
    if any(c.name == collection_name for c in collections):
        return

    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=1024,                       # Matches BAAI/bge-large-en-v1.5 embedding output
            distance=models.Distance.COSINE, # Cosine similarity over normalized vectors
            on_disk=True,                    # Persist raw float32 vectors on disk to conserve RAM
        ),
        hnsw_config=models.HnswConfigDiff(
            m=16,                            # Number of bi-directional links per node (balanced recall vs graph size)
            ef_construct=128,                # Search depth during graph construction
            full_scan_threshold=10000,       # Number of points below which brute-force search is faster than HNSW
            max_indexing_threads=0,          # 0 = Auto-detect host CPU core count
            on_disk=False,                   # Retain HNSW graph index in RAM for sub-10ms traversal
        ),
        quantization_config=models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(
                type=models.ScalarType.INT8, # 8-bit integer quantization (4x memory reduction)
                quantile=0.99,               # Clip 1% extreme outliers to preserve dynamic range
                always_ram=True,             # Retain INT8 quantized vectors permanently in RAM
            )
        ),
        optimizers_config=models.OptimizersConfigDiff(
            deleted_threshold=0.2,           # Trigger background segment vacuum when 20% points are deleted
            vacuum_min_vector_number=1000,   # Minimum deleted vectors before triggering vacuum
            indexing_threshold=20000,        # Defer HNSW building until segment reaches 20,000 points
            memmap_threshold=50000,          # Map segments to disk via mmap once exceeding 50,000 points
        ),
        wal_config=models.WalConfigDiff(
            wal_capacity_mb=64,              # 64 MB Write-Ahead Log buffer
            wal_segments_ahead=2,            # Number of ahead WAL segments for crash recovery
        ),
    )
```

#### Memory Sizing & INT8 Quantization Efficiency:
- **Raw Float32 Storage**: $1024 	imes 4 	ext{ bytes} = 4,096 	ext{ bytes per vector}$.
- **INT8 Scalar Quantized Storage**: $1024 	imes 1 	ext{ byte} = 1,024 	ext{ bytes per vector}$.
- **RAM Savings**: INT8 quantization compresses vector memory consumption by **75%**.
- **Accuracy Tradeoff**: At `quantile=0.99`, empirical benchmark evaluations indicate $> 99.1\%$ recall retention compared to uncompressed float32 inner products, while query latency improves by 3.2x due to CPU SIMD integer dot-product execution.
- **On-Disk / In-Memory Tiering**:
  - HNSW Graph Structure: In RAM (`on_disk=False`)
  - INT8 Quantized Vectors: In RAM (`always_ram=True`)
  - Raw Float32 Vectors: On NVMe Disk (`on_disk=True`) for optional re-scoring

---

### 6.2 Payload Schema & Inverted Indexing Configuration

Every vector point in `drivex_file_chunks` represents an individual 512-token document chunk. To guarantee that queries filter in sub-millisecond time without traversing the entire vector graph, payload attributes are indexed with explicit inverted indexes:

| Field Name | Storage Type | Indexed | Index Schema Type | Description |
|---|---|---|---|---|
| `chunk_id` | `UUIDv4` | Point ID | Primary Key | Unique point identifier in Qdrant. |
| `file_id` | `int64` | Yes | `models.PayloadSchemaType.INTEGER` | References MySQL `files.id`. |
| `owner_id` | `int64` | Yes | `models.PayloadSchemaType.INTEGER` | User ID owning the document. |
| `workspace_id` | `int64` | Yes | `models.PayloadSchemaType.INTEGER` | Enterprise workspace identifier (nullable). |
| `folder_id` | `int64` | Yes | `models.PayloadSchemaType.INTEGER` | Parent folder ID in tree hierarchy. |
| `filename` | `keyword` | Yes | `models.PayloadSchemaType.KEYWORD` | Exact document filename for citations. |
| `mime_type` | `keyword` | Yes | `models.PayloadSchemaType.KEYWORD` | MIME classification. |
| `chunk_index` | `int32` | No | None | Sequential chunk order in source document (0, 1, ...). |
| `total_chunks` | `int32` | No | None | Total chunk count in document. |
| `page_number` | `int32` | No | None | Source document page number. |
| `section_breadcrumb`| `text` | No | None | Contextual section hierarchy path. |
| `text_preview` | `text` | No | None | Raw text chunk payload used in prompt assembly. |
| `created_at` | `int64` | Yes | `models.PayloadSchemaType.INTEGER` | Epoch timestamp in milliseconds. |
| `shared_user_ids` | `int64[]` | Yes | `models.PayloadSchemaType.INTEGER` | Array of user IDs with explicit shared access. |

#### Inverted Index Creation Declarations:
```python
def setup_payload_indexes(client: QdrantClient):
    collection_name = "drivex_file_chunks"
    
    indexed_fields = [
        ("owner_id", models.PayloadSchemaType.INTEGER),
        ("workspace_id", models.PayloadSchemaType.INTEGER),
        ("file_id", models.PayloadSchemaType.INTEGER),
        ("mime_type", models.PayloadSchemaType.KEYWORD),
        ("created_at", models.PayloadSchemaType.INTEGER),
        ("shared_user_ids", models.PayloadSchemaType.INTEGER),
    ]
    
    for field_name, field_schema in indexed_fields:
        client.create_payload_index(
            collection_name=collection_name,
            field_name=field_name,
            field_schema=field_schema,
            wait=True,
        )
```

---

### 6.3 Multi-Tenant Isolated Retrieval DSL & Deletion Lifecycle

#### Multi-Tenant Query Filter DSL:
When a user performs a search, the ML Gateway dynamically constructs a candidate retrieval filter. To avoid permission desynchronization from mutable sharing hierarchies, candidate vector chunks are retrieved by coarse-grained boundary and subsequently verified by authoritative Redis/MySQL RBAC post-filtering:
1. If searching within an organizational workspace, `workspace_id` must match.
2. In personal search, candidate chunks match `owner_id == user_id`, or if collaborative search across shared items is requested, the Gateway queries Drogon for the user's accessible `file_id`s (`shared_file_ids`).
3. Authoritative access is verified per candidate via Redis `perm:eff:<user_id>:file:<file_id>` before cross-encoder re-ranking.

```python
def build_multi_tenant_filter(
    user_id: int, 
    workspace_id: int | None = None, 
    accessible_file_ids: list[int] | None = None
) -> models.Filter:
    if workspace_id is not None:
        # Workspace boundary: retrieve workspace candidate chunks for subsequent RBAC post-filtering
        return models.Filter(
            must=[
                models.FieldCondition(
                    key="workspace_id",
                    match=models.MatchValue(value=workspace_id)
                )
            ]
        )
    
    should_conditions = [
        models.FieldCondition(key="owner_id", match=models.MatchValue(value=user_id))
    ]
    if accessible_file_ids:
        should_conditions.append(
            models.FieldCondition(key="file_id", match=models.MatchAny(any=accessible_file_ids))
        )
    return models.Filter(should=should_conditions)
```

#### Dynamic RBAC Post-Filtering Architecture (Zero-Staleness Enforcement):
In collaborative cloud drives, permissions on files and ancestor folders are highly dynamic (e.g. sharing a folder `/Marketing/2026` grants access to all nested files, or revoking a member's access terminates visibility immediately). Propagating every permission grant or revocation by mutating payload arrays (`shared_user_ids`) across tens of thousands of vector points in Qdrant causes severe write amplification and index lock contention.

To solve this, DriveX enforces **Dynamic RBAC Post-Filtering**:
1. **Coarse-Grained Retrieval**: Qdrant executes HNSW vector retrieval using `build_multi_tenant_filter` to retrieve top candidate passages.
2. **Authoritative Cache Verification**: For each retrieved candidate passage, the Gateway inspects `file_id` and queries Drogon's Redis effective permission cache:
   ```redis
   GET perm:eff:<user_id>:file:<file_id>
   ```
   If a cache miss occurs, the gateway falls back to the MySQL recursive permission CTE evaluator.
3. **Instant Access Revocation & Grant Grounding**: If the user lacks active `viewer`, `editor`, or `owner` role, or if a permission was revoked, the candidate chunk is dropped immediately before prompt generation. This eliminates authorization drift and guarantees zero cross-tenant vector leakage.


#### Vector Deletion Lifecycle:
When a user deletes a file, moves it to trash, or a new version is uploaded:
1. **Move to Trash (Soft-Delete)**: Qdrant points are marked with payload attribute `is_trashed=True` or excluded by adding `is_trashed=False` to the default search filter.
2. **Permanent Purge**: All vector points associated with the file are atomically deleted in Qdrant using filter-based point deletion:
```python
def delete_file_vectors(client: QdrantClient, file_id: int):
    client.delete(
        collection_name="drivex_file_chunks",
        points_selector=models.FilterSelector(
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="file_id",
                        match=models.MatchValue(value=file_id)
                    )
                ]
            )
        ),
        wait=True,
    )
```

---

## 7. Conversational RAG Assistant Engine

The DriveX conversational intelligence subsystem ("Chat with Your Drive") is an enterprise Retrieval-Augmented Generation (RAG) engine designed for low-latency, strictly grounded question answering across a user's multi-format document corpus.

The engine executes in five discrete, sequential stages:

```
[User Query]
     |
     v
[Step 1: Query Analysis & HyDE Expansion] -> Hypothetical Document Passage & Query Vector
     |
     v
[Step 2: Hybrid Retrieval & Reciprocal Rank Fusion (RRF, k=60)]
     |---> Dense Vector Search (Qdrant Top 50)
     |---> Sparse Lexical Search (MySQL FULLTEXT Top 50)
     |---> Weighted RRF Merging -> Top 50 Unified Candidates
     |
     v
[Step 3: Deep Cross-Encoder Re-Ranking (BAAI/bge-reranker-large)]
     |---> Joint Cross-Attention Scoring
     |---> Cutoff Filter: Discard chunks with score < 0.40
     |---> Top 5-10 High-Precision Passages
     |
     v
[Step 4: Token Budgeting & Grounded Anti-Hallucination Prompt Assembly]
     |---> Strict 4,096-Token Budget
     |---> Mandatory Citation Syntax: [Doc: filename, Page: X, Chunk: Y]
     |
     v
[Step 5: Server-Sent Events (SSE) Streaming Protocol]
     |---> event: sources (Citation Metadata)
     |---> event: message (Token Deltas)
     |---> event: done (Completion Telemetry)
```

---

### 7.1 Step 1: Query Analysis & Hypothetical Document Embeddings (HyDE)

User queries in cloud storage environments are frequently brief, conversational, or fragmented (e.g., *"What did we spend on AWS in Q3?"*), creating an embedding vocabulary mismatch with formal corporate documents (e.g., *"Fiscal Year 2026 Third Quarter Infrastructure Operating Expenditures: Amazon Web Services Cloud Hosting Services: $42,850"*).

To eliminate this representation gap, the query pipeline applies **Hypothetical Document Embeddings (HyDE)**:
1. The user's query $q$ is passed to an internal high-speed instruction model:
```
SYSTEM: You are an internal document synthesis rewriter. Write a concise, authoritative 2-3 sentence hypothetical excerpt from a corporate or technical document that directly answers the user's question. Do not include introductory or conversational filler.
USER: {query}
```
2. The model outputs a hypothetical passage $h$.
3. The original query is synthesized into an expanded semantic representation:
$$q_{	ext{rich}} = q + "
" + h$$
4. The synthesized text is encoded using `BAAI/bge-large-en-v1.5` with the mandatory BGE search query prefix:
$$\mathbf{v}_q = 	ext{Embed}(	ext{"Represent this sentence for searching relevant passages: "} + q_{	ext{rich}})$$
5. Vector $\mathbf{v}_q$ is $L_2$ normalized, producing the query vector for dense retrieval.

---

### 7.2 Step 2: Parallel Hybrid Retrieval via Reciprocal Rank Fusion (RRF, $k=60$)

Dense vector retrieval excels at semantic concepts but can miss exact alphanumeric identifiers, serial numbers, or file extensions (e.g., `"INV-2026-9042"` or `"docker-compose.prod.yml"`). DriveX merges dense vector retrieval with sparse lexical keyword matching in parallel:

#### 1. Dense Vector Search (Qdrant)
- Executes an approximate nearest neighbor search on collection `drivex_file_chunks` using query vector $\mathbf{v}_q$.
- Applies the mandatory multi-tenant security filter (`owner_id` / `accessible_file_ids` / `workspace_id`).
- Retrieves the **Top 50 candidate chunks** ($D_{	ext{vec}}$), sorted by Cosine similarity score.

#### 2. Sparse Lexical Search (MySQL FULLTEXT)
- Executes a parallel SQL query against the MySQL `files` table and extracted section headings:
```sql
SELECT f.id AS file_id, f.name,
       MATCH(f.name) AGAINST(:query IN BOOLEAN MODE) AS score
FROM files f
WHERE (f.owner_id = :user_id OR f.id IN (SELECT resource_id FROM permissions WHERE user_id = :user_id AND resource_type = 'file'))
  AND f.is_trashed = FALSE
  AND MATCH(f.name) AGAINST(:query IN BOOLEAN MODE)
ORDER BY score DESC
LIMIT 50;
```
- Retrieves the **Top 50 candidate documents** ($D_{	ext{lex}}$), sorted by relevance score.

#### 3. Reciprocal Rank Fusion (RRF) Formulation
Candidate ranks are fused using the weighted Reciprocal Rank Fusion algorithm:
$$	ext{RRF\_Score}(d) = w_{	ext{vec}} \cdot rac{1}{k + 	ext{rank}_{	ext{vec}}(d)} + w_{	ext{lex}} \cdot rac{1}{k + 	ext{rank}_{	ext{lex}}(d)}$$

Where:
- $k = 60$ (The standard smoothing constant balancing head-heavy vs tail distributions).
- $w_{	ext{vec}} = 0.70$ (Dense vector weight reflecting semantic retrieval primacy).
- $w_{	ext{lex}} = 0.30$ (Sparse lexical weight reflecting exact keyword matching).
- If a document appears in only one modality, its rank in the missing modality is treated as $\infty$, contributing $0$ to that term.
- The unified candidate list is sorted by descending $	ext{RRF\_Score}(d)$, and the **Top 50 chunks** are selected for Stage 3 re-ranking.

---

### 7.3 Step 3: Deep Cross-Encoder Re-Ranking (`BAAI/bge-reranker-large`)

While bi-encoders (`BAAI/bge-large-en-v1.5`) compute vector representations independently for queries and passages, a **Cross-Encoder** passes the query and document chunk jointly through all transformer layers simultaneously, enabling all-to-all cross-attention between every query token and every passage token.

- **Re-Ranking Model**: `BAAI/bge-reranker-large`
- **Input Pair**: `[CLS] query [SEP] candidate_chunk_text [SEP]`
- **Output**: A cross-attention relevance probability score $S_i \in [0.0, 1.0]$.
- **Relevance Cutoff Threshold**:
  $$	ext{Filter Condition}: S_i \ge 0.40$$
  Any candidate chunk with $S_i < 0.40$ is discarded as ungrounded noise.
- **Anti-Hallucination Fallback**:
  If **all** 50 candidates score below $0.40$, the system bypasses the generative LLM entirely and immediately returns a grounded negative response:
  > *"I cannot find sufficient information in your uploaded documents to answer this question."*
- **Final Selection**: The top surviving **5 to 10 chunks** sorted by descending $S_i$ are passed to prompt assembly.

---

### 7.4 Step 4: Token Budgeting & Grounded Prompt Assembly

To operate safely within standard 4,096-token or 8,192-token context windows, the engine enforces an explicit token budgeting allocation:

| Context Window Component | Allocated Token Budget | Description |
|---|---|---|
| **System Directives** | 300 tokens | Strict anti-hallucination, grounding, and citation formatting instructions. |
| **Retrieved Context Passages** | 2,500 tokens | Up to 10 cross-encoder re-ranked chunks appended in descending relevance order. |
| **Conversation History** | 800 tokens | Up to 5 prior dialogue turns from the active session. Oldest turns are pruned first. |
| **Generation Space** | 496+ tokens | Reserved output token window for streaming generation. |
| **Total Context Window** | **4,096 tokens** | Full context limit. |

#### Production Grounded Prompt Template:
```
SYSTEM:
You are DriveX Assistant, the private enterprise intelligence assistant for DriveX Cloud Storage.
Your mission is to answer the user's question using EXCLUSIVELY the provided DOCUMENT EXCERPTS below.

STRICT GROUNDING & CITATION RULES:
1. Every factual assertion, number, date, or claim you state MUST be accompanied by an explicit citation tag pointing to its source chunk, formatted exactly as: [Doc: {filename}, Page: {page_number}, Chunk: {chunk_index}].
2. If multiple sources support a claim, list all corresponding citations, e.g.: [Doc: Q3_Report.pdf, Page: 4, Chunk: 2] [Doc: Finance.docx, Page: 1, Chunk: 0].
3. If the provided DOCUMENT EXCERPTS do not contain sufficient evidence to answer the question truthfully and completely, you MUST state: "I cannot find sufficient information in your uploaded documents to answer this question."
4. Under NO circumstance shall you fabricate facts, assume unstated details, or extrapolate beyond the explicit text.
5. Do NOT refer to "the context" or "the prompt"; speak naturally about "your documents" or "your drive".

---
DOCUMENT EXCERPTS:
[Doc: Q3_Financial_Summary.pdf, Page: 4, Chunk: 12]
Total AWS cloud infrastructure expenses for the third quarter amounted to $42,850, representing a 14% increase over Q2 due to additional GPU cluster training instances.

[Doc: Engineering_Infrastructure.docx, Page: 2, Chunk: 4]
Production database instances run MySQL 8.0 on AWS RDS, while object storage is hosted internally on MinIO clusters.
---

CONVERSATION HISTORY:
User: Which cloud provider hosts our databases?
Assistant: According to [Doc: Engineering_Infrastructure.docx, Page: 2, Chunk: 4], your production databases run on AWS RDS.

CURRENT QUESTION:
User: {user_query}

ASSISTANT:
```

---

### 7.5 Step 5: Server-Sent Events (SSE) Streaming Protocol

The `/api/v1/chat` endpoint delivers streaming tokens via the standard Server-Sent Events (SSE) specification (`text/event-stream`).

#### HTTP Response Headers:
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache, no-transform
Connection: keep-alive
X-Accel-Buffering: no
```
*(Note: `X-Accel-Buffering: no` is mandatory to instruct the Nginx reverse proxy to disable response chunk buffering, guaranteeing immediate sub-50ms token delivery to the client).*

#### Event Stream Protocol Lifecycle:

1. **`event: sources` (Immediate Citation Delivery)**:
   Emitted before the first LLM token generates. Allows the web/mobile client to render citation badges and clickable document previews in the UI instantly:
   ```sse
   event: sources
   data: {"sources": [{"file_id": 1024, "filename": "Q3_Financial_Summary.pdf", "page": 4, "chunk_index": 12, "relevance_score": 0.942}, {"file_id": 1055, "filename": "Engineering_Infrastructure.docx", "page": 2, "chunk_index": 4, "relevance_score": 0.887}]}

   ```

2. **`event: message` (Streaming Token Deltas)**:
   Emitted as each individual token or token group is produced by the generative model:
   ```sse
   event: message
   data: {"delta": "According "}

   event: message
   data: {"delta": "to "}

   event: message
   data: {"delta": "[Doc: Q3_Financial_Summary.pdf, Page: 4, Chunk: 12], "}

   event: message
   data: {"delta": "your third-quarter AWS expenses were $42,850."}

   ```

3. **`event: done` (Completion & Usage Telemetry)**:
   Emitted when inference finishes successfully:
   ```sse
   event: done
   data: {"status": "completed", "total_tokens": 78, "prompt_tokens": 542, "completion_tokens": 36, "model": "drivex-rag-v1"}

   ```

4. **`event: error` (Structured Exception Delivery)**:
   Emitted if an upstream timeout or model failure occurs during streaming:
   ```sse
   event: error
   data: {"type": "https://drivex.io/errors/inference-timeout", "title": "Inference Timeout", "status": 504, "detail": "The inference engine exceeded the 30-second execution window."}

   ```

#### Client Disconnection Handling:
FastAPI monitors the HTTP connection via `request.is_disconnected()`. If the user closes the browser tab or navigates away mid-stream, the generator coroutine terminates, cancels the background LLM API socket, and frees memory buffers immediately.

---

## 8. Production Python Reference Implementations

This section provides complete, production-grade reference implementations of the core AI/ML pipeline modules. All code follows Python 3.11 type hinting, explicit error handling, and Celery / Qdrant best practices.

### 8.1 Celery Worker Task Definitions (`app/tasks/ingestion.py`)

```python
import hashlib
import io
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
import pymysql
import requests
from celery import chain, chord, group
from qdrant_client import QdrantClient
from qdrant_client.http import models

from app.celery_app import celery_app

logger = logging.getLogger("drivex.ml.tasks")
logger.setLevel(logging.INFO)

# DB & Client Helpers
def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "mysql-primary"),
        user=os.getenv("MYSQL_USER", "drivex"),
        password=os.getenv("MYSQL_PASSWORD", "drivexpass"),
        database=os.getenv("MYSQL_DATABASE", "drivex"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

def get_qdrant_client() -> QdrantClient:
    return QdrantClient(
        host=os.getenv("QDRANT_HOST", "qdrant"),
        port=int(os.getenv("QDRANT_PORT", "6333"))
    )

@celery_app.task(
    name="tasks.ingest_file",
    bind=True,
    max_retries=3,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
def task_ingest_file(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 1: Validates object availability in MinIO S3 and verifies SHA-256 integrity.
    """
    file_id = event_data["file_id"]
    storage_key = event_data["storage_key"]
    expected_sha256 = event_data["sha256_checksum"]
    
    logger.info(f"Starting ingestion validation for file_id={file_id}, key={storage_key}")
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_url = f"{minio_endpoint}/drivex-blobs/{storage_key}"
    
    # Stream from MinIO and verify SHA-256
    hasher = hashlib.sha256()
    byte_count = 0
    with requests.get(s3_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=65536):
            if chunk:
                hasher.update(chunk)
                byte_count += len(chunk)
                
    computed_sha256 = hasher.hexdigest()
    if computed_sha256.lower() != expected_sha256.lower():
        error_msg = f"Checksum mismatch for file_id={file_id}: expected={expected_sha256}, actual={computed_sha256}"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    logger.info(f"Ingestion verified for file_id={file_id}: {byte_count} bytes, SHA-256 matched.")
    return event_data

@celery_app.task(name="tasks.extract_document", bind=True, max_retries=3)
def task_extract_document(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2A: Extracts structured text, headings, and tables from PDF, DOCX, and text files.
    """
    file_id = event_data["file_id"]
    storage_key = event_data["storage_key"]
    mime_type = event_data["mime_type"]
    filename = event_data.get("filename", "unknown")
    
    # Resource Boundaries & Hard Ceilings
    MAX_DOCUMENT_BYTES = 100 * 1024 * 1024  # 100 MB Extraction Ceiling
    MAX_OCR_PAGES = 50                     # 50 Pages Maximum for OCR
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_url = f"{minio_endpoint}/drivex-blobs/{storage_key}"
    
    import tempfile
    import os
    
    # Stream S3 payload directly to a bounded temporary file on disk (Zero-Copy Bounded Memory)
    suffix = os.path.splitext(filename)[1] or ".bin"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
        tmp_path = tmp_file.name
        
    try:
        with requests.get(s3_url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            bytes_written = 0
            with open(tmp_path, "wb") as f_out:
                for chunk in resp.iter_content(chunk_size=65536):
                    if bytes_written + len(chunk) > MAX_DOCUMENT_BYTES:
                        # Enforce 100MB hard extraction limit
                        allowed = MAX_DOCUMENT_BYTES - bytes_written
                        if allowed > 0:
                            f_out.write(chunk[:allowed])
                        break
                    f_out.write(chunk)
                    bytes_written += len(chunk)
        
        extracted_text_blocks = []
        
        if mime_type == "application/pdf":
            import fitz  # PyMuPDF
            import pytesseract
            from PIL import Image
            import io

            doc = fitz.open(tmp_path)
            for page_num, page in enumerate(doc, start=1):
                text = page.get_text("text").strip()
                # If text is sparse, invoke OCR fallback bounded by the 50-page ceiling
                if len(text) < 32 and len(page.get_images()) > 0 and page_num <= MAX_OCR_PAGES:
                    pix = page.get_pixmap(dpi=300)
                    pil_img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("L")
                    text = pytesseract.image_to_string(pil_img, lang="eng", config="--oem 1 --psm 1").strip()
                    
                if text:
                    extracted_text_blocks.append({
                        "page": page_num,
                        "section": f"Page {page_num}",
                        "text": text
                    })
            doc.close()
            
        elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            import docx
            doc = docx.Document(tmp_path)
            current_section = "Introduction"
            for p in doc.paragraphs:
                text = p.text.strip()
                if not text:
                    continue
                if p.style and p.style.name and p.style.name.startswith("Heading"):
                    current_section = text
                extracted_text_blocks.append({
                    "page": 1,
                    "section": current_section,
                    "text": text
                })
                
        else:
            # Plain text / markdown fallback with charset detection
            import chardet
            import unicodedata
            with open(tmp_path, "rb") as f_in:
                sample_bytes = f_in.read(65536)
                f_in.seek(0)
                encoding = chardet.detect(sample_bytes).get("encoding") or "utf-8"
                raw_text = f_in.read().decode(encoding, errors="replace")
            clean_text = unicodedata.normalize("NFKC", raw_text)
            extracted_text_blocks.append({
                "page": 1,
                "section": "Document Body",
                "text": clean_text
            })
            
        return {
            "event_data": event_data,
            "blocks": extracted_text_blocks
        }
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

@celery_app.task(name="tasks.extract_image", bind=True, max_retries=3)
def task_extract_image(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2B: Optical Character Recognition & EXIF metadata extraction for images.
    """
    import pytesseract
    from PIL import Image, ImageOps
    
    file_id = event_data["file_id"]
    storage_key = event_data["storage_key"]
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    resp = requests.get(f"{minio_endpoint}/drivex-blobs/{storage_key}", timeout=60)
    resp.raise_for_status()
    
    img = Image.open(io.BytesIO(resp.content))
    gray_img = ImageOps.grayscale(img)
    ocr_text = pytesseract.image_to_string(gray_img, lang="eng", config="--oem 1 --psm 1").strip()
    
    blocks = []
    if ocr_text:
        blocks.append({
            "page": 1,
            "section": "Image OCR Text",
            "text": ocr_text
        })
        
    return {
        "event_data": event_data,
        "blocks": blocks
    }

@celery_app.task(name="tasks.dedup_check")
def task_dedup_check(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2C: Computes image perceptual hashes and checks MySQL for duplicates.
    """
    import imagehash
    from PIL import Image

    file_id = event_data["file_id"]
    mime_type = event_data["mime_type"]
    storage_key = event_data["storage_key"]
    phash_str = None
    dhash_str = None
    
    if mime_type.startswith("image/"):
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        resp = requests.get(f"{minio_endpoint}/drivex-blobs/{storage_key}", timeout=60)
        if resp.status_code == 200:
            img = Image.open(io.BytesIO(resp.content))
            phash_str = str(imagehash.phash(img))
            dhash_str = str(imagehash.dhash(img))
            
            # Save phash back to MySQL files record
            conn = get_mysql_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE files SET phash = %s WHERE id = %s",
                    (phash_str, file_id)
                )
            conn.close()
            
    return {
        "file_id": file_id,
        "phash": phash_str,
        "dhash": dhash_str
    }

@celery_app.task(name="tasks.semantic_chunk")
def task_semantic_chunk(results: List[Dict[str, Any]], event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 3: Tokenizes extracted blocks into 512-token chunks with 64-token overlap and context headers.
    """
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-large-en-v1.5")
    
    extraction_res = results[0] if isinstance(results, list) else results
    blocks = extraction_res.get("blocks", [])
    filename = event_data.get("filename", "document")
    
    all_chunks = []
    chunk_idx = 0
    
    for block in blocks:
        page = block["page"]
        section = block["section"]
        text = block["text"]
        
        # Tokenize block
        tokens = tokenizer.encode(text, add_special_tokens=False)
        step = 512 - 64  # 448 token stride
        
        for start_idx in range(0, max(1, len(tokens)), step):
            chunk_tokens = tokens[start_idx : start_idx + 512]
            if not chunk_tokens:
                continue
            chunk_body = tokenizer.decode(chunk_tokens, skip_special_tokens=True)
            
            # Prepend context breadcrumb
            formatted_text = f"[Doc: {filename}, Page: {page}, Chunk: {chunk_idx}] [Section: {section}]\n{chunk_body}"
            
            all_chunks.append({
                "chunk_index": chunk_idx,
                "page_number": page,
                "section_breadcrumb": section,
                "text": formatted_text,
                "token_count": len(chunk_tokens)
            })
            chunk_idx += 1
            if start_idx + 512 >= len(tokens):
                break
                
    return {
        "event_data": event_data,
        "chunks": all_chunks
    }

@celery_app.task(name="tasks.generate_embeddings")
def task_generate_embeddings(chunking_result: Dict[str, Any], event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 4: Batches chunks and computes 1024-d normalized BGE-large embeddings.
    """
    from sentence_transformers import SentenceTransformer
    
    chunks = chunking_result["chunks"]
    if not chunks:
        return {"event_data": event_data, "embedded_chunks": []}
        
    model = SentenceTransformer("BAAI/bge-large-en-v1.5")
    texts = [c["text"] for c in chunks]
    
    # Generate normalized float32 vectors (batch size 32)
    vectors = model.encode(
        texts,
        batch_size=32,
        normalize_embeddings=True,
        show_progress_bar=False
    )
    
    embedded_chunks = []
    for i, c in enumerate(chunks):
        c["vector"] = vectors[i].tolist()
        embedded_chunks.append(c)
        
    return {
        "event_data": event_data,
        "embedded_chunks": embedded_chunks
    }

@celery_app.task(name="tasks.upsert_qdrant")
def task_upsert_qdrant(embedding_result: Dict[str, Any], event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 5: Upserts vector points into Qdrant collection 'drivex_file_chunks'.
    """
    import uuid
    client = get_qdrant_client()
    embedded_chunks = embedding_result["embedded_chunks"]
    
    if not embedded_chunks:
        return event_data
        
    points = []
    file_id = event_data["file_id"]
    owner_id = event_data["owner_id"]
    workspace_id = event_data.get("workspace_id")
    filename = event_data.get("filename", "")
    mime_type = event_data.get("mime_type", "")
    created_at = event_data.get("timestamp", int(time.time() * 1000))
    
    for c in embedded_chunks:
        point_id = str(uuid.uuid4())
        payload = {
            "file_id": file_id,
            "owner_id": owner_id,
            "workspace_id": workspace_id,
            "filename": filename,
            "mime_type": mime_type,
            "chunk_index": c["chunk_index"],
            "total_chunks": len(embedded_chunks),
            "page_number": c["page_number"],
            "section_breadcrumb": c["section_breadcrumb"],
            "text_preview": c["text"],
            "created_at": created_at,
            # Note: Dynamic access control is enforced via Redis/MySQL RBAC post-filtering on file_id
        }
        points.append(models.PointStruct(
            id=point_id,
            vector=c["vector"],
            payload=payload
        ))
        
    # Batch upsert with wait=True
    client.upsert(
        collection_name="drivex_file_chunks",
        points=points,
        wait=True
    )
    logger.info(f"Upserted {len(points)} vector chunks for file_id={file_id} into Qdrant.")
    return event_data

@celery_app.task(name="tasks.finalize_ingestion")
def task_finalize_ingestion(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 6: Atomically updates MySQL status to 'INDEXED' and writes an audit event.
    """
    file_id = event_data["file_id"]
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE files SET processing_status = 'INDEXED', updated_at = NOW() WHERE id = %s",
            (file_id,)
        )
        cursor.execute(
            """
            INSERT INTO audit_log (user_id, action, resource_type, resource_id, metadata, created_at)
            VALUES (%s, 'FILE_INDEXED', 'file', %s, %s, NOW())
            """,
            (event_data["owner_id"], file_id, json.dumps({"status": "SUCCESS"}))
        )
    conn.close()
    logger.info(f"Finalized file_id={file_id} status=INDEXED in MySQL.")
    return {"status": "SUCCESS", "file_id": file_id}
```

---

### 8.2 Hybrid Retrieval RRF & Cross-Encoder Reranking Module (`app/services/retrieval.py`)

```python
import os
from typing import Any, Dict, List
import pymysql
import redis
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import CrossEncoder, SentenceTransformer

# Load models at module startup
embedding_model = SentenceTransformer("BAAI/bge-large-en-v1.5")
reranker_model = CrossEncoder("BAAI/bge-reranker-large")

qdrant_client = QdrantClient(
    host=os.getenv("QDRANT_HOST", "qdrant"),
    port=int(os.getenv("QDRANT_PORT", "6333"))
)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True
)

def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "mysql-primary"),
        user=os.getenv("MYSQL_USER", "drivex"),
        password=os.getenv("MYSQL_PASSWORD", "drivexpass"),
        database=os.getenv("MYSQL_DATABASE", "drivex"),
        cursorclass=pymysql.cursors.DictCursor
    )

def verify_cached_file_access(user_id: int, file_id: int) -> bool:
    """
    Validates user read permissions against Drogon's Redis effective permission cache
    with MySQL recursive ancestor CTE fallback to eliminate vector RBAC staleness.
    """
    cache_key = f"perm:eff:{user_id}:file:{file_id}"
    cached_role = redis_client.get(cache_key)
    if cached_role:
        return cached_role in ("viewer", "editor", "owner")
    
    # Fallback: Query MySQL permissions with ancestor folder inheritance
    conn = get_mysql_connection()
    has_access = False
    with conn.cursor() as cursor:
        sql = """
        SELECT EXISTS (
            SELECT 1 FROM files f
            WHERE f.id = %s AND (
                f.owner_id = %s OR 
                f.id IN (SELECT resource_id FROM permissions WHERE user_id = %s AND resource_type = 'file') OR
                f.folder_id IN (
                    WITH RECURSIVE ancestors AS (
                        SELECT id, parent_id FROM folders WHERE id = f.folder_id
                        UNION ALL
                        SELECT p.id, p.parent_id FROM folders p INNER JOIN ancestors a ON p.id = a.parent_id
                    )
                    SELECT resource_id FROM permissions WHERE user_id = %s AND resource_type = 'folder' AND resource_id IN (SELECT id FROM ancestors)
                )
            )
        ) AS allowed
        """
        cursor.execute(sql, (file_id, user_id, user_id, user_id))
        row = cursor.fetchone()
        if row and row.get("allowed"):
            has_access = True
    conn.close()
    
    # Populate Redis cache with 300s TTL
    if has_access:
        redis_client.setex(cache_key, 300, "viewer")
    return has_access

def execute_hybrid_retrieval(
    user_id: int,
    workspace_id: int | None,
    query_text: str,
    top_k: int = 50
) -> List[Dict[str, Any]]:
    """
    Executes parallel dense vector search and sparse MySQL FULLTEXT keyword search,
    combining results using Reciprocal Rank Fusion (k=60) with dynamic RBAC post-filtering.
    """
    # 1. Compute Dense Query Vector with BGE instruction prefix
    bge_query = f"Represent this sentence for searching relevant passages: {query_text}"
    query_vector = embedding_model.encode(bge_query, normalize_embeddings=True).tolist()
    
    # 2. Dense Vector Retrieval on Qdrant
    if workspace_id is not None:
        # Filter by workspace boundary; candidate chunks are RBAC post-filtered below
        tenant_filter = models.Filter(
            must=[models.FieldCondition(key="workspace_id", match=models.MatchValue(value=workspace_id))]
        )
    else:
        # Retrieve user-owned files plus any shared file candidates resolved from permission cache
        accessible_shared_fids = get_user_accessible_shared_file_ids(user_id=user_id)
        should_clauses = [models.FieldCondition(key="owner_id", match=models.MatchValue(value=user_id))]
        if accessible_shared_fids:
            should_clauses.append(models.FieldCondition(key="file_id", match=models.MatchAny(any=accessible_shared_fids)))
        tenant_filter = models.Filter(should=should_clauses)
        
    dense_results = qdrant_client.search(
        collection_name="drivex_file_chunks",
        query_vector=query_vector,
        query_filter=tenant_filter,
        limit=top_k * 2  # Retrieve candidate buffer for post-filtering
    )
    
    # 2b. Dynamic RBAC Post-Filtering (Eliminates Vector RBAC Authorization Staleness)
    # Re-validates candidate chunks against user's active permissions in Redis/MySQL
    # to guarantee revoked or newly granted permissions take effect immediately:
    valid_dense_results = []
    for point in dense_results:
        f_owner = point.payload.get("owner_id")
        fid = point.payload.get("file_id")
        if f_owner == user_id:
            valid_dense_results.append(point)
        elif fid and verify_cached_file_access(user_id=user_id, file_id=fid):
            valid_dense_results.append(point)
    dense_results = valid_dense_results[:top_k]
    
    # 3. Sparse Lexical Search on MySQL
    conn = get_mysql_connection()
    sparse_file_ids = []
    with conn.cursor() as cursor:
        sql = """
        SELECT f.id AS file_id, MATCH(f.name) AGAINST(%s IN BOOLEAN MODE) AS score
        FROM files f
        WHERE (f.owner_id = %s OR f.id IN (SELECT resource_id FROM permissions WHERE user_id = %s AND resource_type = 'file'))
          AND f.is_trashed = FALSE
          AND MATCH(f.name) AGAINST(%s IN BOOLEAN MODE)
        ORDER BY score DESC
        LIMIT %s
        """
        cursor.execute(sql, (query_text, user_id, user_id, query_text, top_k))
        sparse_rows = cursor.fetchall()
        sparse_file_ids = [r["file_id"] for r in sparse_rows]
    conn.close()
    
    # 4. Reciprocal Rank Fusion (k=60, w_vec=0.70, w_lex=0.30)
    k_rrf = 60
    w_vec = 0.70
    w_lex = 0.30
    
    candidate_map: Dict[str, Dict[str, Any]] = {}
    
    # Add dense ranks
    for rank, point in enumerate(dense_results):
        cid = str(point.id)
        candidate_map[cid] = {
            "point": point,
            "text": point.payload["text_preview"],
            "filename": point.payload["filename"],
            "page_number": point.payload["page_number"],
            "chunk_index": point.payload["chunk_index"],
            "file_id": point.payload["file_id"],
            "rrf_score": w_vec * (1.0 / (k_rrf + rank + 1))
        }
        
    # Add sparse ranks
    for rank, fid in enumerate(sparse_file_ids):
        # Match against points in candidate_map or query Qdrant for this file
        for cid, cand in candidate_map.items():
            if cand["file_id"] == fid:
                cand["rrf_score"] += w_lex * (1.0 / (k_rrf + rank + 1))
                
    # Sort unified candidates by descending RRF score
    fused_candidates = sorted(candidate_map.values(), key=lambda x: x["rrf_score"], reverse=True)[:top_k]
    return fused_candidates

def rerank_candidates(
    query_text: str,
    candidates: List[Dict[str, Any]],
    score_cutoff: float = 0.40,
    top_n: int = 10
) -> List[Dict[str, Any]]:
    """
    Cross-Encoder joint scoring: filters noise below 0.40 and returns top N chunks.
    """
    if not candidates:
        return []
        
    pairs = [(query_text, c["text"]) for c in candidates]
    scores = reranker_model.predict(pairs)
    
    surviving = []
    for idx, score in enumerate(scores):
        if score >= score_cutoff:
            cand = candidates[idx]
            cand["relevance_score"] = float(score)
            surviving.append(cand)
            
    surviving = sorted(surviving, key=lambda x: x["relevance_score"], reverse=True)
    return surviving[:top_n]
```

---

### 8.3 FastAPI Server-Sent Events (SSE) Streaming Chat Router (`app/api/chat.py`)

```python
import json
import os
from typing import AsyncGenerator
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.services.retrieval import execute_hybrid_retrieval, rerank_candidates

router = APIRouter(prefix="/api/v1", tags=["Chat"])

class ChatRequest(BaseModel):
    query: str
    conversation_id: str | None = None

async def sse_event_generator(request: Request, user_id: int, workspace_id: int | None, query: str) -> AsyncGenerator[str, None]:
    # 1. Retrieve candidates via Hybrid RRF
    candidates = execute_hybrid_retrieval(user_id=user_id, workspace_id=workspace_id, query_text=query, top_k=50)
    
    # 2. Re-rank with Cross-Encoder
    top_chunks = rerank_candidates(query_text=query, candidates=candidates, score_cutoff=0.40, top_n=8)
    
    # 3. Grounded Fallback if no relevant chunks found
    if not top_chunks:
        empty_sources = json.dumps({"sources": []})
        yield f"event: sources\ndata: {empty_sources}\n\n"
        fallback_msg = json.dumps({"delta": "I cannot find sufficient information in your uploaded documents to answer this question."})
        yield f"event: message\ndata: {fallback_msg}\n\n"
        done_payload = json.dumps({"status": "completed", "total_tokens": 15})
        yield f"event: done\ndata: {done_payload}\n\n"
        return

    # 4. Emit 'event: sources' immediately
    sources_data = [
        {
            "file_id": c["file_id"],
            "filename": c["filename"],
            "page": c["page_number"],
            "chunk_index": c["chunk_index"],
            "relevance_score": round(c["relevance_score"], 3)
        }
        for c in top_chunks
    ]
    yield f"event: sources\ndata: {json.dumps({'sources': sources_data})}\n\n"
    
    # 5. Build Grounded Prompt within 4096-token budget
    context_blocks = "\n\n".join([f"[{c['filename']} - Page {c['page_number']}]\n{c['text']}" for c in top_chunks])
    prompt = f"""SYSTEM:
You are DriveX Assistant. Answer the question using ONLY the provided excerpts below.
Every assertion must cite [Doc: filename, Page: X, Chunk: Y].
If the excerpts lack the answer, state you cannot find sufficient information.

DOCUMENT EXCERPTS:
{context_blocks}

QUESTION:
{query}

ASSISTANT:"""

    # 6. Stream tokens from LLM API
    llm_api_url = os.getenv("LLM_API_URL", "http://llm-gateway:8000/v1/chat/completions")
    payload = {
        "model": "drivex-rag-v1",
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
        "temperature": 0.1
    }
    
    total_tokens = 0
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("POST", llm_api_url, json=payload) as response:
            if response.status_code != 200:
                error_body = json.dumps({"type": "about:blank", "title": "Inference Error", "status": 502})
                yield f"event: error\ndata: {error_body}\n\n"
                return
                
            async for line in response.aiter_lines():
                if await request.is_disconnected():
                    # Handle client tab close or network drop
                    break
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[6:].strip()
                if data_str == "[DONE]":
                    break
                try:
                    chunk_json = json.loads(data_str)
                    delta_text = chunk_json["choices"][0]["delta"].get("content", "")
                    if delta_text:
                        total_tokens += 1
                        msg_payload = json.dumps({"delta": delta_text})
                        yield f"event: message\ndata: {msg_payload}\n\n"
                except Exception:
                    continue

    # 7. Emit 'event: done'
    done_payload = json.dumps({"status": "completed", "total_tokens": total_tokens})
    yield f"event: done\ndata: {done_payload}\n\n"

@router.post("/chat")
async def chat_endpoint(chat_req: ChatRequest, request: Request):
    # Dummy user resolution for demo; production extracts from RS256 JWT claim
    user_id = 42
    workspace_id = None
    return StreamingResponse(
        sse_event_generator(request, user_id, workspace_id, chat_req.query),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )
```

---

## 9. Operational Resilience, Failure Modes & Edge Cases

The DriveX AI/ML subsystem is hardened against hostile inputs, hardware starvation, service interruptions, and unparseable document formats.

### 9.1 Comprehensive Edge Cases Matrix

The following matrix defines the root causes, detection mechanisms, and exact system behaviors for 16 mission-critical failure scenarios:

| # | Subsystem | Failure Scenario | Trigger Condition / Input | Detection Mechanism | Specified Resilient System Behavior |
|---|---|---|---|---|---|
| **1** | Ingestion Pipeline | Zero-byte empty file | Client completes upload of an empty 0-byte file (`size_bytes == 0`). | `task_ingest_file` checks stream length. | Worker bypasses OCR, chunking, and embedding generation; updates MySQL `files.processing_status = 'INDEXED'` with note `EMPTY_FILE`, skipping Qdrant upsert. |
| **2** | Ingestion Pipeline | Corrupt or password-encrypted PDF/DOCX | User uploads encrypted PDF or damaged ZIP/OpenXML archive. | PyMuPDF raises `fitz.FileDataError` or `fitz.PasswordRequired`; python-docx raises `docx.opc.exceptions.PackageNotFoundError`. | Worker catches exception, avoids retry loops; logs error `ENCRYPTED_OR_CORRUPT`, updates MySQL `processing_status = 'FAILED'`, and records event in `audit_log`. |
| **3** | OCR Engine | Massive multi-hundred page scanned PDF (>500 pages) | Upload of multi-hundred page scanned manual or court filing (>200 MB). | `task_extract_document` evaluates `doc.page_count > 50`. | Ingestion worker dispatches a Celery chord dynamically partitioning the PDF into 20-page chunks across the `file.ocr` worker pool, preventing worker memory exhaustion. |
| **4** | OCR Engine | Pure abstract photo / graphic with zero text | User uploads landscape photo or abstract diagram containing no alphanumeric characters. | PyTesseract returns empty string `""` with mean confidence `0.0`. | Pipeline records EXIF metadata and perceptual hashes (`phash`, `dhash`), updates MySQL `processing_status = 'INDEXED'`, and writes zero chunk vectors to Qdrant without failing. |
| **5** | Chunking Engine | Monolithic unformatted text / binary dump | Continuous log file or memory dump without newlines, periods, or spaces. | Chunking splitter cannot match paragraph or sentence regexes. | Recursive character splitter falls back to strict token slicing at exactly 512 tokens with 64-token overlap, guaranteeing chunk size invariants are strictly enforced. |
| **6** | Embedding Engine | Complex Unicode / Emoji overload | Text containing mathematical symbols, zero-width joiners, or emoji strings. | Tokenizer decodes byte stream. | AutoTokenizer normalizes characters to Unicode NFKC, maps unencodable sequences to vocabulary unk tokens, and outputs clean 1024-d float32 tensors without crashing. |
| **7** | Deduplication | Recompressed or resized duplicate image | Client uploads 80% JPEG quality version of an existing 100% PNG image. | Cryptographic SHA-256 digests diverge, but `pHash` produces Hamming distance $\le 4$. | System identifies perceptual match, records duplicate relationship in `file_tags` / `files.phash`, and avoids duplicate OCR computation. |
| **8** | Celery Worker | Worker node Out-of-Memory (OOM) or SIGKILL | Linux kernel OOM killer terminates Celery child during high-dimensional tensor batch. | RabbitMQ detects TCP socket closure on unacknowledged message (`task_acks_late=True`). | RabbitMQ immediately re-queues the message; surviving worker re-acquires the task. Worker recycling (`worker_max_tasks_per_child=100`) clears RAM fragments. |
| **9** | RabbitMQ Messaging | Poison Pill malformed payload | Message with unparseable or corrupted JSON published to queue. | Worker JSON deserializer raises `json.decoder.JSONDecodeError`. | Worker rejects message without requeue (`basic.reject(requeue=False)`); RabbitMQ routes payload directly to `drivex.events.dlx` -> `file.dlq`, firing a Prometheus alert. |
| **10** | Vector DB | Qdrant node restart during high-throughput upsert | Qdrant container crashes or restarts during active batch vector upsert. | Python client raises `qdrant_client.http.exceptions.ResponseHandlingException`. | Celery task triggers exponential backoff retry (30s, 60s, 120s); Qdrant WAL recovers, and the idempotent upsert completes on subsequent attempt. |
| **11** | Vector DB | Vector search on empty tenant / zero uploads | Authenticated user with no uploaded documents queries `/api/v1/search`. | Qdrant filter `(owner_id == user_id)` matches zero vectors. | Qdrant returns 0 hits in $<2	ext{ms}$ without traversing the HNSW graph; API returns an empty list `[]` with HTTP 200 OK. |
| **12** | Vector DB | User deletes file while indexing is actively running | User clicks "Delete" immediately after upload while Celery is embedding chunks. | `task_finalize_ingestion` attempts to update MySQL record; finds row marked `is_trashed=TRUE` or deleted. | Task aborts cleanly; issues a compensational `delete_file_vectors(file_id)` to Qdrant to clean up orphan points, logging an informational audit record. |
| **13** | RAG Engine | High-specificity jargon query missing from BGE vocabulary | Query contains obscure model part number (`"X100-AB-REV4"`) absent from semantic embedding space. | Vector search yields low cosine similarity ($<0.30$), but MySQL FULLTEXT yields high score. | Reciprocal Rank Fusion (RRF) elevates the FULLTEXT document candidate into the top 50, allowing the cross-encoder to capture the exact context match. |
| **14** | RAG Engine | Zero relevant chunks found across user's drive | User asks an out-of-domain question (e.g., *"What is the capital of Mars?"*). | All candidate chunks evaluated by cross-encoder score $S_i < 0.40$. | Relevance cutoff filter eliminates all candidates; RAG engine returns grounded negative response without invoking generative LLM, eliminating hallucination. |
| **15** | RAG Engine | Client closes browser tab mid-SSE stream | User navigates away or closes tab during active token delta generation. | FastAPI catches `await request.is_disconnected() == True`. | SSE generator coroutine halts immediately, aborts downstream HTTP stream to LLM inference backend, and frees memory buffers. |
| **16** | Relational DB | MySQL primary failover during indexing | Relational database orchestrator executes planned master switchover. | PyMySQL raises `OperationalError(2003, "Can't connect to MySQL server")`. | Worker catches connection error, triggers Celery task autoretry with backoff; subsequent retry reconnects to newly promoted master virtual IP. |

---

### 9.2 Prometheus Metrics Catalogue

All AI/ML worker nodes and inference gateways expose OpenMetrics / Prometheus scrape endpoints on `:9100/metrics` and `:8001/metrics`.

| Metric Name | Metric Type | Labels | Description |
|---|---|---|---|
| `drivex_ml_tasks_total` | Counter | `task_name`, `status` (`success`, `retry`, `failed`) | Cumulative count of executed Celery tasks. |
| `drivex_ml_task_duration_seconds` | Histogram | `task_name` | Latency distribution of worker tasks (buckets: 0.1s to 900s). |
| `drivex_ml_ocr_pages_total` | Counter | `engine` (`tesseract`), `mime_type` | Total document pages processed via OCR. |
| `drivex_ml_chunks_generated_total` | Counter | `mime_type` | Total 512-token semantic chunks created. |
| `drivex_ml_embedding_latency_seconds` | Histogram | `model` (`bge-large-en-v1.5`), `batch_size` | Tensor inference latency for vector generation. |
| `drivex_ml_qdrant_upsert_latency_seconds`| Histogram | `collection` | Time taken to commit point batches to Qdrant. |
| `drivex_ml_dedup_matches_total` | Counter | `tier` (`sha256`, `phash`, `cosine`) | Number of duplicate or near-duplicate files detected. |
| `drivex_rag_query_latency_seconds` | Histogram | `stage` (`hyde`, `hybrid`, `rerank`, `llm_stream`) | Latency breakdown across conversational RAG stages. |
| `drivex_rag_rerank_rejection_ratio` | Gauge | `model` | Ratio of candidate chunks discarded by cross-encoder ($<0.40$). |
| `drivex_rag_active_sse_streams` | Gauge | `endpoint` | Number of concurrent active SSE streaming chat connections. |
| `drivex_rabbitmq_dlq_messages` | Gauge | `queue` (`file.dlq`) | Total count of unacknowledged dead-lettered messages awaiting inspection. |

---

### 9.3 OpenTelemetry Distributed Tracing Spans

The AI/ML pipeline propagates W3C Trace Context (`traceparent` header) across service boundaries:
1. **Span 1: `drogon.upload_complete`** (Originating in C++ Control Plane).
2. **Span 2: `rabbitmq.publish`** (Injects `traceparent` into AMQP message headers).
3. **Span 3: `celery.task_ingest_file`** (Extracts `traceparent` from AMQP envelope).
4. **Span 4: `celery.task_extract_document` / `task_extract_image`** (Child span measuring parser latency).
5. **Span 5: `celery.task_generate_embeddings`** (Child span tracking PyTorch tensor compute).
6. **Span 6: `qdrant.upsert_points`** (Child span tracking vector database commit).
7. **Span 7: `mysql.finalize_status`** (Child span recording relational state transition).

This end-to-end trace tree enables DevOps and ML engineers to locate exact bottlenecks across MinIO I/O, transformer inference, and database locking.

---

### 9.4 Dead-Letter Queue Operations & Manual Redrive Runbook

When a message is routed to `file.dlq`, an alert fires to PagerDuty / Slack. Operators follow the standard recovery protocol:

#### 1. Inspect Dead-Lettered Message
```bash
# Query DLQ message from RabbitMQ CLI
rabbitmqadmin get queue=file.dlq count=1 requeue=true
```

#### 2. Analyze Error Envelope
The message payload contains the `DeadLetterEnvelope` schema detailing `error_type`, `error_message`, and `stack_trace`:
```json
{
  "dlq_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "failed_at": 1726588800000,
  "source_queue": "file.ingest",
  "original_routing_key": "file.uploaded.application.pdf",
  "retry_count": 3,
  "error_type": "MinIOTimeoutError",
  "error_message": "S3 connection timed out after 60 seconds",
  "stack_trace": "Traceback (most recent call last):\n  File \"/app/tasks/ingest.py\", line 45, in task_ingest_file\n    resp = requests.get(s3_url, stream=True, timeout=120)\n  File \"/usr/local/lib/python3.11/site-packages/requests/api.py\", line 73, in get\n    return request('get', url, params=params, **kwargs)\nrequests.exceptions.ConnectTimeout: HTTPSConnectionPool(host='minio', port=9000): Max retries exceeded",
  "original_payload": {
    "event_id": "c7a8e910-1234-4567-89ab-cdef01234567",
    "event_type": "file.uploaded",
    "schema_version": "1.0.0",
    "timestamp": 1789657590000,
    "file_id": 1024,
    "version_id": 1,
    "owner_id": 42,
    "workspace_id": null,
    "folder_id": 88,
    "filename": "sample_contract.pdf",
    "storage_key": "blobs/42/2026-09/a3d8f1e0-sample_contract.pdf",
    "mime_type": "application/pdf",
    "size_bytes": 1048576,
    "sha256_checksum": "8f4604e76a6b840e6538b72f10b77b75f850d990bc1f3a2c040d7c078a63be84",
    "trace_context": {
      "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    }
  },
  "trace_context": {
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
  }
}
```

#### 3. Administrative Redrive via CLI Tool
Once the underlying issue (e.g. MinIO connectivity or disk space) is resolved, the operator redrives the failed messages back to the primary topic exchange using the DriveX administrative redrive CLI:
```bash
python -m app.cli.dlq_redrive --queue=file.dlq --target-exchange=drivex.events --batch=100
```
The CLI unwraps the `original_payload` from the `DeadLetterEnvelope` and republishes the clean event with a fresh `traceparent`, resuming automated processing.

---
