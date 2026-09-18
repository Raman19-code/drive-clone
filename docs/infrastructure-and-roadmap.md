# DriveX Infrastructure, Security, Observability & Phased Roadmap Specification

**Document Version**: 1.0.0-RELEASE  
**Status**: Authoritative Technical Specification (Requirement R5 Master Blueprint)  
**Target System**: DriveX Distributed Cloud Storage & AI Search Platform  
**Target Audience**: DevOps Engineers, SREs, Security Architects, Systems Implementers  

---

## 1. Executive Summary & Core Architectural Invariants

DriveX is an enterprise-grade, self-hosted, scalable cloud storage platform engineered to deliver the capabilities of modern cloud drives (such as Google Drive) while maintaining complete sovereignty over data, low-latency metadata operations, and an integrated, event-driven Artificial Intelligence / Vector Search plane.

To achieve sustained performance across workloads exceeding 10,000 concurrent active users and multi-terabyte datasets, the DriveX infrastructure enforces three foundational operational invariants:

```
+---------------------------------------------------------------------------------------------------+
|                                   DRIVEX INFRASTRUCTURE INVARIANTS                                 |
+---------------------------------------------------------------------------------------------------+
| 1. ZERO-BYTE CONTROL PLANE BOTTLENECK:                                                            |
|    Binary data streams NEVER transit through or buffer within the Drogon C++ API or Python        |
|    ML worker processes. All uploads and downloads stream directly between the client user-agent   |
|    and the MinIO S3 object storage cluster via cryptographically signed SigV4 Pre-Signed URLs.    |
|                                                                                                   |
| 2. ASYNCHRONOUS DECOUPLING OF COMPUTATIONAL WORKLOADS:                                            |
|    Heavy compute operations (PyMuPDF document parsing, Tesseract OCR, semantic token chunking,    |
|    BAAI/bge-large-en-v1.5 embedding generation, and perceptual pHash/dHash calculation) are        |
|    completely offloaded to an asynchronous AMQP message bus (RabbitMQ) and executed by a Celery   |
|    worker fleet with strict concurrency controls and dead-letter isolation.                      |
|                                                                                                   |
| 3. TWO-PHASE CONSISTENT RESOURCE ENFORCEMENT:                                                     |
|    All state transitions governing physical resource limits (storage quotas, rate limits,         |
|    and file write locks) execute via a two-phase reservation pattern: fast, atomic in-memory      |
|    reservation in Redis 7 (pre-operation), followed by transactional reconciliation and write     |
|    commitment in MySQL 8 (post-operation).                                                        |
+---------------------------------------------------------------------------------------------------+
```

### 1.1 Complete Subsystem Topology Matrix (13 Production Services)

The production deployment decouples the platform into 13 specialized containerized services operating across segmented networks:

| # | Service Name | Binary / Base Image | Host Ports | Internal Ports | Network Segments | Storage / Volumes | Role & Lifecycle |
|---|--------------|---------------------|------------|----------------|------------------|-------------------|------------------|
| 1 | `frontend-nginx` | `nginx:1.25-alpine` | `80:80`, `443:443` | `80`, `443` | `drivex-public`, `drivex-internal` | Nginx configs, SSL certs, HTMX templates | Edge reverse proxy, TLS 1.3 termination, rate limiting, SSE passthrough |
| 2 | `drogon-api` | C++20 Drogon (`ubuntu:24.04`) | None (Proxied) | `8080` | `drivex-internal`, `drivex-storage` | Ephemeral config mount | High-throughput stateless REST control plane, auth, metadata, MinIO URL broker |
| 3 | `ml-workers-api` | Python 3.11 FastAPI / Uvicorn | None (Proxied) | `8001` | `drivex-internal` | Read-only model cache | Internal HTTP gateway for semantic search and streaming conversational RAG (SSE) |
| 4 | `ml-workers-celery` | Python 3.11 Celery 5.4 | None (Internal) | None | `drivex-internal`, `drivex-storage` | Shared temp scratch volume, model cache | Asynchronous worker fleet: OCR, text extraction, semantic chunking, embeddings |
| 5 | `mysql-primary` | `mysql:8.0` | `3306:3306` (Optional) | `3306` | `drivex-internal` | `mysql_primary_data` (NVMe PVC) | Authoritative transactional relational store; binary logging enabled for replication |
| 6 | `mysql-replica` | `mysql:8.0` | None | `3306` | `drivex-internal` | `mysql_replica_data` (NVMe PVC) | Read-only replica for directory browsing, search hydration, and reporting queries |
| 7 | `redis` | `redis:7.2-alpine` | `6379:6379` (Internal) | `6379` | `drivex-internal` | `redis_data` (AOF persistent) | Cache-aside store, active session tokens, quota reservations, distributed locks |
| 8 | `minio` | `minio/minio:RELEASE.2024-05-10` | `9000:9000`, `9001:9001` | `9000`, `9001` | `drivex-public`, `drivex-storage` | `minio_data` (Distributed erasure-coded) | S3-compatible object storage data plane; direct upload/download destination |
| 9 | `rabbitmq` | `rabbitmq:3.13-management-alpine` | `5672:5672`, `15672:15672` | `5672`, `15672` | `drivex-internal` | `rabbitmq_data` (Durable queue storage) | AMQP 0-9-1 message broker, topic exchanges, DLQ, and retry queues |
| 10 | `qdrant` | `qdrant/qdrant:v1.9.2` | `6333:6333` | `6333`, `6334` | `drivex-internal` | `qdrant_data` (Persistent vector store) | Vector database; HNSW graph indexing, INT8 scalar quantization, tenant payload filtering |
| 11 | `otel-collector` | `otel/opentelemetry-collector-contrib:0.99.0` | None | `4317` (gRPC), `4318` (HTTP), `8889` | `drivex-internal` | Collector config mount | Central telemetry ingest; trace parsing, context propagation, export to backends |
| 12 | `prometheus` | `prom/prometheus:v2.52.0` | `9090:9090` | `9090` | `drivex-internal` | `prometheus_data` (TSDB volume) | Time-series metrics engine; scraping API RED metrics, Celery queues, DB pools |
| 13 | `grafana` | `grafana/grafana:10.4.2` | `3000:3000` | `3000` | `drivex-internal`, `drivex-public` | `grafana_data`, dashboard provisioning | Operational visualization: Golden Signals, Storage Bandwidth, AI Pipeline Analytics |

---

## 2. Section 1: Production Deployment Topologies

### 2.1 Complete Production Multi-Service Stack (`docker-compose.prod.yml`)

The following specification provides the complete, production-hardened 13-service Docker Compose topology. It enforces non-root container execution, strict CPU/Memory resource constraints, container-level healthchecks with exponential backoff, isolated network bridges, and persistent volume definitions.

```yaml
version: "3.9"

# ==============================================================================
# DriveX Production Multi-Service Orchestration Specification
# Target Architecture: 13-Service Complete Cloud Storage & AI Search Stack
# ==============================================================================

networks:
  # Perimeter network: Ingress Nginx, MinIO direct transfers, Grafana external UI
  drivex-public:
    driver: bridge
    ipam:
      driver: default
      config:
        - subnet: 172.28.10.0/24
    driver_opts:
      com.docker.network.bridge.name: br-drivex-pub

  # Internal microservices network: Control Plane, Workers, DBs, Caches, Telemetry
  drivex-internal:
    driver: bridge
    internal: true
    ipam:
      driver: default
      config:
        - subnet: 172.28.20.0/24
    driver_opts:
      com.docker.network.bridge.name: br-drivex-int

  # Dedicated high-throughput storage network: MinIO, Drogon API, Celery Workers
  drivex-storage:
    driver: bridge
    internal: true
    ipam:
      driver: default
      config:
        - subnet: 172.28.30.0/24
    driver_opts:
      com.docker.network.bridge.name: br-drivex-str

volumes:
  mysql_primary_data:
    driver: local
  mysql_replica_data:
    driver: local
  redis_data:
    driver: local
  minio_data:
    driver: local
  rabbitmq_data:
    driver: local
  qdrant_data:
    driver: local
  prometheus_data:
    driver: local
  grafana_data:
    driver: local
  model_cache:
    driver: local

services:
  # ----------------------------------------------------------------------------
  # 1. Edge Ingress & Reverse Proxy: Nginx
  # ----------------------------------------------------------------------------
  frontend-nginx:
    image: nginx:1.25-alpine
    container_name: drivex-frontend-nginx
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ../infra/docker/nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ../infra/docker/nginx/conf.d:/etc/nginx/conf.d:ro
      - ../infra/docker/nginx/ssl:/etc/nginx/ssl:ro
      - ../frontend/static:/usr/share/nginx/html/static:ro
      - ../frontend/templates:/usr/share/nginx/html/templates:ro
    networks:
      - drivex-public
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 1024M
        reservations:
          cpus: "0.5"
          memory: 256M
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://127.0.0.1:80/healthz || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 5s
    depends_on:
      drogon-api:
        condition: service_healthy
      ml-workers-api:
        condition: service_healthy

  # ----------------------------------------------------------------------------
  # 2. Control Plane: Drogon C++ REST API Server
  # ----------------------------------------------------------------------------
  drogon-api:
    build:
      context: ../api
      dockerfile: ../infra/docker/Dockerfile.api
    image: drivex/drogon-api:1.0.0-prod
    container_name: drivex-drogon-api
    restart: unless-stopped
    environment:
      - APP_ENV=production
      - LOG_LEVEL=INFO
      - SERVER_THREADS=16
      - DB_HOST=mysql-primary
      - DB_PORT=3306
      - DB_USER=drivex_app
      - DB_PASSWORD=DriveX_Secure_Passwd_2026!
      - DB_NAME=drivex
      - DB_MAX_CONNECTIONS=100
      - DB_REPLICA_HOST=mysql-replica
      - DB_REPLICA_PORT=3306
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - REDIS_PASSWORD=DriveX_Redis_Passwd_2026!
      - MINIO_ENDPOINT=http://minio:9000
      - MINIO_EXTERNAL_ENDPOINT=https://storage.drivex.example.com
      - MINIO_ACCESS_KEY=drivex_minio_admin
      - MINIO_SECRET_KEY=DriveX_MinIO_Secret_Key_2026!
      - MINIO_BUCKET=drivex-blobs
      - RABBITMQ_URL=amqp://drivex_mq:DriveX_Rabbit_Passwd_2026!@rabbitmq:5672//
      - JWT_PUBLIC_KEY_PATH=/etc/drivex/keys/jwt_rs256.pub
      - JWT_PRIVATE_KEY_PATH=/etc/drivex/keys/jwt_rs256.key
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
    volumes:
      - ../infra/docker/keys:/etc/drivex/keys:ro
    networks:
      - drivex-internal
      - drivex-storage
    ulimits:
      nofile:
        soft: 65535
        hard: 65535
    deploy:
      resources:
        limits:
          cpus: "4.0"
          memory: 4096M
        reservations:
          cpus: "1.0"
          memory: 1024M
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://127.0.0.1:8080/health || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 10s
    depends_on:
      mysql-primary:
        condition: service_healthy
      redis:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
      minio:
        condition: service_healthy

  # ----------------------------------------------------------------------------
  # 3. AI / Search Gateway: Python FastAPI Service
  # ----------------------------------------------------------------------------
  ml-workers-api:
    build:
      context: ../ml-workers
      dockerfile: ../infra/docker/Dockerfile.ml-workers
    image: drivex/ml-workers:1.0.0-prod
    container_name: drivex-ml-api
    command: ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "4", "--log-config", "log_conf.json"]
    restart: unless-stopped
    environment:
      - APP_ENV=production
      - LOG_LEVEL=info
      - QDRANT_HOST=qdrant
      - QDRANT_PORT=6333
      - QDRANT_GRPC_PORT=6334
      - QDRANT_API_KEY=DriveX_Qdrant_Key_2026!
      - REDIS_URL=redis://:DriveX_Redis_Passwd_2026!@redis:6379/1
      - MYSQL_HOST=mysql-replica
      - MYSQL_PORT=3306
      - MYSQL_USER=drivex_app
      - MYSQL_PASSWORD=DriveX_Secure_Passwd_2026!
      - MYSQL_DATABASE=drivex
      - EMBEDDING_MODEL=BAAI/bge-large-en-v1.5
      - RERANKER_MODEL=BAAI/bge-reranker-large
      - TRANSFORMERS_CACHE=/cache/models
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
    volumes:
      - model_cache:/cache/models
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "4.0"
          memory: 6144M
        reservations:
          cpus: "1.0"
          memory: 2048M
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://127.0.0.1:8001/health || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 15s
    depends_on:
      qdrant:
        condition: service_healthy
      redis:
        condition: service_healthy

  # ----------------------------------------------------------------------------
  # 4. Asynchronous Compute: Celery Worker Fleet
  # ----------------------------------------------------------------------------
  ml-workers-celery:
    build:
      context: ../ml-workers
      dockerfile: ../infra/docker/Dockerfile.ml-workers
    image: drivex/ml-workers:1.0.0-prod
    container_name: drivex-ml-celery
    command: >
      celery -A app.celery_app worker
      --loglevel=INFO
      --concurrency=4
      --prefetch-multiplier=1
      -Q drivex.file.ingest,drivex.file.ocr,drivex.file.embed,drivex.file.dedup
      -n worker_primary@%h
    restart: unless-stopped
    environment:
      - APP_ENV=production
      - RABBITMQ_URL=amqp://drivex_mq:DriveX_Rabbit_Passwd_2026!@rabbitmq:5672//
      - REDIS_URL=redis://:DriveX_Redis_Passwd_2026!@redis:6379/1
      - MINIO_ENDPOINT=http://minio:9000
      - MINIO_ACCESS_KEY=drivex_minio_admin
      - MINIO_SECRET_KEY=DriveX_MinIO_Secret_Key_2026!
      - MINIO_BUCKET=drivex-blobs
      - QDRANT_HOST=qdrant
      - QDRANT_PORT=6333
      - QDRANT_API_KEY=DriveX_Qdrant_Key_2026!
      - MYSQL_HOST=mysql-primary
      - MYSQL_PORT=3306
      - MYSQL_USER=drivex_app
      - MYSQL_PASSWORD=DriveX_Secure_Passwd_2026!
      - MYSQL_DATABASE=drivex
      - EMBEDDING_MODEL=BAAI/bge-large-en-v1.5
      - TRANSFORMERS_CACHE=/cache/models
      - TESSDATA_PREFIX=/usr/share/tesseract-ocr/5/tessdata
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
    volumes:
      - model_cache:/cache/models
    networks:
      - drivex-internal
      - drivex-storage
    deploy:
      resources:
        limits:
          cpus: "6.0"
          memory: 8192M
        reservations:
          cpus: "2.0"
          memory: 4096M
    healthcheck:
      test: ["CMD-SHELL", "celery -A app.celery_app inspect ping -d worker_primary@$$HOSTNAME || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 3
      start_period: 20s
    depends_on:
      rabbitmq:
        condition: service_healthy
      redis:
        condition: service_healthy
      minio:
        condition: service_healthy
      qdrant:
        condition: service_healthy

  # ----------------------------------------------------------------------------
  # 5. Relational Primary: MySQL 8.0 Primary
  # ----------------------------------------------------------------------------
  mysql-primary:
    image: mysql:8.0
    container_name: drivex-mysql-primary
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: Root_SuperSecret_Password_2026!
      MYSQL_DATABASE: drivex
      MYSQL_USER: drivex_app
      MYSQL_PASSWORD: DriveX_Secure_Passwd_2026!
    command: >
      --default-authentication-plugin=mysql_native_password
      --server-id=101
      --log-bin=mysql-bin
      --binlog-format=ROW
      --binlog-do-db=drivex
      --gtid-mode=ON
      --enforce-gtid-consistency=ON
      --max-connections=1000
      --innodb-buffer-pool-size=4G
      --innodb-log-file-size=512M
      --innodb-flush-log-at-trx-commit=1
      --innodb-flush-method=O_DIRECT
      --character-set-server=utf8mb4
      --collation-server=utf8mb4_unicode_ci
    volumes:
      - mysql_primary_data:/var/lib/mysql
      - ../db/schema.sql:/docker-entrypoint-initdb.d/01_schema.sql:ro
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "4.0"
          memory: 6144M
        reservations:
          cpus: "1.0"
          memory: 4096M
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "drivex_app", "-pDriveX_Secure_Passwd_2026!"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 15s

  # ----------------------------------------------------------------------------
  # 6. Relational Read Replica: MySQL 8.0 Replica
  # ----------------------------------------------------------------------------
  mysql-replica:
    image: mysql:8.0
    container_name: drivex-mysql-replica
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: Root_SuperSecret_Password_2026!
    command: >
      --default-authentication-plugin=mysql_native_password
      --server-id=102
      --log-bin=mysql-bin
      --binlog-format=ROW
      --binlog-do-db=drivex
      --gtid-mode=ON
      --enforce-gtid-consistency=ON
      --read-only=ON
      --super-read-only=ON
      --max-connections=1000
      --innodb-buffer-pool-size=2G
      --character-set-server=utf8mb4
      --collation-server=utf8mb4_unicode_ci
    volumes:
      - mysql_replica_data:/var/lib/mysql
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 4096M
        reservations:
          cpus: "0.5"
          memory: 2048M
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-pRoot_SuperSecret_Password_2026!"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 15s
    depends_on:
      mysql-primary:
        condition: service_healthy

  # ----------------------------------------------------------------------------
  # 7. In-Memory Cache & Session Manager: Redis 7.2
  # ----------------------------------------------------------------------------
  redis:
    image: redis:7.2-alpine
    container_name: drivex-redis
    restart: unless-stopped
    command: >
      redis-server
      --requirepass DriveX_Redis_Passwd_2026!
      --appendonly yes
      --appendfsync everysec
      --maxmemory 2147483648
      --maxmemory-policy allkeys-lru
      --tcp-backlog 4096
      --databases 16
    volumes:
      - redis_data:/data
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 3072M
        reservations:
          cpus: "0.5"
          memory: 1024M
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "DriveX_Redis_Passwd_2026!", "ping"]
      interval: 5s
      timeout: 2s
      retries: 5
      start_period: 5s

  # ----------------------------------------------------------------------------
  # 8. S3 Object Storage Data Plane: MinIO
  # ----------------------------------------------------------------------------
  minio:
    image: minio/minio:RELEASE.2024-05-10T01-41-38Z
    container_name: drivex-minio
    restart: unless-stopped
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: drivex_minio_admin
      MINIO_ROOT_PASSWORD: DriveX_MinIO_Secret_Key_2026!
      MINIO_BROWSER: "on"
      MINIO_PROMETHEUS_AUTH_TYPE: public
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data
    networks:
      - drivex-public
      - drivex-storage
    deploy:
      resources:
        limits:
          cpus: "4.0"
          memory: 4096M
        reservations:
          cpus: "1.0"
          memory: 1024M
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://127.0.0.1:9000/minio/health/live || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 5
      start_period: 10s

  # ----------------------------------------------------------------------------
  # 9. Asynchronous Message Broker: RabbitMQ 3.13
  # ----------------------------------------------------------------------------
  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    container_name: drivex-rabbitmq
    restart: unless-stopped
    environment:
      RABBITMQ_DEFAULT_USER: drivex_mq
      RABBITMQ_DEFAULT_PASS: DriveX_Rabbit_Passwd_2026!
      RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: "+P 1048576"
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 2048M
        reservations:
          cpus: "0.5"
          memory: 512M
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s

  # ----------------------------------------------------------------------------
  # 10. High-Dimensional Vector Search Engine: Qdrant
  # ----------------------------------------------------------------------------
  qdrant:
    image: qdrant/qdrant:v1.9.2
    container_name: drivex-qdrant
    restart: unless-stopped
    environment:
      QDRANT__SERVICE__API_KEY: DriveX_Qdrant_Key_2026!
      QDRANT__STORAGE__PERFORMANCE__MAX_SEARCH_THREADS: 4
      QDRANT__STORAGE__ON_DISK_PAYLOAD: "true"
      QDRANT__TELEMETRY_DISABLED: "true"
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "4.0"
          memory: 4096M
        reservations:
          cpus: "1.0"
          memory: 1024M
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://127.0.0.1:6333/readyz || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 10s

  # ----------------------------------------------------------------------------
  # 11. Distributed Telemetry Ingestion: OpenTelemetry Collector
  # ----------------------------------------------------------------------------
  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.99.0
    container_name: drivex-otel-collector
    restart: unless-stopped
    command: ["--config=/etc/otelcol-contrib/config.yaml"]
    volumes:
      - ../infra/docker/otel/otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml:ro
    ports:
      - "4317:4317" # OTLP gRPC
      - "4318:4318" # OTLP HTTP
      - "8889:8889" # Prometheus metrics exporter
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "1.0"
          memory: 1024M
        reservations:
          cpus: "0.2"
          memory: 256M
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://127.0.0.1:13133/ || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 5s

  # ----------------------------------------------------------------------------
  # 12. Operational Metrics Database: Prometheus
  # ----------------------------------------------------------------------------
  prometheus:
    image: prom/prometheus:v2.52.0
    container_name: drivex-prometheus
    restart: unless-stopped
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=30d
      - --web.enable-lifecycle
      - --web.console.libraries=/usr/share/prometheus/console_libraries
      - --web.console.templates=/usr/share/prometheus/consoles
    volumes:
      - ../infra/docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    networks:
      - drivex-internal
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 2048M
        reservations:
          cpus: "0.5"
          memory: 512M
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://127.0.0.1:9090/-/healthy || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 5s

  # ----------------------------------------------------------------------------
  # 13. Observability Visualization: Grafana
  # ----------------------------------------------------------------------------
  grafana:
    image: grafana/grafana:10.4.2
    container_name: drivex-grafana
    restart: unless-stopped
    environment:
      - GF_SECURITY_ADMIN_USER=drivex_admin
      - GF_SECURITY_ADMIN_PASSWORD=DriveX_Grafana_Secure_2026!
      - GF_USERS_ALLOW_SIGN_UP=false
      - GF_SERVER_ROOT_URL=https://monitor.drivex.example.com
      - GF_INSTALL_PLUGINS=grafana-piechart-panel
    ports:
      - "3000:3000"
    volumes:
      - ../infra/docker/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - ../infra/docker/grafana/provisioning:/etc/grafana/provisioning:ro
      - grafana_data:/var/lib/grafana
    networks:
      - drivex-internal
      - drivex-public
    deploy:
      resources:
        limits:
          cpus: "1.0"
          memory: 1024M
        reservations:
          cpus: "0.2"
          memory: 256M
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://127.0.0.1:3000/api/health || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 10s
    depends_on:
      prometheus:
        condition: service_healthy
```

---

### 2.2 Production Kubernetes Manifest Architecture (`infra/k8s/`)

In production enterprise clusters, DriveX is deployed within a dedicated namespace (`drivex`) governed by Kubernetes declarative primitives, automated pod autoscaling, PersistentVolumeClaims, Headless Services for stateful network stability, and cert-manager automated TLS.

```
+---------------------------------------------------------------------------------------------------+
|                                  KUBERNETES DEPLOYMENT TOPOLOGY                                    |
+---------------------------------------------------------------------------------------------------+
|  [Ingress Controller (Nginx)] <--- (TLS cert-manager / Let's Encrypt)                              |
|           |                                                                                       |
|           +---> drivex-frontend-svc (ClusterIP:80)                                                |
|           +---> drivex-api-svc (ClusterIP:8080) -------> [HPA: CPU 70%, 3-10 Pods]               |
|           +---> drivex-ml-api-svc (ClusterIP:8001) ----> [HPA: CPU 75%, 2-6 Pods]                |
|                                                                                                   |
|  [Async Event Bus]                                                                                |
|    RabbitMQ Cluster (StatefulSet: 3 nodes) <--- AMQP                                             |
|           ^                                                                                       |
|           | (Queue depth metric: drivex.file.ingest target 50/pod)                               |
|    drivex-ml-workers-celery (Deployment) <-------- [KEDA ScaledObject: 2-20 Pods]                |
|                                                                                                   |
|  [Stateful Persistence Plane (volumeClaimTemplates - StorageClass: fast-nvme)]                    |
|    - mysql-primary-0 (StatefulSet, Primary)                                                       |
|    - mysql-replica-0 (StatefulSet, Read-Only Replica)                                             |
|    - redis-cluster (StatefulSet: 3 masters, 3 replicas)                                           |
|    - minio-distributed (StatefulSet: 4 nodes, 16 PVC drives, Erasure Coding N/2)                 |
|    - qdrant-cluster (StatefulSet: 2 nodes, Replicated Shards)                                     |
+---------------------------------------------------------------------------------------------------+
```

#### 2.2.1 Namespace, ConfigMaps & Secrets Management

Configuration parameters are partitioned into environment-agnostic ConfigMaps and encrypted Secrets (managed via SealedSecrets or HashiCorp Vault ExternalSecrets):

```yaml
# infra/k8s/00-namespace-config.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: drivex
  labels:
    app.kubernetes.io/name: drivex
    pod-security.kubernetes.io/enforce: baseline
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: drivex-config
  namespace: drivex
data:
  APP_ENV: "production"
  LOG_LEVEL: "INFO"
  DB_HOST: "mysql-primary-0.mysql-headless.drivex.svc.cluster.local"
  DB_PORT: "3306"
  DB_NAME: "drivex"
  DB_REPLICA_HOST: "mysql-replica-0.mysql-headless.drivex.svc.cluster.local"
  REDIS_HOST: "redis-master.drivex.svc.cluster.local"
  REDIS_PORT: "6379"
  MINIO_ENDPOINT: "http://minio-hl.drivex.svc.cluster.local:9000"
  MINIO_BUCKET: "drivex-blobs"
  RABBITMQ_HOST: "rabbitmq-headless.drivex.svc.cluster.local"
  QDRANT_HOST: "qdrant-headless.drivex.svc.cluster.local"
  QDRANT_PORT: "6333"
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector.drivex.svc.cluster.local:4317"
---
apiVersion: v1
kind: Secret
metadata:
  name: drivex-secrets
  namespace: drivex
type: Opaque
stringData:
  DB_PASSWORD: "DriveX_Secure_Passwd_2026!"
  REDIS_PASSWORD: "DriveX_Redis_Passwd_2026!"
  MINIO_ACCESS_KEY: "drivex_minio_admin"
  MINIO_SECRET_KEY: "DriveX_MinIO_Secret_Key_2026!"
  RABBITMQ_PASSWORD: "DriveX_Rabbit_Passwd_2026!"
  QDRANT_API_KEY: "DriveX_Qdrant_Key_2026!"
  JWT_PRIVATE_KEY: |
    -----BEGIN RSA PRIVATE KEY-----
    MIIEowIBAAKCAQEA0Y3t... [Production RS256 Private Key]
    -----END RSA PRIVATE KEY-----
  JWT_PUBLIC_KEY: |
    -----BEGIN PUBLIC KEY-----
    MIIBIjANBgkqhkiG9w0B... [Production RS256 Public Key]
    -----END PUBLIC KEY-----
```

#### 2.2.2 Control Plane Stateless Deployment & Horizontal Pod Autoscaler (HPA)

```yaml
# infra/k8s/10-api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: drivex-api
  namespace: drivex
  labels:
    app: drivex-api
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: drivex-api
  template:
    metadata:
      labels:
        app: drivex-api
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/path: "/metrics"
        prometheus.io/port: "8080"
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchExpressions:
                    - key: app
                      operator: In
                      values: ["drivex-api"]
                topologyKey: "kubernetes.io/hostname"
      containers:
        - name: drogon-api
          image: drivex/drogon-api:1.0.0-prod
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
              name: http
          envFrom:
            - configMapRef:
                name: drivex-config
            - secretRef:
                name: drivex-secrets
          resources:
            requests:
              cpu: "1000m"
              memory: "1024Mi"
            limits:
              cpu: "4000m"
              memory: "4096Mi"
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 15
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 2
            successThreshold: 1
            failureThreshold: 2
          securityContext:
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 10001
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
---
apiVersion: v1
kind: Service
metadata:
  name: drivex-api
  namespace: drivex
spec:
  type: ClusterIP
  selector:
    app: drivex-api
  ports:
    - name: http
      port: 8080
      targetPort: 8080
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: drivex-api-hpa
  namespace: drivex
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: drivex-api
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
```

#### 2.2.3 Asynchronous Celery Worker Fleet with KEDA Queue Autoscaling

To handle sudden bursts of file uploads without unbounded memory consumption or task queue latency, worker pods autoscale from 2 to 20 replicas using KEDA (`ScaledObject`) directly observing the RabbitMQ `drivex.file.ingest` queue depth:

```yaml
# infra/k8s/20-celery-keda.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: drivex-ml-workers-celery
  namespace: drivex
  labels:
    app: drivex-ml-workers-celery
spec:
  replicas: 2
  selector:
    matchLabels:
      app: drivex-ml-workers-celery
  template:
    metadata:
      labels:
        app: drivex-ml-workers-celery
    spec:
      containers:
        - name: celery-worker
          image: drivex/ml-workers:1.0.0-prod
          command:
            - celery
            - -A
            - app.celery_app
            - worker
            - --loglevel=INFO
            - --concurrency=4
            - --prefetch-multiplier=1
            - -Q
            - drivex.file.ingest,drivex.file.ocr,drivex.file.embed,drivex.file.dedup
          envFrom:
            - configMapRef:
                name: drivex-config
            - secretRef:
                name: drivex-secrets
          resources:
            requests:
              cpu: "2000m"
              memory: "4096Mi"
            limits:
              cpu: "6000m"
              memory: "8192Mi"
          volumeMounts:
            - name: model-cache
              mountPath: /cache/models
      volumes:
        - name: model-cache
          persistentVolumeClaim:
            claimName: model-cache-pvc
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: celery-queue-scaler
  namespace: drivex
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: drivex-ml-workers-celery
  minReplicaCount: 2
  maxReplicaCount: 20
  pollingInterval: 10
  cooldownPeriod: 300
  triggers:
    - type: rabbitmq
      metadata:
        protocol: amqp
        queueName: drivex.file.ingest
        mode: QueueLength
        value: "50"
      authenticationRef:
        name: keda-rabbitmq-auth
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: keda-rabbitmq-auth
  namespace: drivex
spec:
  secretTargetRef:
    - parameter: host
      name: drivex-secrets
      key: RABBITMQ_URL
```

#### 2.2.4 StatefulSet Relational Architecture (`mysql-primary.yaml`)

```yaml
# infra/k8s/30-mysql-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql-primary
  namespace: drivex
spec:
  serviceName: mysql-headless
  replicas: 1
  selector:
    matchLabels:
      app: mysql-primary
  template:
    metadata:
      labels:
        app: mysql-primary
    spec:
      containers:
        - name: mysql
          image: mysql:8.0
          args:
            - --default-authentication-plugin=mysql_native_password
            - --server-id=101
            - --log-bin=mysql-bin
            - --binlog-format=ROW
            - --gtid-mode=ON
            - --enforce-gtid-consistency=ON
            - --innodb-buffer-pool-size=4G
            - --max-connections=1000
          ports:
            - containerPort: 3306
              name: mysql
          env:
            - name: MYSQL_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: drivex-secrets
                  key: DB_ROOT_PASSWORD
            - name: MYSQL_DATABASE
              value: "drivex"
            - name: MYSQL_USER
              value: "drivex_app"
            - name: MYSQL_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: drivex-secrets
                  key: DB_PASSWORD
          resources:
            requests:
              cpu: "2000m"
              memory: "4096Mi"
            limits:
              cpu: "4000m"
              memory: "6144Mi"
          volumeMounts:
            - name: mysql-data
              mountPath: /var/lib/mysql
  volumeClaimTemplates:
    - metadata:
        name: mysql-data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: "fast-nvme"
        resources:
          requests:
            storage: 200Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mysql-headless
  namespace: drivex
spec:
  clusterIP: None
  selector:
    app: mysql-primary
  ports:
    - port: 3306
      name: mysql
```

#### 2.2.5 Production Nginx Ingress Architecture with cert-manager

```yaml
# infra/k8s/40-nginx-ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: drivex-ingress
  namespace: drivex
  annotations:
    kubernetes.io/ingress.class: "nginx"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/ssl-protocols: "TLSv1.3 TLSv1.2"
    nginx.ingress.kubernetes.io/ssl-ciphers: "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384"
    nginx.ingress.kubernetes.io/proxy-body-size: "10m"
    nginx.ingress.kubernetes.io/proxy-connect-timeout: "15"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "60"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "60"
    nginx.ingress.kubernetes.io/configuration-snippet: |
      more_set_headers "Strict-Transport-Security: max-age=63072000; includeSubDomains; preload";
      more_set_headers "X-Frame-Options: SAMEORIGIN";
      more_set_headers "X-Content-Type-Options: nosniff";
spec:
  tls:
    - hosts:
        - drivex.example.com
      secretName: drivex-tls-cert
  rules:
    - host: drivex.example.com
      http:
        paths:
          # Server-Sent Events (SSE) AI Streaming endpoint: Buffering MUST be OFF
          - path: /api/v1/chat
            pathType: Exact
            backend:
              service:
                name: drivex-ml-api
                port:
                  number: 8001
          # Semantic Search API
          - path: /api/v1/search
            pathType: Exact
            backend:
              service:
                name: drivex-ml-api
                port:
                  number: 8001
          # Control Plane Drogon C++ REST API
          - path: /api/v1/
            pathType: Prefix
            backend:
              service:
                name: drivex-api
                port:
                  number: 8080
          # Frontend Hypermedia Static & UI Templates
          - path: /
            pathType: Prefix
            backend:
              service:
                name: drivex-frontend
                port:
                  number: 80
```

---

## 3. Section 2: Nginx Reverse Proxy, TLS, Rate Limiting & Storage Quotas

### 3.1 Nginx Reverse Proxy Architecture & Routing Rules

The edge proxy sits at the perimeter of the DriveX network fabric. It is responsible for terminating external client TCP/TLS connections, enforcing security headers, executing rate limiting algorithms before requests touch internal microservices, and disabling buffering for long-lived Server-Sent Events (SSE).

#### Complete Production Nginx Configuration (`infra/docker/nginx/nginx.conf`)

```nginx
# ==============================================================================
# DriveX Edge Reverse Proxy Specification
# Standard: TLS 1.3, Leaky Bucket / Token Bucket Rate Limiting, Zero SSE Buffering
# ==============================================================================

user nginx;
worker_processes auto;
worker_rlimit_nofile 65535;
error_log /var/log/nginx/error.log warn;
pid /var/run/nginx.pid;

events {
    worker_connections 16384;
    use epoll;
    multi_accept on;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    # Performance Tuning
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    keepalive_requests 10000;
    types_hash_max_size 2048;
    server_tokens off;

    # Client Buffering Limits
    client_body_buffer_size 128k;
    client_max_body_size 10M; # Disallow large uploads via proxy; client must use S3 SigV4
    client_header_buffer_size 4k;
    large_client_header_buffers 4 16k;

    # Structured JSON Log Format for Correlation
    log_format json_analytics escape=json '{'
        '"timestamp":"$time_iso8601",'
        '"remote_addr":"$remote_addr",'
        '"request_id":"$request_id",'
        '"trace_id":"$http_traceparent",'
        '"user_id":"$jwt_claim_sub",'
        '"request_method":"$request_method",'
        '"request_uri":"$request_uri",'
        '"status":$status,'
        '"body_bytes_sent":$body_bytes_sent,'
        '"request_time":$request_time,'
        '"upstream_response_time":"$upstream_response_time",'
        '"upstream_addr":"$upstream_addr",'
        '"http_referrer":"$http_referer",'
        '"http_user_agent":"$http_user_agent"'
    '}';

    access_log /var/log/nginx/access_json.log json_analytics;

    # --------------------------------------------------------------------------
    # SSL / TLS 1.3 Cryptographic Hardening
    # --------------------------------------------------------------------------
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers on;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_ecdh_curve X25519:prime256v1:secp384r1;

    ssl_session_cache shared:SSL:50m;
    ssl_session_timeout 1d;
    ssl_session_tickets off;

    # OCSP Stapling
    ssl_stapling on;
    ssl_stapling_verify on;
    resolver 1.1.1.1 8.8.8.8 valid=300s;
    resolver_timeout 5s;

    # --------------------------------------------------------------------------
    # Rate Limiting Dual-Tier Engine
    # --------------------------------------------------------------------------
    # 1. IP-Based Leaky Bucket (Per-IP DDoS & Scraping Protection: 100 req/s, burst 50)
    limit_req_zone $binary_remote_addr zone=ip_leaky_bucket:30m rate=100r/s;

    # 2. Auth Route Zone (Brute-Force Login Protection: 5 req/min, burst 3)
    limit_req_zone $binary_remote_addr zone=auth_route_limit:10m rate=5r/m;

    # Note on Authenticated User Rate Limiting:
    # Authenticated per-user token-bucket rate limiting (50 req/s, burst 20) is explicitly
    # delegated to Drogon C++ JwtAuthFilter via Redis sliding-window counters keyed on
    # verified user_id (rate:user:<user_id>). This avoids Nginx memory zone exhaustion
    # from 800-byte RS256 JWT strings and prevents header manipulation bypass attacks.

    limit_req_status 429;

    # --------------------------------------------------------------------------
    # Upstream Connection Pools (Keepalive Enabled)
    # --------------------------------------------------------------------------
    upstream drogon_api_upstream {
        server drogon-api:8080 max_fails=3 fail_timeout=10s;
        keepalive 128;
    }

    upstream ml_api_upstream {
        server ml-workers-api:8001 max_fails=3 fail_timeout=10s;
        keepalive 64;
    }

    # --------------------------------------------------------------------------
    # Virtual Host: HTTP to HTTPS Redirect
    # --------------------------------------------------------------------------
    server {
        listen 80;
        listen [::]:80;
        server_name drivex.example.com storage.drivex.example.com;
        location /.well-known/acme-challenge/ {
            root /var/www/certbot;
        }
        location / {
            return 301 https://$host$request_uri;
        }
    }

    # --------------------------------------------------------------------------
    # Virtual Host: Primary Application Gateway (HTTPS)
    # --------------------------------------------------------------------------
    server {
        listen 443 ssl http2;
        listen [::]:443 ssl http2;
        server_name drivex.example.com;

        ssl_certificate /etc/nginx/ssl/fullchain.pem;
        ssl_certificate_key /etc/nginx/ssl/privkey.pem;

        # Defense-in-Depth Security Headers
        add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload" always;
        add_header X-Frame-Options "SAMEORIGIN" always;
        add_header X-Content-Type-Options "nosniff" always;
        add_header X-XSS-Protection "1; mode=block" always;
        add_header Referrer-Policy "strict-origin-when-cross-origin" always;
        add_header Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob: https:; connect-src 'self' https://storage.drivex.example.com; font-src 'self' data:;" always;

        # Custom JSON Error Model for Rate Limiting
        error_page 429 /429.json;
        location = /429.json {
            default_type application/problem+json;
            return 429 '{"type":"https://api.drivex.dev/errors/rate-limit-exceeded","title":"Too Many Requests","status":429,"detail":"Rate limit threshold exceeded. Please throttle your request cadence."}';
        }

        # Health Check
        location = /healthz {
            access_log off;
            default_type text/plain;
            return 200 "OK\n";
        }

        # --- Route 1: Authentication Endpoints (Strict Brute-Force Rate Limit) ---
        location /api/v1/auth/ {
            limit_req zone=auth_route_limit burst=3 nodelay;
            limit_req zone=ip_leaky_bucket burst=50 nodelay;

            proxy_pass http://drogon_api_upstream;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Request-ID $request_id;
        }

        # --- Route 2: Conversational RAG SSE Streaming (Critical SSE Tuning) ---
        location = /api/v1/chat {
            limit_req zone=ip_leaky_bucket burst=20 nodelay;

            proxy_pass http://ml_api_upstream/chat;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Server-Sent Events (SSE) Buffering Overrides
            proxy_buffering off;
            proxy_cache off;
            proxy_read_timeout 3600s;
            proxy_send_timeout 3600s;
            chunked_transfer_encoding on;
            tcp_nodelay on;
        }

        # --- Route 3: Semantic Search Endpoint ---
        location = /api/v1/search {
            limit_req zone=user_token_bucket burst=20 nodelay;
            limit_req zone=ip_leaky_bucket burst=50 nodelay;

            proxy_pass http://ml_api_upstream/search;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }

        # --- Route 4: Core Drogon Control Plane REST APIs ---
        location /api/v1/ {
            limit_req zone=user_token_bucket burst=20 nodelay;
            limit_req zone=ip_leaky_bucket burst=50 nodelay;

            proxy_pass http://drogon_api_upstream;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Request-ID $request_id;

            # Direct file payload upload rejected here: enforced via 10M cap
            client_max_body_size 10M;
        }

        # --- Route 5: Static Assets & HTMX UI Templates ---
        location / {
            root /usr/share/nginx/html/templates;
            try_files $uri $uri/ /index.html;
            limit_req zone=ip_leaky_bucket burst=50 nodelay;

            location ~* \.(?:css|js|jpg|jpeg|gif|png|ico|svg|woff|woff2)$ {
                root /usr/share/nginx/html/static;
                expires 30d;
                add_header Cache-Control "public, no-transform";
            }
        }
    }
}
```

---

### 3.2 Dual-Tier Rate Limiting Architecture

DriveX employs two complementary rate-limiting algorithms to ensure both network perimeter protection and multi-tenant fairness:

```
                      [Inbound Client Request]
                                 |
                                 v
                 +-------------------------------+
                 |  Tier 1: Edge IP Leaky Bucket |
                 |  - Layer: Nginx Edge Proxy    |
                 |  - Capacity: 50 requests      |
                 |  - Leak Rate: 100 req/sec     |
                 |  - Target: Volumetric DDoS    |
                 +---------------+---------------+
                                 | Allowed
                                 v
                 +-------------------------------+
                 |  Tier 2: User Token Bucket    |
                 |  - Layer: Drogon C++ & Redis  |
                 |  - Key: rate:user:<user_id>   |
                 |  - Capacity: 20 tokens        |
                 |  - Refill: 50 tokens/sec      |
                 |  - Target: Tenant Throttling  |
                 +---------------+---------------+
                                 | Allowed
                                 v
                 +-------------------------------+
                 | Application Core / ML Gateway |
                 +-------------------------------+
```

#### Algorithm Specifications

1. **Tier 1: IP-Based Leaky Bucket (`ip_leaky_bucket`)**:
   - **Execution Layer**: Nginx Reverse Proxy (`infra/docker/nginx/nginx.conf`).
   - **Key**: `$binary_remote_addr` (IPv4 consumes 4 bytes, IPv6 consumes 16 bytes in Nginx shared memory).
   - **Zone Size**: 30 MB (allocates tracking slots for approximately 480,000 concurrent distinct client IP addresses).
   - **Leak Rate**: 100 requests per second (`rate=100r/s`).
   - **Burst Capacity**: 50 requests (`burst=50 nodelay`).
   - **Behavior**: Absorbs high-frequency bursts from modern multi-tab browser sessions without queuing latency; drops abusive flood traffic immediately with HTTP 429.

2. **Tier 2: Authenticated User Token Bucket (`rate:user:<user_id>`)**:
   - **Execution Layer**: Drogon C++ `JwtAuthFilter` and FastAPI ML Gateway.
   - **State Store**: Redis 7.x in-memory store utilizing atomic sliding-window token counters (`rate:user:<user_id>`).
   - **Key**: Cryptographically verified `user_id` claim parsed from untampered RS256 JWT payloads (never raw header strings).
   - **Refill Rate**: 50 tokens per second.
   - **Burst Bucket**: 20 tokens.
   - **Security & Memory Rationale**: Delegating authenticated user throttling from Nginx to Drogon/Redis prevents Nginx memory zone exhaustion (where 800-byte RS256 JWT tokens would exhaust a 30MB zone with only ~35,000 active sessions) and eliminates authorization header forgery bypass attacks.

3. **HTTP 429 Problem Details Contract**:
   When rate limits are triggered, Nginx emits an RFC 7807 compliant JSON response accompanied by a standard `Retry-After` header:
   ```http
   HTTP/1.1 429 Too Many Requests
   Content-Type: application/problem+json
   Retry-After: 1
   X-RateLimit-Limit: 100
   X-RateLimit-Remaining: 0

   {
     "type": "https://api.drivex.dev/errors/rate-limit-exceeded",
     "title": "Too Many Requests",
     "status": 429,
     "detail": "Rate limit threshold exceeded. Please throttle your request cadence."
   }
   ```

---

### 3.3 Storage Quota Enforcement Architecture (Two-Phase Reservation)

Storage quota accounting presents a critical concurrency hazard in cloud storage: if quota is evaluated solely at the start of an upload, a malicious user could initiate 100 parallel 10GB uploads simultaneously, exceeding their 15GB quota 66-fold before metadata commits. Conversely, locking the MySQL `users` row for the entire duration of a 30-minute upload would serialize all account operations and trigger transaction timeouts.

DriveX solves this with a **Two-Phase Reservation Protocol**:

```
[Phase 1: Pre-Upload Reservation]
Client ---> POST /api/v1/files/upload-url 
            { requested_size_bytes, folder_id, content_hash } ---> Drogon C++ API
                                                                        |
                                                                        v
                                                          Redis: Atomic Check & Reserve
                                                          (Lua Script execution)
                                                          Key: user:{id}:reserved_bytes
                                                          TTL: 3600s
                                                                        |
       <--- 200 OK { upload_url, session_id } <-------------------------+

[Direct MinIO Data Stream]
Client ---> HTTP PUT (binary file stream) ---> MinIO S3 Object Storage
                                                   | (Direct client-to-storage)
                                                   v

[Phase 2: Post-Upload Commit & Reconciliation]
Client ---> POST /api/v1/files/upload-complete 
            { session_id, storage_key } ---> Drogon C++ API
                                                 |
                                                 v
                                         MinIO HeadObject Check
                                         (Validates existence, ETag & exact actual_size)
                                                 |
                                                 v
                                         MySQL Transaction:
                                         1. INSERT INTO files (...)
                                         2. INSERT INTO file_versions (...)
                                         3. UPDATE users SET storage_used_bytes = 
                                            storage_used_bytes + :actual_size
                                                 |
                                                 v
                                         Redis Atomic Reconciliation:
                                         DECRBY user:{id}:reserved_bytes :requested_size
                                         INCRBY user:{id}:used_bytes :actual_size
                                                 |
                                                 v
                                         Publish Event: drivex.events (RabbitMQ)
                                                 |
       <--- 200 OK { file_id, version_id } <----+
```

#### Production Redis Lua Reservation Script (`api/src/services/lua/reserve_quota.lua`)

```lua
-- KEYS[1]: User quota hash key (e.g. "user:1024:quota")
-- ARGV[1]: Requested file size in bytes
-- ARGV[2]: Default quota limit (e.g. 16106127360 = 15 GB)
-- ARGV[3]: Reservation TTL in seconds (e.g. 3600)

local quota_key = KEYS[1]
local requested = tonumber(ARGV[1])
local default_quota = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])

-- Retrieve current state from Redis Hash
local current_used = tonumber(redis.call('HGET', quota_key, 'used_bytes') or '0')
local reserved = tonumber(redis.call('HGET', quota_key, 'reserved_bytes') or '0')
local total_quota = tonumber(redis.call('HGET', quota_key, 'max_bytes') or default_quota)

-- Strict Quota Boundary Check
if (current_used + reserved + requested) > total_quota then
    -- Quota Exceeded: Return 0 with remaining available bytes
    local available = total_quota - (current_used + reserved)
    if available < 0 then available = 0 end
    return {0, tostring(available), tostring(total_quota)}
else
    -- Reservation Permitted: Increment reserved_bytes atomically
    local new_reserved = redis.call('HINCRBY', quota_key, 'reserved_bytes', requested)
    redis.call('EXPIRE', quota_key, ttl_seconds)
    return {1, tostring(new_reserved), tostring(total_quota)}
end
```

#### Production Redis Lua Release & Reconciliation Script (`api/src/services/lua/release_quota.lua`)

```lua
-- KEYS[1]: User quota hash key (e.g. "user:1024:quota")
-- ARGV[1]: Previously reserved size in bytes to decrement
-- ARGV[2]: Actual committed size in bytes to increment in used_bytes

local quota_key = KEYS[1]
local reserved_to_release = tonumber(ARGV[1])
local actual_committed = tonumber(ARGV[2])

-- Decrement reservation (ensure never drops below zero due to racing cleanup)
local current_reserved = tonumber(redis.call('HGET', quota_key, 'reserved_bytes') or '0')
local new_reserved = current_reserved - reserved_to_release
if new_reserved < 0 then new_reserved = 0 end
redis.call('HSET', quota_key, 'reserved_bytes', new_reserved)

-- Increment cached used_bytes
local new_used = redis.call('HINCRBY', quota_key, 'used_bytes', actual_committed)

return {1, tostring(new_reserved), tostring(new_used)}
```

#### Drift Reconciliation Worker (Celery Beat Task)

To prevent drift caused by network partitions, client crashes after URL generation, or abandoned uploads, a periodic Celery Beat task runs hourly:
1. Queries MySQL for all `users` records.
2. Sums `size_bytes` across active, non-trashed `file_versions` for each user.
3. Re-synchronizes `users.storage_used_bytes` in MySQL if drift $> 0.1\%$.
4. Flushes expired reservation keys (`user:{id}:reserved_bytes`) older than 3600 seconds in Redis.
5. Invokes MinIO `ListMultipartUploads` to abort orphaned upload sessions older than 24 hours.

---

## 4. Section 3: Full-Stack Observability Suite

### 4.1 OpenTelemetry Distributed Tracing Architecture

DriveX implements end-to-end distributed tracing conforming to the **W3C TraceContext** standard (`traceparent` header format: `00-{trace_id}-{span_id}-{trace_flags}`). Tracing spans cross network, language, and process boundaries from client browser requests through the Nginx edge, C++ Drogon control plane, RabbitMQ message attributes, and Python Celery/FastAPI workers.

```
+---------------------------------------------------------------------------------------------------+
|                              DISTRIBUTED TRACE LIFECYCLE TOPOLOGY                                 |
+---------------------------------------------------------------------------------------------------+
|  [Client Browser]                                                                                 |
|         | Inbound HTTP: traceparent (W3C Standard)                                                |
|         v                                                                                         |
|  [Nginx Edge] --------> Emits Access Span                                                         |
|         | Forwarded HTTP                                                                          |
|         v                                                                                         |
|  [Drogon C++ API] ----> Root Span: drogon.http.request (route, user_id)                          |
|         |                Child Span: mysql.query.insert_metadata                                  |
|         |                Child Span: minio.s3.head_object                                         |
|         |                                                                                         |
|         +---> AMQP Publish: drivex.events (Injects traceparent into message headers)              |
|                    |                                                                              |
|                    v                                                                              |
|         [RabbitMQ Topic Exchange]                                                                 |
|                    |                                                                              |
|                    v                                                                              |
|  [Celery Worker] -----> Extracts traceparent from AMQP header properties                          |
|                          Child Span: celery.task.ingest_file                                      |
|                          Child Span: pymupdf.extract_text                                         |
|                          Child Span: bge_large.generate_embeddings                                |
|                          Child Span: qdrant.upsert_points                                         |
|                                 |                                                                 |
|                                 v                                                                 |
|                  [OpenTelemetry Collector (Port 4317)]                                            |
|                                 |                                                                 |
|                                 +---> [Tempo / Jaeger Storage Backend]                            |
+---------------------------------------------------------------------------------------------------+
```

#### 4.1.1 Drogon C++ OpenTelemetry SDK Instrumentation

The C++ Drogon API initializes the OpenTelemetry C++ SDK on application startup:

```cpp
// api/src/middleware/OtelTracingFilter.cc
#include <drogon/HttpFilter.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;

class OtelTracingFilter : public drogon::HttpFilter<OtelTracingFilter> {
public:
    virtual void doFilter(const drogon::HttpRequestPtr &req,
                         drogon::FilterCallback &&fcb,
                         drogon::FilterChainCallback &&fccb) override {
        
        auto tracer = trace::Provider::GetTracerProvider()->GetTracer("drivex-drogon-api", "1.0.0");
        
        // Extract incoming W3C TraceContext from HTTP header
        std::string traceparent = req->getHeader("traceparent");
        trace::StartSpanOptions options;
        
        // Build Span
        nostd::shared_ptr<trace::Span> span = tracer->StartSpan(
            req->getMethodString() + " " + req->getPath(),
            {{"http.method", req->getMethodString()},
             {"http.url", req->getPath()},
             {"http.client_ip", req->getPeerAddr().toIp()}},
            options
        );

        auto scope = tracer->WithActiveSpan(span);

        // Continue filter chain
        fccb();

        // On response completion
        span->SetAttribute("http.status_code", 200);
        span->End();
    }
};
```

#### 4.1.2 AMQP Header Propagation (C++ Publisher to Python Consumer)

Before publishing upload confirmation events to RabbitMQ, Drogon injects the active W3C trace context into the AMQP message properties table:

```cpp
// AMQP Header Injection in Drogon
void publishUploadEvent(const UploadEvent& event, const trace::SpanContext& spanContext) {
    char traceparentBuffer[128];
    snprintf(traceparentBuffer, sizeof(traceparentBuffer),
             "00-%s-%s-01",
             spanContext.trace_id().ToHex().c_str(),
             spanContext.span_id().ToHex().c_str());

    AmqpClient::Table messageHeaders;
    messageHeaders.insert(AmqpClient::TableEntry("traceparent", std::string(traceparentBuffer)));
    
    AmqpClient::BasicMessage::ptr_t message = AmqpClient::BasicMessage::Create(event.toJson());
    message->HeaderTable(messageHeaders);
    message->DeliveryMode(AmqpClient::BasicMessage::dm_persistent);

    channel->BasicPublish("drivex.events", "file.uploaded." + event.mimeCategory, message);
}
```

In Python Celery workers, signal handlers extract the header and bind it to the execution context:

```python
# ml-workers/app/observability/tracing.py
from celery.signals import task_prerun, task_postrun
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

tracer = trace.get_tracer("drivex-celery-worker", "1.0.0")

@task_prerun.connect
def on_task_prerun(task_id, task, args, kwargs, **_):
    headers = getattr(task.request, "headers", {}) or {}
    carrier = {"traceparent": headers.get("traceparent", "")}
    ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
    
    span = tracer.start_span(
        name=f"celery.{task.name}",
        context=ctx,
        attributes={"celery.task_id": task_id, "celery.queue": task.request.delivery_info.get("routing_key")}
    )
    task.__otel_span = span

@task_postrun.connect
def on_task_postrun(task_id, task, **_):
    span = getattr(task, "__otel_span", None)
    if span:
        span.end()
```

---

### 4.2 Prometheus Metrics Instrumentation Catalogue

Prometheus scrapes metrics across all running microservices every 15 seconds. The table below documents the authoritative metrics catalogue:

| Metric Name | Type | Dimensions / Labels | Scrape Endpoint | Architectural Intent & SLA Threshold |
|---|---|---|---|---|
| `drivex_http_requests_total` | Counter | `method`, `route`, `status_code` | `drogon-api:8080/metrics` | API throughput tracking. Target: 0.00% 5xx errors under normal load. |
| `drivex_http_request_duration_seconds` | Histogram | `method`, `route` (le: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5) | `drogon-api:8080/metrics` | RED Latency. Target: Metadata CRUD p99 < 15ms (Stage 1), < 50ms (Stage 5). |
| `drivex_db_connections_active` | Gauge | `pool` (`primary`, `replica`) | `drogon-api:8080/metrics` | Active MySQL connection pool utilization. Alert if > 80% pool capacity. |
| `drivex_storage_quota_bytes` | Gauge | `user_id`, `tier` | `drogon-api:8080/metrics` | Total provisioned storage quota per user. |
| `drivex_storage_used_bytes` | Gauge | `user_id` | `drogon-api:8080/metrics` | Reconciled storage consumption per user in MySQL. |
| `drivex_storage_reserved_bytes` | Gauge | `user_id` | `drogon-api:8080/metrics` | Active in-flight upload reservations currently held in Redis. |
| `drivex_presigned_urls_issued_total` | Counter | `operation` (`upload_put`, `download_get`) | `drogon-api:8080/metrics` | Pre-signed URL generation velocity. |
| `drivex_rabbitmq_queue_messages_ready` | Gauge | `queue` (`ingest`, `ocr`, `embed`, `dedup`) | `rabbitmq:15672/metrics` | Unprocessed queue backlog. Triggers KEDA autoscaling if > 50 messages/pod. |
| `drivex_rabbitmq_queue_messages_unacked` | Gauge | `queue` | `rabbitmq:15672/metrics` | In-flight messages being processed by Celery worker pool. |
| `drivex_celery_task_duration_seconds` | Histogram | `task_name`, `status` | `ml-workers-celery/metrics` | Celery execution latency. Alert if p95 OCR duration > 120s. |
| `drivex_celery_tasks_failed_total` | Counter | `task_name`, `error_type` | `ml-workers-celery/metrics` | Task failure rate. Messages routing to DLQ. |
| `drivex_ocr_pages_processed_total` | Counter | `engine` (`tesseract`), `mime_type` | `ml-workers-celery/metrics` | Volume of optical character recognition pages scanned. |
| `drivex_qdrant_search_latency_seconds`| Histogram | `collection`, `filter_type` | `ml-workers-api:8001/metrics` | Vector search query latency. Target: p99 < 20ms. |
| `drivex_rag_first_token_duration_seconds`| Histogram | `model` (`deepseek`, `llama3`) | `ml-workers-api:8001/metrics` | Time-to-First-Token (TTFT) for SSE chat streaming. Target: p95 < 800ms. |
| `drivex_rag_tokens_generated_total` | Counter | `model` | `ml-workers-api:8001/metrics` | LLM token throughput tracking. |

---

### 4.3 Production Grafana Monitoring Dashboard Specifications

DriveX provisions three dedicated Grafana monitoring dashboards via JSON provisioning:

#### Dashboard 1: Platform Golden Signals & API Concurrency (`dashboards/golden_signals.json`)
- **Panel 1 (Traffic Volume)**: `sum(rate(drivex_http_requests_total[1m])) by (route)` — Displays aggregate requests per second (RPS) partitioned by functional route category.
- **Panel 2 (Latency Percentiles)**: `histogram_quantile(0.99, sum(rate(drivex_http_request_duration_seconds_bucket[1m])) by (le))` and `histogram_quantile(0.50, ...)` — Tracks p50, p90, and p99 response times for C++ Drogon routes.
- **Panel 3 (Error Budget Rate)**: `sum(rate(drivex_http_requests_total{status_code=~"5.."}[1m])) / sum(rate(drivex_http_requests_total[1m])) * 100` — Displays 5xx error budget consumption against the 99.9% availability SLA.
- **Panel 4 (Rate Limiting Pressure)**: `sum(rate(drivex_http_requests_total{status_code="429"}[1m]))` — Visualizes abusive traffic throttling from leaky bucket and token bucket limits.
- **Panel 5 (Database Pool Saturation)**: `drivex_db_connections_active / 100 * 100` — Gauge indicating thread connection saturation for primary and replica MySQL pools.

#### Dashboard 2: Object Storage Transfer & Quota Saturation (`dashboards/storage_transfer.json`)
- **Panel 1 (Transfer Bandwidth)**: `sum(rate(minio_s3_traffic_sent_bytes[1m]))` and `sum(rate(minio_s3_traffic_received_bytes[1m]))` — Real-time direct client-to-MinIO streaming bandwidth (MB/s).
- **Panel 2 (Pre-Signed Token Issuance Rate)**: `sum(rate(drivex_presigned_urls_issued_total[1m])) by (operation)` — Tracks velocity of upload PUT versus download GET negotiations.
- **Panel 3 (User Quota Utilization Distribution)**: `drivex_storage_used_bytes / drivex_storage_quota_bytes * 100` — Heatmap displaying distribution of storage quota consumption across tenant cohorts.
- **Panel 4 (In-Flight Upload Reservations)**: `sum(drivex_storage_reserved_bytes)` — Total bytes currently reserved in Redis awaiting upload confirmation or TTL expiry.

#### Dashboard 3: AI Pipeline Analytics & Vector Search (`dashboards/ai_pipeline.json`)
- **Panel 1 (RabbitMQ Queue Backlog)**: `drivex_rabbitmq_queue_messages_ready` — Displays message depth across `drivex.file.ingest`, `ocr`, `embed`, and `dedup` queues.
- **Panel 2 (Celery Processing Latency)**: `histogram_quantile(0.95, sum(rate(drivex_celery_task_duration_seconds_bucket[5m])) by (le, task_name))` — p95 runtime across document extraction, chunking, and embedding.
- **Panel 3 (Qdrant Search Latency)**: `histogram_quantile(0.99, sum(rate(drivex_qdrant_search_latency_seconds_bucket[1m])) by (le))` — High-dimensional vector query latency.
- **Panel 4 (RAG Conversational Stream Latency)**: `histogram_quantile(0.95, sum(rate(drivex_rag_first_token_duration_seconds_bucket[1m])) by (le))` — Time to First Token (TTFT) for streaming responses over SSE.
- **Panel 5 (Dead-Letter Queue Alerts)**: `increase(drivex_rabbitmq_queue_messages_ready{queue="drivex.dlq"}[1m])` — Critical indicator of failed tasks requiring operator intervention.

---

### 4.4 Structured JSON Logging Standard

Every service emits structured, parseable JSON logs to standard output adhering to the unified schema:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DriveXLogRecord",
  "type": "object",
  "required": ["timestamp", "level", "service", "trace_id", "span_id", "event", "message"],
  "properties": {
    "timestamp": { "type": "string", "format": "date-time" },
    "level": { "type": "string", "enum": ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"] },
    "service": { "type": "string", "enum": ["drogon-api", "ml-workers-celery", "ml-workers-api", "frontend-nginx"] },
    "trace_id": { "type": "string" },
    "span_id": { "type": "string" },
    "user_id": { "type": ["integer", "null"] },
    "file_id": { "type": ["integer", "null"] },
    "event": { "type": "string" },
    "duration_ms": { "type": "number" },
    "http_status": { "type": "integer" },
    "message": { "type": "string" },
    "error_details": { "type": "object" }
  }
}
```

#### Production Log Samples

*1. Drogon C++ Upload Confirmation Log*:
```json
{
  "timestamp": "2026-09-17T14:30:12.108Z",
  "level": "INFO",
  "service": "drogon-api",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "user_id": 105,
  "file_id": 2048,
  "event": "file.upload.confirmed",
  "duration_ms": 12.4,
  "http_status": 200,
  "message": "Upload confirmation validated via MinIO HeadObject; metadata committed to MySQL and event published to RabbitMQ."
}
```

*2. Celery Worker Vector Upsert Log*:
```json
{
  "timestamp": "2026-09-17T14:30:15.842Z",
  "level": "INFO",
  "service": "ml-workers-celery",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "5c34e897a14e9102",
  "user_id": 105,
  "file_id": 2048,
  "event": "qdrant.upsert.completed",
  "duration_ms": 41.2,
  "message": "Successfully indexed 24 chunks into Qdrant collection drivex_file_chunks with payload tags."
}
```

---

## 5. Section 4: Phased 5-Stage Implementation Roadmap

The implementation plan is structured into five sequential, verifiable phases. Progression to a subsequent phase requires 100% satisfaction of all entry criteria, automated unit/integration test suites, and empirical k6 load-testing benchmarks.

```
+---------------------------------------------------------------------------------------------------+
|                                 5-STAGE IMPLEMENTATION ROADMAP                                    |
+---------------------------------------------------------------------------------------------------+
| Phase 1: Core Storage & Foundation (Weeks 1-2)                                                    |
|  - C++ Drogon REST API & Argon2id / RS256 Auth                                                    |
|  - MySQL Canonical Schema & Redis Cache-Aside Sessions                                            |
|  - MinIO S3 SigV4 Pre-Signed PUT / GET Upload/Download Pipeline                                   |
|  - Two-Phase Redis Lua Quota Reservation                                                          |
|  - Automated Test: k6 Stage 1 (50 VUs metadata) & Stage 2 (500 VUs small files)                   |
+---------------------------------------------------------------------------------------------------+
                                                | Exit Gate 1 Passed
                                                v
+---------------------------------------------------------------------------------------------------+
| Phase 2: Sharing, RBAC & Versioning (Week 3)                                                      |
|  - Multi-Tier RBAC Engine (Viewer, Editor, Owner) with Recursive Ancestor Inheritance             |
|  - Expiring Cryptographic Share Links with Optional Password Protection                          |
|  - Append-Only File Versioning & Reversion APIs                                                    |
|  - Soft-Delete Trash Retention Lifecycle & Audit Logging                                          |
|  - Automated Test: k6 500 VUs Concurrent Permission & Share Link Resolution (<10ms)               |
+---------------------------------------------------------------------------------------------------+
                                                | Exit Gate 2 Passed
                                                v
+---------------------------------------------------------------------------------------------------+
| Phase 3: Asynchronous AI/ML Pipeline (Weeks 4-5)                                                  |
|  - RabbitMQ AMQP Topology (drivex.events, DLX, TTL Retry Queues)                                  |
|  - Celery Worker Cluster (PyMuPDF, python-docx, Tesseract OCR Ingestion)                          |
|  - Recursive Semantic Text Chunking (512 tokens / 64 overlap)                                     |
|  - BAAI/bge-large-en-v1.5 Embeddings & Qdrant HNSW Collection Indexing                           |
|  - Automated Test: k6 Stage 3 (2,000 VUs, 10-50MB files, sustained throughput > 500 MB/s)         |
+---------------------------------------------------------------------------------------------------+
                                                | Exit Gate 3 Passed
                                                v
+---------------------------------------------------------------------------------------------------+
| Phase 4: Intelligent Assistant & Deduplication (Week 6)                                           |
|  - Hybrid Vector + MySQL FULLTEXT Search with Reciprocal Rank Fusion (RRF)                        |
|  - Deep Cross-Encoder Re-Ranking (bge-reranker-large)                                             |
|  - Conversational RAG with HyDE Expansion & SSE Token Streaming (/api/v1/chat)                    |
|  - Cryptographic SHA-256 & Perceptual Image Hashing (pHash/dHash Hamming <= 6)                   |
|  - Automated Test: k6 Stage 4 (5,000 VUs, 100MB+ files, resume-on-failure, SSE TTFT < 800ms)      |
+---------------------------------------------------------------------------------------------------+
                                                | Exit Gate 4 Passed
                                                v
+---------------------------------------------------------------------------------------------------+
| Phase 5: Scale, Hardening & Production Readiness (Weeks 7-8)                                      |
|  - Multi-Node Drogon Load Balancing & MySQL Replica Read-Splitting                               |
|  - Horizontal Database Sharding Roadmap on owner_id / workspace_id                                |
|  - Dual Rate Limiting (IP Leaky Bucket + User Token Bucket)                                       |
|  - OpenTelemetry C++/Python Distributed Tracing & Prometheus / Grafana Dashboards                 |
|  - Production Kubernetes Packaging with KEDA Queue Autoscaling                                    |
|  - Automated Test: k6 Stage 5 (10,000 VUs Full Mixed Workload, 4-Hour Soak Test)                  |
+---------------------------------------------------------------------------------------------------+
```

---

### 5.1 Phase 1: Core Storage & Foundation (Weeks 1–2)

#### 1. Components & Architecture
- **Stateless Drogon C++ Core**: Application bootstrap in `src/main.cpp`, asynchronous MySQL connection pool, non-blocking Redis RESP client pool, and dedicated CPU thread pool for Argon2id hashing.
- **Identity & Authentication**: `/api/v1/auth/register`, `/api/v1/auth/login`, `/api/v1/auth/refresh`. RS256 asymmetric JWT generation with 15-minute access expiry and 30-day rotating refresh tokens in Redis.
- **Hierarchical Directory Tree**: Adjacency list modeling in MySQL `folders` table with recursive CTE path resolution and cycle detection in `PathResolver`.
- **Direct-to-MinIO Storage Brokerage**: `StorageService` negotiating AWS SigV4 pre-signed PUT URLs (`/api/v1/files/upload-url`) and pre-signed GET URLs (`/api/v1/files/{id}/download-url`).
- **Two-Phase Quota Reservation**: Atomic Redis Lua reservation script and MySQL reconciliation.
- **Static HTMX Frontend**: Folder navigation, direct browser-to-MinIO file upload progress bar.

#### 2. Dependencies
- MySQL 8.0 instance with `db/schema.sql` applied.
- Redis 7.2 running with persistence.
- MinIO instance with `drivex-blobs` bucket initialized with private ACL and S3 CORS policy (`mc cors set local/drivex-blobs`).

#### 3. Exact Deliverables & Code Artifacts
- `infra/minio/cors.xml`: MinIO S3 CORS XML policy with ETag exposure.
- `api/CMakeLists.txt`: Fully linked build configuration (`Drogon`, `jwt-cpp`, `argon2`, `OpenSSL`, `libcurl`).
- `api/src/controllers/AuthController.cc`: Authentication endpoints.
- `api/src/controllers/FilesController.cc`: File upload and download negotiation.
- `api/src/controllers/FoldersController.cc`: Folder CRUD and recursive path listing.
- `api/src/services/StorageService.cc`: S3 SigV4 URL signer.
- `api/src/services/QuotaService.cc`: Redis Lua reservation evaluator.
- `api/src/utils/PathResolver.cc`: Recursive CTE folder tree queries and cycle prevention.
- `frontend/static/js/upload.js`: 3-step direct S3 upload client with progress events.

#### 4. Concrete Entry Criteria
- Development environment provisioned with Docker Compose backing services healthy.
- Cryptographic RS256 key pair (`jwt_rs256.key` and `jwt_rs256.pub`) generated and mounted.

#### 5. Concrete Exit Criteria
- Users can register, authenticate, and refresh tokens.
- Nested folders can be created up to depth 32 without cycle anomalies.
- Files up to 500MB stream directly to MinIO without API memory consumption.
- Uploads exceeding user quota are rejected with HTTP 413 prior to pre-signed URL generation.
- Automated k6 tests for Stage 1 and Stage 2 pass 100% of assertions.

#### 6. Automated Verification Tests

##### A. Unit & Integration Test Commands
```bash
# Build C++ API
cd /Users/rajat/Desktop/drive-clone/api && mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc)
ctest --output-on-failure

# Verify MinIO SigV4 Pre-Signing
curl -X POST http://localhost:8080/api/v1/files/upload-url \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"test.pdf","folder_id":null,"size_bytes":1048576,"mime_type":"application/pdf","content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}' \
  | jq -e '.upload_url != null'
```

##### B. Automated Load Test: Stage 1 (50 Workers - Metadata Baseline)
- **Script**: `load-tests/stage1_50workers.js`
- **Execution Command**: `k6 run load-tests/stage1_50workers.js`
- **Configuration**: 50 virtual users (VUs) executing continuous folder navigation and metadata lookups for 60 seconds.
- **Threshold Assertions**:
  - `http_req_failed`: `rate < 0.001` (0.1% max error rate).
  - `http_req_duration{type:metadata}`: `p(99) < 15ms`.

##### C. Automated Load Test: Stage 2 (500 Workers - Small File Uploads)
- **Script**: `load-tests/stage2_500workers.js`
- **Execution Command**: `k6 run load-tests/stage2_500workers.js`
- **Configuration**: 500 concurrent VUs executing upload URL negotiations, direct MinIO PUTs (files <= 1MB), and confirmation calls for 120 seconds.
- **Threshold Assertions**:
  - `http_req_failed`: `rate < 0.001` (99.9% success rate).
  - `http_req_duration{type:api_negotiation}`: `p(99) < 50ms`.

---

### 5.2 Phase 2: Sharing, RBAC & Versioning (Week 3)

#### 1. Components & Architecture
- **RBAC Authorization Engine**: `PermissionService` evaluating `viewer`, `editor`, and `owner` permissions with recursive ancestor tree inheritance (permissions set on a parent folder automatically propagate to child folders and files).
- **Cryptographic Share Links**: `/api/v1/share-links` issuing high-entropy tokens (32 random bytes hex-encoded, SHA-256 hashed in database). Supports optional password protection (Argon2id), max download limits, and expiration timestamps.
- **Append-Only File Versioning**: Every upload overwriting an existing file creates a new immutable record in `file_versions`, incrementing `version_num` while updating `files.current_version_id`. Historical versions remain accessible and restorable.
- **Soft-Delete Trash Lifecycle**: Two-phase deletion (`is_trashed=TRUE`, `trashed_at=NOW()`). Trashed files are excluded from active folder listings; automated background worker permanently purges files trashed $> 30$ days.
- **Audit Ledger**: Immutable event records written to `audit_log` detailing user actions, IP addresses, and resource IDs.

#### 2. Dependencies
- Phase 1 signed off and operational.
- `permissions`, `share_links`, `file_versions`, and `audit_log` tables active in MySQL.

#### 3. Exact Deliverables & Code Artifacts
- `api/src/services/PermissionService.cc`: Permission evaluator with Redis permission caching (`perm:user:{id}:file:{id}`).
- `api/src/controllers/ShareController.cc`: Share link generation, token resolution, and permission grants.
- `api/src/controllers/FilesController.cc`: Version listing (`/files/{id}/versions`) and restoration (`/files/{id}/versions/{v}/restore`).
- `frontend/templates/share-dialog.html`: Sharing modal with permission toggles and link copier.
- `load-tests/stage2b_permissions.js`: k6 load test script benchmarking concurrent permission resolution across deep trees under 500 VUs.

#### 4. Concrete Entry Criteria
- Phase 1 test suite passing with zero regressions.
- Database migration verifying foreign keys and indexes on `permissions` and `share_links`.

#### 5. Concrete Exit Criteria
- Users with `viewer` role cannot modify, rename, or delete files.
- Modifying a parent folder permission instantly alters access for nested child files.
- Public share link tokens resolve correctly; expired or access-capped links return HTTP 403 Forbidden.
- Overwriting a file preserves all previous binary versions in MinIO and metadata in `file_versions`.
- Automated permission resolution benchmark meets $< 10$ms latency target under 500 concurrent workers.

#### 6. Automated Verification Tests

##### A. Permission Inheritance Test
```bash
# Verify viewer cannot delete file
curl -X DELETE "http://localhost:8080/api/v1/files/2048" \
  -H "Authorization: Bearer $VIEWER_JWT" \
  | jq -e '.status == 403'
```

##### B. Automated Load Test: Concurrent Permission Resolution (500 Workers)
- **Script**: `load-tests/stage2b_permissions.js`
- **Execution Command**: `k6 run load-tests/stage2b_permissions.js`
- **Configuration**: 500 VUs querying shared resources across deep 5-level directory trees for 60 seconds.
- **Threshold Assertions**:
  - `http_req_failed`: `rate < 0.001` (99.9% success rate).
  - `http_req_duration{type:permission_check}`: `p(99) < 10ms` (leveraging Redis permission cache).

---

### 5.3 Phase 3: Asynchronous AI/ML Pipeline (Weeks 4–5)

#### 1. Components & Architecture
- **AMQP Messaging Topology**: RabbitMQ topic exchange `drivex.events`, dead-letter exchange `drivex.events.dlx`, and TTL retry queues (`drivex.retry.30s`, `drivex.retry.5m`).
- **Celery Ingestion Workers**: Python 3.11 Celery cluster with late acknowledgments (`task_acks_late=True`) and `worker_prefetch_multiplier=1`.
- **Multi-Format Extraction**:
  - Native PDF: PyMuPDF (`fitz`) extracting structured text blocks, fonts, and bounding boxes.
  - Scanned PDF & Images: Tesseract OCR 5 (`--psm 1 --oem 1`) extracting text from raster pages.
  - Word: `python-docx` extracting hierarchical sections, headings, and table data.
  - Text/Markdown: UTF-8 normalization with `chardet` encoding fallback.
- **Semantic Overlap Chunking**: Recursive character/token splitter targeting 512 tokens with 64-token overlap and hierarchical heading breadcrumb injection (`[Doc: {name} | Section: {head} | Page: {p}]`).
- **Dense Vector Embeddings**: `BAAI/bge-large-en-v1.5` generating 1024-dimensional float32 dense vectors with $L_2$ unit normalization.
- **Qdrant Vector Database Collection**: Initializing collection `drivex_file_chunks` configured with Cosine distance metric, in-memory HNSW graphs ($M=16, \text{ef\_construct}=100$), INT8 scalar quantization, and payload inverted indexes on `owner_id`, `workspace_id`, and `file_id`.
- **Basic Semantic Search**: `/api/v1/search` endpoint proxying to FastAPI to return vector-ranked chunks.

#### 2. Dependencies
- RabbitMQ 3.13 cluster healthy with quorum queue support.
- Qdrant v1.9+ deployed and ready on port 6333.
- Python ML worker container with PyTorch, Sentence-Transformers, PyMuPDF, and Tesseract 5 binaries installed.

#### 3. Exact Deliverables & Code Artifacts
- `ml-workers/app/celery_app.py`: Celery production broker, queue, and retry definitions.
- `ml-workers/app/tasks/embed_file.py`: Ingestion, extraction, chunking, and embedding task pipeline.
- `ml-workers/app/models/embeddings.py`: Thread-safe singleton model loader for `BAAI/bge-large-en-v1.5`.
- `ml-workers/app/api/search.py`: FastAPI semantic search controller with tenant filtering.
- `api/src/controllers/SearchController.cc`: Drogon proxy hydrating ML search results with MySQL file metadata.
- `load-tests/stage3_2000workers.js`: k6 load test script benchmarking sustained storage I/O > 500 MB/s under 2,000 VUs.

#### 4. Concrete Entry Criteria
- Phase 1 and 2 successfully signed off.
- Pre-downloaded transformer model weights mounted into `/cache/models`.

#### 5. Concrete Exit Criteria
- Every confirmed upload of a PDF, DOCX, TXT, or JPEG automatically produces chunk vectors in Qdrant within 30 seconds.
- Corrupt or password-protected files gracefully fail with `processing_status = 'FAILED'`, isolating poison pill messages to `drivex.dlq` after 3 retries without crashing worker nodes.
- Semantic search returns relevant document passages with Recall@10 $> 85\%$ on validation dataset.
- Automated k6 Stage 3 load test validates sustained throughput $> 500$ MB/s under 2,000 concurrent workers.

#### 6. Automated Verification Tests

##### A. Ingestion & Vector Accuracy Test
```bash
# Upload test research paper, wait 15s, execute semantic query
curl -X POST http://localhost:8001/search \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"query":"distributed consensus algorithms","top_k":5}' \
  | jq -e '.results[0].score > 0.75'
```

##### B. Automated Load Test: Stage 3 (2,000 Workers - Mixed Files 10–50MB)
- **Script**: `load-tests/stage3_2000workers.js`
- **Execution Command**: `k6 run load-tests/stage3_2000workers.js`
- **Configuration**: 2,000 concurrent VUs executing metadata browsing, large file uploads (10–50MB), and semantic searches for 300 seconds.
- **Threshold Assertions**:
  - `http_req_failed`: `rate < 0.005` (99.5% success rate).
  - `http_req_duration{type:search}`: `p(99) < 20ms`.
  - `storage_throughput_bytes`: `rate > 524288000` (> 500 MB/s sustained MinIO I/O).
  - `rabbitmq_queue_messages_ready{queue="drivex.file.ingest"}`: `< 100` (Worker pool keeps pace with ingestion).

---

### 5.4 Phase 4: Intelligent Assistant & Deduplication (Week 6)

#### 1. Components & Architecture
- **Query Expansion via HyDE**: Transforming ambiguous or terse conversational queries into rich synthetic document passages using Hypothetical Document Embeddings prior to retrieval.
- **Hybrid Search via Reciprocal Rank Fusion (RRF)**: Merging dense vector similarity search from Qdrant with sparse keyword matching from MySQL `FULLTEXT` index:
  $$\text{RRF\_Score}(d) = \frac{1}{60 + \text{rank}_{\text{dense}}(d)} + \frac{1}{60 + \text{rank}_{\text{sparse}}(d)}$$
- **Deep Cross-Encoder Re-Ranking**: Top-50 merged candidates evaluated by `BAAI/bge-reranker-large` cross-encoder to compute deep semantic relevance scores with cutoff threshold ($S \ge 0.40$).
- **Token Budgeting & Grounded Citation Assembly**: Allocating a strict 4,096-token context window with mandatory citation formatting (`[Document: {name}, Page: {p}]`). Chunks scoring below threshold are pruned to prevent model hallucination.
- **Server-Sent Events (SSE) Streaming**: Low-latency token streaming to browser client via `/api/v1/chat`, with dedicated `event: sources` event conveying document citations.
- **Multi-Layer Deduplication Engine**:
  - Cryptographic SHA-256 byte matching in MySQL.
  - Perceptual image hashing: 64-bit DCT perceptual hash (`pHash`) and gradient difference hash (`dHash`) evaluated with Hamming distance:
    $$\text{Hamming Distance} = \text{popcount}(\text{hash}_1 \oplus \text{hash}_2) \le 6$$

#### 2. Dependencies
- Phase 3 Qdrant vector index fully populated.
- LLM inference backend (OpenAI-compatible API endpoint or self-hosted vLLM container).

#### 3. Exact Deliverables & Code Artifacts
- `ml-workers/app/api/chat.py`: Streaming RAG conversational controller with HyDE and SSE delivery.
- `ml-workers/app/models/reranker.py`: Cross-encoder model loader and scoring pipeline.
- `ml-workers/app/tasks/dedup_check.py`: SHA-256 and pHash/dHash calculation and duplicate link creator.
- `frontend/templates/chat.html`: HTMX SSE chat interface with source citation drawer.
- `load-tests/stage4_5000workers.js`: k6 load test script benchmarking conversational RAG TTFT (<800ms) and resilience under 5,000 VUs.

#### 4. Concrete Entry Criteria
- Phase 3 passed and verified.
- Cross-encoder model weights loaded into cache.

#### 5. Concrete Exit Criteria
- Conversational assistant streams responses with Time-to-First-Token (TTFT) $< 800$ms.
- Assistant strictly adheres to retrieved facts; responds with "insufficient context" when queries cannot be answered from drive documents.
- Resized, recompressed, or watermarked duplicate images trigger near-duplicate flags.
- Automated k6 Stage 4 load test validates 5,000 concurrent workers with network interruption simulation.

#### 6. Automated Verification Tests

##### A. RAG Grounding & Citation Test
```bash
# Query chat endpoint via SSE, verify source citation event is emitted
curl -N -X POST http://localhost:8001/chat \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Summarize the cloud architecture specifications"}]}' \
  | grep "event: sources"
```

##### B. Automated Load Test: Stage 4 (5,000 Workers - Large Files 100MB+ & Resumable)
- **Script**: `load-tests/stage4_5000workers.js`
- **Execution Command**: `k6 run load-tests/stage4_5000workers.js`
- **Configuration**: 5,000 concurrent VUs uploading large files (100MB+) via multipart pre-signed URLs, injecting random TCP resets to test chunk retry/resumption, while concurrently streaming RAG chat tokens.
- **Threshold Assertions**:
  - `http_req_failed`: `rate < 0.01` (99.0% completion across network disruptions).
  - `rag_ttft_seconds`: `p(95) < 0.8s` (TTFT < 800ms).

---

### 5.5 Phase 5: Scale, Hardening & Production Readiness (Weeks 7–8)

#### 1. Components & Architecture
- **Multi-Node Drogon Load Balancing**: Horizontal scaling of Drogon API instances behind Nginx with round-robin load distribution.
- **MySQL Read-Splitting & Read-Your-Writes Consistency**: Read queries (folder browsing, metadata lookups, share links) routed to `mysql-replica`; write mutations routed to `mysql-primary`. Enforces "Read-Your-Writes" consistency: Drogon pins user read sessions to `mysql-primary` for a 2.0-second sticky window post-write (exceeding maximum replication lag SLA < 1.0s) or warms Redis folder cache directly on commit, preventing stale-replica read race conditions.
- **Horizontal Sharding Roadmap**: Architecture for partitioning database on `owner_id` / `workspace_id` using Vitess or Citus when metadata exceeds 500 million rows.
- **Edge Hardening & Rate Limiting**: Full deployment of IP-based Leaky Bucket (100r/s) in Nginx and User Token Bucket (50r/s) in Drogon/Redis with RFC 7807 error models.
- **Full-Stack Observability Deployment**: OpenTelemetry Collector forwarding traces to Tempo/Jaeger; Prometheus scraping all 13 services; Grafana dashboards online.
- **Production Kubernetes Packaging**: SealedSecrets, KEDA autoscaling, and ResourceQuotas deployed to production cluster.

#### 2. Dependencies
- Complete integration of Phases 1 through 4.
- High-capacity test infrastructure capable of generating 10,000 concurrent user streams.

#### 3. Exact Deliverables & Code Artifacts
- `infra/docker/nginx/nginx.conf`: Production rate-limiting and TLS configuration.
- `infra/k8s/`: Complete Kubernetes production manifest suite with KEDA `ScaledObject`s.
- `infra/docker/grafana/dashboards/`: Production Grafana dashboard JSON configurations.
- `load-tests/stage5_10000workers.js`: Full-scale mixed enterprise benchmark suite.

#### 4. Concrete Entry Criteria
- All previous phases (1–4) fully signed off with zero open blocker defects.
- Infrastructure monitoring stack active and logging metrics.

#### 5. Concrete Exit Criteria
- Zero data loss or corrupted S3 objects under continuous 4-hour soak test at 10,000 concurrent workers.
- Failover test: MySQL primary node crash triggers automatic promotion with Drogon connection pool recovery $< 15$ seconds.
- Chaos test: SIGKILL of Celery worker pods results in zero lost messages due to RabbitMQ quorum queues.
- Stage 5 k6 benchmark suite completes with all SLA thresholds satisfied.
- Forensic audit completed and master blueprint signed off.

#### 6. Automated Verification Tests

##### A. Failover & Chaos Verification
```bash
# Kill primary MySQL container, verify replica recovery and API availability
docker kill drivex-mysql-primary
sleep 10
curl -f http://localhost:8080/health || exit 1
```

##### B. Automated Load Test: Stage 5 (10,000 Workers - Full Mixed Enterprise Workload)
- **Script**: `load-tests/stage5_10000workers.js`
- **Execution Command**: `k6 run load-tests/stage5_10000workers.js`
- **Configuration**: 10,000 concurrent virtual users executing a realistic blended enterprise workload:
  - 40% Directory browsing & folder tree navigation.
  - 25% File metadata retrieval & permissions resolution.
  - 15% Pre-signed upload & download file operations (1MB to 100MB).
  - 12% Semantic vector search queries.
  - 8% Interactive RAG conversational chat streams over SSE.
  - Duration: 4-hour soak test.
- **Threshold Assertions**:
  - `http_req_failed`: `rate < 0.001` (< 0.1% aggregate failure).
  - `http_req_duration{type:metadata}`: `p(99) < 50ms`.
  - `http_req_duration{type:search}`: `p(99) < 35ms`.
  - `rag_time_to_first_token_seconds`: `p(95) < 1.0s`.
  - `redis_cache_hit_ratio`: `> 0.85` (> 85% cache hits on folder listings).
  - `mysql_replica_lag_seconds`: `< 1.0s`.

---

## 6. Section 5: Disaster Recovery, Backup & Security Hardening Specifications

### 6.1 Automated MySQL Point-in-Time Recovery (PITR)
- **Binary Logging**: ROW format binary logging enabled on primary (`--binlog-format=ROW`, `--log-bin=mysql-bin`).
- **Snapshot Cadence**: Daily physical database snapshots taken via Percona XtraBackup at 02:00 UTC, uploaded to an isolated backup MinIO/S3 bucket with Object Lock (WORM).
- **RPO / RTO SLAs**:
  - **Recovery Point Objective (RPO)**: $\le 5$ minutes (achieved via continuous binary log streaming to remote object storage).
  - **Recovery Time Objective (RTO)**: $\le 30$ minutes (restoring base snapshot and replaying binary logs).

### 6.2 MinIO S3 Bucket Provisioning, CORS Policy & Replication
- **S3 CORS Configuration**: Required for direct browser-to-MinIO pre-signed binary uploads originating from web origin `https://drivex.example.com` and local development origin `http://localhost:8080`.
- **CORS XML Policy (`infra/minio/cors.xml`)**:
  ```xml
  <CORSConfiguration>
    <CORSRule>
      <AllowedOrigin>https://drivex.example.com</AllowedOrigin>
      <AllowedOrigin>http://localhost:8080</AllowedOrigin>
      <AllowedMethod>PUT</AllowedMethod>
      <AllowedMethod>GET</AllowedMethod>
      <AllowedMethod>HEAD</AllowedMethod>
      <AllowedMethod>OPTIONS</AllowedMethod>
      <AllowedHeader>*</AllowedHeader>
      <ExposeHeader>ETag</ExposeHeader>
      <MaxAgeSeconds>3600</MaxAgeSeconds>
    </CORSRule>
  </CORSConfiguration>
  ```
- **CLI Setup & Verification Instructions**:
  ```bash
  # Configure MinIO client credentials
  mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

  # Ensure drivex-blobs bucket exists
  mc mb --ignore-existing local/drivex-blobs

  # Apply S3 CORS configuration
  mc cors set local/drivex-blobs infra/minio/cors.xml

  # Verify active CORS rules
  mc cors info local/drivex-blobs
  ```
- **Bucket Versioning**: Enabled on `drivex-blobs` bucket to prevent accidental object overwrite or administrative deletion.
- **Site Replication**: Multi-site active-passive replication configured between primary datacenter and disaster recovery datacenter using MinIO Site Replication (`mc admin replicate`).
- **Lifecycle Expiration Rules**: Expired multipart upload parts purged automatically after 24 hours (`AbortIncompleteMultipartUpload`).

### 6.3 Qdrant Vector Collection Snapshots
- **Automated Snapshot Cron**: Daily automated snapshots created via Qdrant REST API (`POST /collections/drivex_file_chunks/snapshots`).
- **Offsite Archive**: Snapshots downloaded and archived to S3 disaster recovery storage. Recovery involves single API call (`PUT /collections/drivex_file_chunks/snapshots/recover`).

### 6.4 Zero-Trust Network Policy Hardening (Kubernetes)
- Inter-pod network policies restrict database access exclusively to authorized API pods.
- MySQL port `3306` is inaccessible from outside the `drivex` namespace.
- All intra-cluster inter-service communication passes through mutual TLS (mTLS) via Istio or Cilium service mesh.
