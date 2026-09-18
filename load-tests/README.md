# DriveX k6 Staged Load-Testing Suite

Staged k6 benchmark suite verifying system latency, concurrency, throughput, and error-rate SLAs across implementation phases:

- `stage1_50workers.js`: Phase 1 Baseline — 50 concurrent VUs executing metadata-only operations (`GET /api/v1/folders`, `GET /api/v1/users/me`). Target: p99 < 15ms, error rate < 0.1%.
- `stage2_500workers.js`: Phase 1 Upload Flow — 500 concurrent VUs executing upload URL negotiations, direct MinIO PUTs (<=1MB), and confirmation commits. Target: p99 < 50ms, error rate < 0.1%.
- `stage2b_permissions.js`: Phase 2 RBAC Resolution — 500 concurrent VUs querying permissions and nested folder paths across deep trees. Target: p99 < 10ms (Redis cache-aside).
- `stage3_2000workers.js`: Phase 3 AI/ML & Ingestion — 2,000 concurrent VUs executing mixed metadata queries and 10MB–50MB upload transfers. Target: Sustained MinIO I/O > 500 MB/s, search p99 < 20ms.
- `stage4_5000workers.js`: Phase 4 Conversational RAG — 5,000 concurrent VUs querying `/api/v1/chat` and `/api/v1/search` with SSE streaming. Target: Time-to-First-Token (TTFT) < 800ms, error rate < 1.0%.
- `stage5_10000workers.js`: Phase 5 Enterprise Soak — 10,000 concurrent VUs executing blended enterprise workload (40% browsing, 25% permissions/metadata, 15% uploads/downloads, 12% semantic search, 8% conversational RAG). Target: metadata p99 < 50ms, search p99 < 35ms, TTFT < 1.0s, error rate < 0.1%.

## Running Benchmarks

```bash
# Set environment variables
export BASE_URL="http://localhost:8080"
export TEST_JWT="<valid_rs256_jwt_token>"

# Run desired stage
k6 run load-tests/stage1_50workers.js
k6 run load-tests/stage2_500workers.js
k6 run load-tests/stage2b_permissions.js
k6 run load-tests/stage3_2000workers.js
k6 run load-tests/stage4_5000workers.js
k6 run load-tests/stage5_10000workers.js
```
