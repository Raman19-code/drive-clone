# DriveX REST API Specification & Master Integration Guide

**Document Version**: 1.0.0-PROD  
**Author**: DriveX Systems Architecture Group  
**Target Runtimes**: C++ Drogon (Control Plane), MinIO S3 (Data Plane), MySQL 8.0, Redis 7.0, RabbitMQ 3.13, Python Celery & FastAPI (AI/ML), Qdrant Vector Engine  
**OpenAPI Specification Companion**: `docs/api-spec.yaml` (OpenAPI 3.0.3)  
**Status**: Authoritative Production Standard  

---

## 1. Architectural Overview & System Contracts

DriveX is an enterprise-grade, high-throughput, horizontally scalable cloud storage platform engineered as a self-hosted Google Drive analogue. Built to handle massive concurrent traffic and petabyte-scale file storage, DriveX enforces a strict architectural bifurcation between the **Control Plane** (metadata, access control, relational persistence, AI workflows) and the **Data Plane** (raw binary file streaming).

```
+----------------------------------------------------------------------------------------------------+
|                                            Client Tier                                             |
|                     Web Application (HTMX + Tailwind)  |  Mobile / CLI Apps                        |
+----------------------------------------------------------------------------------------------------+
                                |                                            |
         [Control Plane: HTTPS / REST / SSE]                [Data Plane: Direct HTTPS / S3 PUT & GET]
                                v                                            v
+--------------------------------------------------+       +-----------------------------------------+
|        Nginx Reverse Proxy & Edge Ingress        |       |        MinIO Distributed Object Store   |
|  - TLS 1.3 Termination                           |       |  - Port 9000 (S3 API)                   |
|  - Leaky-Bucket Rate Limiting                    |       |  - Erasure Coding Data Storage          |
|  - SSE Request Unbuffering (proxy_buffering off) |       |  - Direct SHA-256 Checksum Validation   |
+--------------------------------------------------+       +-----------------------------------------+
                                |                                            ^
                                v                                            |
+--------------------------------------------------+                         | (HeadObject /
|        Drogon C++ Stateless Control Plane        |-------------------------+  SigV4 Presign)
|  - Multi-Reactor Event Loop (epoll / kqueue)     |
|  - Asymmetric RS256 JWT Token Verification       |
|  - Offloaded CPU Thread Pool (Argon2id Hashing)  |
+--------------------------------------------------+
          |                       |                       |
          v                       v                       v
+------------------+    +-------------------+   +------------------------------------+
|   Redis 7 Cache  |    |   MySQL 8 RDBMS   |   |   RabbitMQ Message Broker          |
| - Session tokens |    | - Primary Storage |   | - Exchange: drivex.events (Topic)  |
| - Folder listings|    | - Recursive CTEs  |   | - Dead-Letter & Retry Queues       |
| - Quota locks    |    | - InnoDB ACID     |   +------------------------------------+
+------------------+    +-------------------+                     |
                                                                  v
                                                        +--------------------+
                                                        |   Celery Workers   |
                                                        | (PyMuPDF / OCR)    |
                                                        +--------------------+
                                                                  |
                                                                  v
                                                        +--------------------+
                                                        | Qdrant Vector DB   |
                                                        | (bge-large-en-v1.5)|
                                                        +--------------------+
```

### 1.1 Ingress & Routing Topology
1. **API Endpoints (`/api/v1/*`)**: Routed to the Drogon C++ Core API cluster on port `8080`.
2. **Object Storage (`/drivex-blobs/*`)**: Pre-signed URLs target MinIO on port `9000` directly or through edge ingress. Under no circumstances do raw binary uploads or downloads traverse Drogon memory buffers.
3. **Conversational Assistant (`/api/v1/chat`)**: Proxied through Drogon or directed to the FastAPI ML backend with `proxy_buffering off` to support continuous Server-Sent Events (SSE) token streaming.

### 1.2 Base URLs & Protocols
- **Production Edge HTTPS**: `https://api.drivex.dev`
- **Local Development**: `http://localhost:8080`
- **Protocol**: HTTP/1.1 and HTTP/2 over TLS 1.3
- **Content Encoding**: `application/json; charset=utf-8` for REST metadata, `application/problem+json` for error states, `text/event-stream; charset=utf-8` for AI chat.

---

## 2. Authentication, Cryptography & Session Management

### 2.1 Argon2id Password Hashing Specification (`m=64MB, t=3, p=4`)
User passwords and share link passcodes are hashed using **Argon2id** conforming to RFC 9106 with canonical parameters (`m=64MB, t=3, p=4`). Argon2id combines memory-hardness (mitigating GPU/ASIC brute-force) with side-channel resistance against cache-timing attacks.

#### Cryptographic Parameters:
- **Algorithm Identifier**: `Argon2id` (v=19)
- **Memory Cost ($m$)**: `m=64MB` (`65536 KiB` / 64 MiB)
- **Time Cost / Iterations ($t$)**: `t=3` iterations
- **Parallelism ($p$)**: `p=4` threads
- **Salt Length**: 16 cryptographically secure random bytes generated via `/dev/urandom`
- **Output Digest Length**: 32 bytes (256 bits)
- **Encoded Modular Crypt Format**: `$argon2id$v=19$m=65536,t=3,p=4$<salt-b64>$<hash-b64>`

#### Drogon CPU Offloading Architecture:
Argon2id verification requires approximately 60ms to 90ms of dedicated CPU time. To prevent blocking the non-blocking Trantor/epoll event loop running on the I/O threads, all password hashing and verification tasks are dispatched to Drogon's CPU task thread pool:
```cpp
// Asynchronous offload from I/O Reactor to CPU Worker Pool
drogon::app().getAsyncWorker()->run([req, callback, email, password]() {
    // Executes inside worker thread
    bool valid = argon2id_verify(stored_hash, password);
    drogon::app().getLoop()->queueInLoop([callback, valid]() {
        // Returns result on I/O thread
        if (!valid) {
            callback(buildProblemDetails(HttpStatusCode::k401Unauthorized, "INVALID_CREDENTIALS"));
            return;
        }
        // Issue RS256 JWT...
    });
});
```

### 2.2 Asymmetric RS256 JSON Web Tokens (Access Tokens)
Authentication is stateless using asymmetric **RS256** (RSA Signature with SHA-256) JSON Web Tokens conforming to RFC 7519.
- **Key Pair**: 2048-bit or 4096-bit RSA key pair. The private key resides exclusively in the authentication service / secret store; Drogon load balancers load only the public key into memory.
- **Verification Overhead**: Public key verification is instantaneous (<0.1ms) and executes directly inside Drogon's `JwtAuthFilter` without requiring database or cache round-trips.
- **Lifespan**: 15 minutes (`900 seconds`).

#### Standard Token Claims:
```json
{
  "iss": "https://api.drivex.dev",
  "sub": "105",
  "email": "jane.doe@example.com",
  "role": "user",
  "jti": "d3b07384-d113-4966-9c44-b04928d32ec4",
  "iat": 1789000000,
  "exp": 1789000900
}
```

### 2.3 Rotating Refresh Tokens & Session Lifecycle
- **Entropy & Format**: 256-bit cryptographically secure random token generated from hardware entropy, represented as a 64-character hexadecimal string.
- **Storage**: Stored hashed (SHA-256) in the MySQL `refresh_tokens` table and indexed in Redis under `auth:ref:<token_hash>`. The cleartext token is returned only once to the client.
- **Cookie Security**: Emitted in a secure cookie named `drivex_refresh_token` configured with:
  `HttpOnly; Secure; SameSite=Strict; Path=/api/v1/auth; Max-Age=2592000` (30 days).
- **Automatic Token Rotation**: Each invocation of `POST /api/v1/auth/refresh` revokes the supplied refresh token and issues a fresh access + refresh token pair.
- **Reuse Detection & Anti-Theft**: If a previously used or revoked refresh token is presented, the system triggers security alert protocols: all active refresh tokens associated with that `user_id` are purged, terminating all sessions across devices.

### 2.4 Token Revocation & Redis Blacklisting
When a user logs out (`POST /api/v1/auth/logout`):
1. The refresh token is flagged `revoked = TRUE` in MySQL and evicted from Redis.
2. The active access token's unique identifier (`jti`) is written to the Redis blacklist:
   - **Key**: `auth:jwt:bl:<jti>`
   - **Value**: `"1"`
   - **TTL**: Remaining token lifespan (`exp - current_timestamp`).
3. `JwtAuthFilter` performs a sub-millisecond check against Redis:
   ```cpp
   if (redisClient->exists("auth:jwt:bl:" + jti)) {
       fcb(buildProblemDetails(HttpStatusCode::k401Unauthorized, "TOKEN_REVOKED"));
       return;
   }
   ```

---

## 3. Standardized Error Handling Architecture (RFC 7807)

All non-2xx responses returned across all DriveX API endpoints adhere strictly to the **RFC 7807 Problem Details** specification using the standard media type:
```http
Content-Type: application/problem+json
```

### 3.1 RFC 7807 JSON Data Schema
```json
{
  "type": "https://api.drivex.dev/errors/storage-quota-exceeded",
  "title": "Storage Quota Exceeded",
  "status": 400,
  "detail": "Uploading 26214400 bytes exceeds your remaining storage quota by 5242880 bytes.",
  "instance": "/api/v1/files/upload-url",
  "code": "STORAGE_QUOTA_EXCEEDED",
  "timestamp": "2026-09-17T15:00:00Z",
  "invalid_params": [
    {
      "name": "size_bytes",
      "reason": "Requested allocation exceeds remaining quota (20971520 bytes available)."
    }
  ]
}
```

### 3.2 Master Error Code Catalog

| HTTP Status | Machine Code (`code`) | Problem Type URI (`type`) | Description | Client Action / Mitigation |
|:---|:---|:---|:---|:---|
| **400** | `INVALID_INPUT` | `https://api.drivex.dev/errors/invalid-input` | Malformed JSON or invalid parameter syntax. | Validate request body against schema. |
| **400** | `STORAGE_QUOTA_EXCEEDED` | `https://api.drivex.dev/errors/quota-exceeded` | Account lacks sufficient storage space for upload. | Delete unused files or upgrade tier. |
| **400** | `CYCLIC_FOLDER_HIERARCHY`| `https://api.drivex.dev/errors/cyclic-folder-hierarchy` | Folder move would place folder inside its own subtree. | Select an alternate destination folder. |
| **400** | `MAX_DEPTH_EXCEEDED` | `https://api.drivex.dev/errors/max-depth-exceeded` | Hierarchy depth exceeds the 32-level limit. | Flatten folder nesting structure. |
| **400** | `CHECKSUM_MISMATCH` | `https://api.drivex.dev/errors/checksum-mismatch` | Computed S3 checksum does not match client digest. | Re-calculate SHA-256 and re-upload. |
| **400** | `STORAGE_OBJECT_MISSING` | `https://api.drivex.dev/errors/storage-object-missing` | S3 HeadObject failed; client never uploaded bytes. | Execute PUT to upload URL before calling complete. |
| **401** | `INVALID_CREDENTIALS` | `https://api.drivex.dev/errors/invalid-credentials` | Email or password incorrect. | Check login credentials and retry. |
| **401** | `TOKEN_EXPIRED` | `https://api.drivex.dev/errors/token-expired` | Access token lifetime (15m) elapsed. | Call `/api/v1/auth/refresh`. |
| **401** | `TOKEN_REVOKED` | `https://api.drivex.dev/errors/token-revoked` | Token explicitly revoked upon logout. | Prompt user to log in again. |
| **401** | `REFRESH_TOKEN_REUSED` | `https://api.drivex.dev/errors/refresh-token-reused` | Revoked or previously consumed refresh token presented; potential replay attack. | Invalidate entire token session family and re-authenticate. |
| **401** | `INVALID_SIGNATURE` | `https://api.drivex.dev/errors/invalid-signature` | Asymmetric RSA signature check failed. | Supply untampered JWT issued by DriveX. |
| **401** | `PASSWORD_REQUIRED` | `https://api.drivex.dev/errors/password-required` | Share link requires Argon2id passcode. | Submit password to `/verify-password`. |
| **403** | `INSUFFICIENT_PERMISSIONS`| `https://api.drivex.dev/errors/forbidden` | User lacks required RBAC role. | Request access from resource owner. |
| **403** | `RESOURCE_LOCKED` | `https://api.drivex.dev/errors/resource-locked` | Resource is undergoing an atomic move operation. | Wait 5 seconds and retry. |
| **404** | `USER_NOT_FOUND` | `https://api.drivex.dev/errors/user-not-found` | Specified user account does not exist. | Verify recipient email address. |
| **404** | `FOLDER_NOT_FOUND` | `https://api.drivex.dev/errors/folder-not-found` | Folder ID does not exist or is trashed. | Verify folder ID. |
| **404** | `FILE_NOT_FOUND` | `https://api.drivex.dev/errors/file-not-found` | File ID does not exist or is trashed. | Verify file ID. |
| **404** | `VERSION_NOT_FOUND` | `https://api.drivex.dev/errors/version-not-found` | Historical file version record missing. | Check `/files/{id}/versions`. |
| **404** | `SHARE_LINK_NOT_FOUND` | `https://api.drivex.dev/errors/share-link-not-found` | Share link token invalid or revoked. | Request updated share link. |
| **409** | `RESOURCE_ALREADY_EXISTS`| `https://api.drivex.dev/errors/resource-exists` | Naming collision in target directory. | Rename item or enable overwrite. |
| **409** | `CONCURRENT_MODIFICATION`| `https://api.drivex.dev/errors/concurrent-modification`| Simultaneous conflicting writes detected. | Reload latest state and retry. |
| **409** | `EMAIL_ALREADY_REGISTERED`| `https://api.drivex.dev/errors/email-already-registered`| User account already exists with the supplied email address. | Log in with existing credentials or reset password. |
| **410** | `SHARE_LINK_EXPIRED` | `https://api.drivex.dev/errors/share-link-expired` | Expiry date passed or download cap reached. | Request owner create a new link. |
| **410** | `UPLOAD_SESSION_EXPIRED`| `https://api.drivex.dev/errors/upload-session-expired` | Pre-signed upload session has elapsed its 15-minute validity window. | Negotiate a new upload URL via `POST /files/upload-url`. |
| **413** | `FILE_SIZE_EXCEEDED` | `https://api.drivex.dev/errors/file-size-exceeded` | File exceeds maximum size ceiling (5 GB). | Compress file or split into parts. |
| **422** | `VALIDATION_FAILED` | `https://api.drivex.dev/errors/validation-failed` | Field constraints failed (e.g. minLength). | Review `invalid_params` array. |
| **429** | `RATE_LIMIT_EXCEEDED` | `https://api.drivex.dev/errors/rate-limit-exceeded` | Request burst rate limit exhausted. | Respect `Retry-After` header. |
| **500** | `INTERNAL_SERVER_ERROR` | `https://api.drivex.dev/errors/internal-error` | Unexpected application core exception. | Report error with `timestamp`. |
| **503** | `STORAGE_UNAVAILABLE` | `https://api.drivex.dev/errors/storage-unavailable`| MinIO S3 cluster unreachable. | Retry after storage recovery. |
| **503** | `DATABASE_READONLY` | `https://api.drivex.dev/errors/database-readonly` | MySQL primary down; read replicas online. | Read queries functional; writes delayed. |
| **503** | `AI_ASSISTANT_UNAVAILABLE`| `https://api.drivex.dev/errors/ai-unavailable` | Vector search or LLM engine down. | Search falls back to SQL keyword search. |

---

## 4. Folder Hierarchy, Traversal & Graph Safety Algorithms

Folders in DriveX are modeled as a self-referential adjacency tree in MySQL (`id`, `name`, `parent_id`, `owner_id`, `is_trashed`). To guarantee high performance, sub-millisecond breadcrumb rendering, and mathematical graph integrity, the following algorithms are enforced.

### 4.1 Breadcrumb Path Resolution via Recursive CTE
When rendering navigation breadcrumbs from the root directory down to a target folder, Drogon avoids iterative SQL queries by executing a single MySQL 8 **Recursive Common Table Expression (CTE)**:

```sql
WITH RECURSIVE breadcrumb_trail AS (
    -- Anchor member: Target folder
    SELECT id, name, parent_id, 0 AS depth
    FROM folders
    WHERE id = :target_folder_id 
      AND owner_id = :auth_user_id 
      AND is_trashed = FALSE
      
    UNION ALL
    
    -- Recursive member: Walk up parent pointers
    SELECT f.id, f.name, f.parent_id, bt.depth + 1
    FROM folders f
    INNER JOIN breadcrumb_trail bt ON f.id = bt.parent_id
    WHERE f.is_trashed = FALSE
)
SELECT id, name, depth 
FROM breadcrumb_trail 
ORDER BY depth DESC;
```
*Index Utilization*: This query runs in <1.5ms by traversing the B-tree primary key `PRIMARY (id)` and composite index `idx_folders_hierarchy (owner_id, parent_id, is_trashed, name)`.

### 4.2 Cyclic Move Prevention Algorithm
When moving a folder $F_{source}$ to become a child of $F_{target}$, the system must guarantee that $F_{target}$ is **not a descendant** of $F_{source}$. Allowing this operation would detach the subtree into an unreachable isolated cycle (circular hierarchy).

#### Formal Verification Logic:
1. **Identity Check**: If $F_{source} = F_{target}$, reject immediately (cannot be parent of itself).
2. **Root Check**: If $F_{target}$ is `NULL` (root directory), the move is guaranteed acyclic. Proceed.
3. **Ancestor Walk / Recursive Traversal**: Walk up the ancestor chain starting at $F_{target}$. If $F_{source}$ is encountered at any point, a cycle would be created. Reject with HTTP 400 (`CYCLIC_FOLDER_HIERARCHY`).

```cpp
// C++ Algorithm implemented in PathResolver.cc
bool wouldCreateCycle(int64_t sourceFolderId, int64_t targetParentId) {
    if (sourceFolderId == targetParentId) return true;
    if (targetParentId == 0) return false; // Root directory is safe

    int64_t currentParent = targetParentId;
    int depth = 0;
    const int MAX_SAFE_DEPTH = 32;

    while (currentParent != 0 && depth < MAX_SAFE_DEPTH) {
        if (currentParent == sourceFolderId) {
            return true; // Cycle detected: target is a descendant of source!
        }
        
        // Fetch parent_id of currentParent from Redis cache or DB
        auto folderMeta = getFolderMetadata(currentParent);
        if (!folderMeta.has_value()) break;
        
        currentParent = folderMeta->parent_id.value_or(0);
        depth++;
    }

    return false; // Acyclic
}
```

### 4.3 Maximum Nesting Depth Enforcement
To prevent performance degradation on file system path strings, deeply nested tree renderings, and recursive queries, DriveX enforces a hard ceiling:
$$\text{Max Depth} = 32 \text{ levels}$$
Before creating a folder or finalizing a move, the path resolver checks:
$$\text{depth}(F_{target}) + \text{subtree\_height}(F_{source}) + 1 \le 32$$
If this ceiling is breached, the API rejects the request with HTTP 400 (`MAX_DEPTH_EXCEEDED`).

### 4.4 Distributed Mutex Locking for Tree Mutations
To prevent concurrent race conditions (e.g., two users moving folder A into folder B while moving folder B into folder A simultaneously), Drogon acquires a distributed mutex lock in Redis:
```
SET lock:folder:<folder_id> <uuid> NX PX 5000
```
- If the lock cannot be acquired within 500ms, the API returns HTTP 403 (`RESOURCE_LOCKED`).
- Upon transaction commit, the lock is released via Lua script validating the UUID token.

---

## 5. RBAC Permission Inheritance Model & Delegation

DriveX employs a granular, hierarchical Role-Based Access Control (RBAC) engine. Access rights assigned to a folder automatically cascade down to all enclosed subfolders, files, and historical versions.

### 5.1 RBAC Roles & Privilege Matrix

| Operation | Viewer | Editor | Owner |
|:---|:---:|:---:|:---:|
| View metadata & browse contents | Yes | Yes | Yes |
| Download binary file stream | Yes | Yes | Yes |
| Request pre-signed upload URL | No | Yes | Yes |
| Upload new file / create subfolder | No | Yes | Yes |
| Create new file version | No | Yes | Yes |
| Rename / relocate file or folder | No | Yes | Yes |
| Soft-delete item to Trash | No | Yes | Yes |
| Restore item from Trash | No | Yes | Yes |
| Permanently purge file / folder | No | No | Yes |
| Grant / revoke user permissions | No | No | Yes |
| Generate public share links | No | Yes | Yes |
| Transfer folder ownership | No | No | Yes |

### 5.2 Ancestor Inheritance Resolution Algorithm
When user $U$ attempts an action requiring minimum role $R_{min}$ on file or folder $X$:
1. **Direct Ownership Check**: If $X.owner\_id == U.id$, return `Role::Owner`.
2. **Direct Permission Check**: Check the `permissions` table for direct grant on $(resource\_type, X.id, U.id)$. If found, assign role $R_{direct}$.
3. **Ancestor Tree Walk**: If $X$ is a file, inspect its parent folder. For folders, walk up the parent hierarchy (`parent_id`) to the root. For each ancestor $A$, check for permissions granted to $U$.
4. **Resolution Rule**: Permissions are **permissive-additive** with local override. If an ancestor grants `editor`, the user possesses `editor` on all descendants. If a child explicitly grants `owner`, the user possesses `owner` on that child.
5. **Evaluation**: If $\text{EffectiveRole}(U, X) \ge R_{min}$, access is granted. Otherwise, HTTP 403 (`INSUFFICIENT_PERMISSIONS`) is returned.

### 5.3 Effective Permission Cache Pattern
To avoid repeated ancestor walks, resolved permissions are cached in Redis:
- **Key**: `perm:eff:<user_id>:<resource_type>:<resource_id>`
- **Value**: `"viewer"` | `"editor"` | `"owner"`
- **TTL**: 300 seconds (5 minutes)
- **Invalidation**: Any mutation in `permissions` (grant, modify, revoke) immediately fires Redis key eviction across the affected resource subtree.

---

## 6. Direct-to-MinIO S3 Pre-Signed Transfer Protocols

### 6.1 Two-Phase Storage Quota Enforcement
To prevent storage over-commit during concurrent uploads, DriveX implements a two-phase reservation protocol:
1. **Phase 1: Pre-Flight Atomic Reservation (Redis)**:
   When `POST /api/v1/files/upload-url` is called with `size_bytes`:
   ```redis
   -- Atomic evaluation via Redis Lua script
   local used = redis.call('GET', 'user:quota:used:' .. KEYS[1]) or 0
   local quota = redis.call('GET', 'user:quota:limit:' .. KEYS[1])
   if tonumber(used) + tonumber(ARGV[1]) <= tonumber(quota) then
       redis.call('INCRBY', 'user:quota:used:' .. KEYS[1], ARGV[1])
       redis.call('SETEX', 'quota:res:' .. ARGV[2], 1800, ARGV[1])
       return 1
   else
       return 0
   end
   ```
2. **Phase 2: Post-Upload Reconciliation (MySQL)**:
   Upon `POST /api/v1/files/upload-complete`, the actual byte size verified via MinIO S3 `HeadObject` is committed to MySQL:
   ```sql
   UPDATE users SET storage_used_bytes = storage_used_bytes + :actual_size WHERE id = :user_id;
   ```
   If the client abandons the upload without calling complete, the Redis reservation key expires automatically after 1800s, freeing the reserved capacity.

### 6.2 Pre-Signed PUT Generation & Checksum Binding
1. **Storage Key Format**: Immutable partitioned path:
   `blobs/{owner_id}/{yyyy-mm}/{uuidv4}-{sanitized_filename}`
2. **AWS SigV4 Signing Parameters**:
   - `X-Amz-Algorithm=AWS4-HMAC-SHA256`
   - `X-Amz-Expires=900` (15 minutes)
   - `x-amz-checksum-sha256`: Client-provided SHA-256 digest is embedded in the signed headers.
3. **Client Upload**:
   The client executes direct HTTP PUT to MinIO:
   ```http
   PUT /drivex-blobs/blobs/105/2026-09/upl_8a92f038.key?X-Amz-Algorithm=... HTTP/1.1
   Host: s3.drivex.dev
   Content-Length: 26214400
   Content-Type: application/vnd.apple.keynote
   x-amz-checksum-sha256: 4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a

   <binary stream>
   ```
   MinIO verifies the streaming SHA-256 digest against `x-amz-checksum-sha256`. If a mismatch occurs, MinIO rejects the transfer with `400 InvalidDigest`.

### 6.3 Upload Confirmation & Consistency Verification
Client submits `POST /api/v1/files/upload-complete` with `upload_id` and MinIO `etag`.
1. Drogon executes non-blocking S3 `HeadObject` against MinIO.
2. Checks:
   - Object exists in bucket.
   - `Content-Length` matches `size_bytes` declared in Step 1.
   - ETag matches client header.
3. MySQL Transaction commits records:
   - `files` entry created or updated.
   - `file_versions` record inserted (`version_number = N+1`).
   - `audit_log` event recorded.
4. AMQP Event `file.uploaded` is dispatched to RabbitMQ exchange `drivex.events`.

---

## 7. AI Search & Conversational RAG Streaming Protocol

### 7.1 Hybrid Search & Reciprocal Rank Fusion (RRF)
Search queries evaluate both semantic meaning and exact keyword occurrences:
- **Dense Vector Retrieval**: Embeds query using `BAAI/bge-large-en-v1.5` (1024 dims), queries Qdrant with user isolation filter (`owner_id = user_id OR shared_user_ids CONTAINS user_id`).
- **Sparse Keyword Retrieval**: Executes MySQL `MATCH(name) AGAINST(:q IN BOOLEAN MODE)` on `files.name`.
- **Rank Fusion Algorithm**:
  $$RRF\_Score(d \in D) = \sum_{m \in \{dense, sparse\}} \frac{1}{k + rank_m(d)}$$
  Where smoothing constant $k = 60$.
- **Cross-Encoder Re-Ranking**: Top-50 candidates are re-scored using `BAAI/bge-reranker-large` to produce the final top-20 results.

### 7.2 Server-Sent Events (SSE) Protocol for RAG Assistant
Endpoint `POST /api/v1/chat` opens a persistent HTTP streaming connection with headers:
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache, no-transform
Connection: keep-alive
X-Accel-Buffering: no
```

#### Event Framing:
```
event: sources
data: [{"file_id":412,"name":"presentation.key","page":3,"relevance":0.94}]

event: token
data: {"delta":"According to "}

event: token
data: {"delta":"slide 3 of the presentation, revenue grew 18%."}

event: done
data: {"finish_reason":"stop","total_tokens":42}
```

#### Frontend HTMX SSE Integration:
```html
<div hx-ext="sse" 
     sse-connect="/api/v1/chat" 
     sse-swap="token" 
     hx-target="#chat-output" 
     hx-swap="beforeend">
  <div id="chat-output"></div>
</div>
```

---

## 8. Exhaustive Endpoint Reference

### 8.1 Authentication Endpoints

---

#### `POST /api/v1/auth/register`
- **Synopsis**: Register a new user account with default 15 GB quota.
- **Security**: None (Public).
- **Rate Limit**: 10 requests / minute per IP.
- **Request Headers**: `Content-Type: application/json`
- **Request Body**:
  ```json
  {
    "email": "jane.doe@example.com",
    "password": "SecurePassword2026!"
  }
  ```
- **Response `201 Created`**:
  ```json
  {
    "token_type": "Bearer",
    "access_token": "eyJhbGciOiJSUzI1NiIs...",
    "expires_in": 900,
    "refresh_token": "a4f89d3c7b2e10f823a49...",
    "refresh_expires_in": 2592000,
    "user": {
      "id": 105,
      "email": "jane.doe@example.com",
      "storage_quota_bytes": 16106127360,
      "storage_used_bytes": 0,
      "status": "active",
      "created_at": "2026-09-17T14:50:00Z"
    }
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: `INVALID_INPUT` (Invalid email format or password length < 8).
  - `409 Conflict`: `EMAIL_ALREADY_REGISTERED` (User already exists).

---

#### `POST /api/v1/auth/login`
- **Synopsis**: Authenticate user credentials, issue RS256 access token, set refresh token cookie.
- **Security**: None (Public).
- **Rate Limit**: 10 requests / minute per IP.
- **Request Body**:
  ```json
  {
    "email": "jane.doe@example.com",
    "password": "SecurePassword2026!"
  }
  ```
- **Response `200 OK`**:
  - **Headers**:
    `Set-Cookie: drivex_refresh_token=a4f89...; Path=/api/v1/auth; HttpOnly; Secure; SameSite=Strict; Max-Age=2592000`
  - **Body**: Same schema as `AuthResponse`.
- **Error Responses**:
  - `401 Unauthorized`: `INVALID_CREDENTIALS` (Argon2id hash verification failure).
  - `429 Too Many Requests`: `RATE_LIMIT_EXCEEDED` (Burst limit exceeded).

---

#### `POST /api/v1/auth/refresh`
- **Synopsis**: Rotate refresh token and obtain a new RS256 access token.
- **Security**: `RefreshTokenCookie` or `RefreshTokenHeader` (`X-Refresh-Token`).
- **Request Body** (Optional): `{ "refresh_token": "a4f89..." }`
- **Response `200 OK`**:
  ```json
  {
    "token_type": "Bearer",
    "access_token": "eyJhbGciOiJSUzI1NiIs...",
    "expires_in": 900,
    "refresh_token": "e8291bf02a4..."
  }
  ```
- **Error Responses**:
  - `401 Unauthorized`: `TOKEN_EXPIRED` or `TOKEN_REVOKED`.

---

#### `POST /api/v1/auth/logout`
- **Synopsis**: Revoke active session, add access token `jti` to Redis blacklist, clear cookies.
- **Security**: `BearerAuth`.
- **Response `200 OK`**:
  - **Headers**: `Set-Cookie: drivex_refresh_token=; Path=/api/v1/auth; Max-Age=0`
  - **Body**: `{ "message": "Successfully logged out" }`

---

#### `GET /api/v1/auth/me`
- **Synopsis**: Fetch profile information and live quota utilization for authenticated user.
- **Security**: `BearerAuth`.
- **Response `200 OK`**:
  ```json
  {
    "id": 105,
    "email": "jane.doe@example.com",
    "storage_quota_bytes": 16106127360,
    "storage_used_bytes": 2147483648,
    "percent_used": 13.33,
    "status": "active",
    "created_at": "2026-09-17T14:50:00Z"
  }
  ```

---

### 8.2 Folder Hierarchy Endpoints

---

#### `POST /api/v1/folders`
- **Synopsis**: Create a folder under `parent_id` (or root if omitted).
- **Security**: `BearerAuth` (Role: `editor` on parent).
- **Request Body**:
  ```json
  {
    "name": "Quarterly Reports",
    "parent_id": 42
  }
  ```
- **Response `201 Created`**:
  ```json
  {
    "id": 88,
    "name": "Quarterly Reports",
    "parent_id": 42,
    "owner_id": 105,
    "is_trashed": false,
    "trashed_at": null,
    "created_at": "2026-09-17T14:55:00Z",
    "updated_at": "2026-09-17T14:55:00Z"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: `MAX_DEPTH_EXCEEDED` (Tree nesting depth > 32).
  - `404 Not Found`: `FOLDER_NOT_FOUND` (Parent folder does not exist).
  - `409 Conflict`: `RESOURCE_ALREADY_EXISTS` (Duplicate name in directory).

---

#### `GET /api/v1/folders`
- **Synopsis**: List folders matching parent query with pagination.
- **Security**: `BearerAuth`.
- **Query Parameters**:
  - `parent_id` (integer, optional): Parent folder filter (null = root).
  - `page` (integer, default: 1): 1-based page number.
  - `limit` (integer, default: 50, max: 100): Page size.
  - `sort_by` (string, enum: `name`, `created_at`, `updated_at`, default: `name`).
  - `sort_order` (string, enum: `asc`, `desc`, default: `asc`).
- **Response `200 OK`**: Returns `FolderListingResponse`.

---

#### `GET /api/v1/folders/{id}`
- **Synopsis**: Get folder metadata, breadcrumb path array, and user effective role.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Path Parameters**: `id` (integer, required).
- **Response `200 OK`**:
  ```json
  {
    "id": 88,
    "name": "Quarterly Reports",
    "parent_id": 42,
    "owner_id": 105,
    "is_trashed": false,
    "trashed_at": null,
    "created_at": "2026-09-17T14:55:00Z",
    "updated_at": "2026-09-17T14:55:00Z",
    "breadcrumbs": [
      { "id": null, "name": "Home" },
      { "id": 42, "name": "Projects" },
      { "id": 88, "name": "Quarterly Reports" }
    ],
    "effective_role": "owner"
  }
  ```

---

#### `PATCH /api/v1/folders/{id}`
- **Synopsis**: Rename folder.
- **Security**: `BearerAuth` (Role: `editor`).
- **Request Body**: `{ "name": "Annual Reports 2026" }`
- **Response `200 OK`**: Returns updated `Folder`.

---

#### `DELETE /api/v1/folders/{id}`
- **Synopsis**: Soft-delete folder to Trash (`permanent=false`) or purge permanently (`permanent=true`).
- **Security**: `BearerAuth` (Role: `editor` for soft-delete; `owner` for purge).
- **Query Parameters**: `permanent` (boolean, default: false).
- **Response `204 No Content`**.

---

#### `GET /api/v1/folders/{id}/children`
- **Synopsis**: List immediate non-trashed subfolders and files inside folder `{id}`.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Response `200 OK`**: Returns `FolderChildrenResponse`.

---

#### `GET /api/v1/folders/{id}/breadcrumbs`
- **Synopsis**: Resolve full ancestor path from root down to folder `{id}` via Recursive CTE.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Response `200 OK`**: Returns array of `Breadcrumb` objects.

---

#### `POST /api/v1/folders/{id}/move`
- **Synopsis**: Relocate folder to `new_parent_id` with cycle detection and depth limit validation.
- **Security**: `BearerAuth` (Role: `editor` on source and target).
- **Request Body**: `{ "new_parent_id": 15 }`
- **Response `200 OK`**: Returns relocated `Folder`.
- **Error Responses**:
  - `400 Bad Request`: `CYCLIC_FOLDER_HIERARCHY` (Target is inside source subtree).
  - `400 Bad Request`: `MAX_DEPTH_EXCEEDED` (Exceeds 32-level ceiling).
  - `403 Forbidden`: `RESOURCE_LOCKED` (Concurrent mutation underway).

---

### 8.3 File Operations & Pre-Signed URL Endpoints

---

#### `POST /api/v1/files/upload-url`
- **Synopsis**: Negotiate pre-signed S3 PUT URL for direct-to-MinIO streaming.
- **Security**: `BearerAuth` (Role: `editor` on target folder).
- **Request Body**:
  ```json
  {
    "folder_id": 88,
    "name": "quarterly_presentation.key",
    "size_bytes": 26214400,
    "mime_type": "application/vnd.apple.keynote",
    "checksum_sha256": "4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"
  }
  ```
- **Response `200 OK`**:
  ```json
  {
    "upload_id": "upl_8a92f038-5183-4a6c-941e-8219df0e318d",
    "upload_url": "https://s3.drivex.dev/drivex-blobs/blobs/105/2026-09/upl_8a92f038.key?X-Amz-Algorithm=AWS4-HMAC-SHA256&...",
    "storage_key": "blobs/105/2026-09/upl_8a92f038.key",
    "expires_at": "2026-09-17T15:15:00Z"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: `STORAGE_QUOTA_EXCEEDED`.
  - `413 Payload Too Large`: `FILE_SIZE_EXCEEDED` (Size > 5 GB).
  - `503 Service Unavailable`: `STORAGE_UNAVAILABLE` (MinIO unreachable).

---

#### `POST /api/v1/files/upload-complete`
- **Synopsis**: Verify S3 `HeadObject`, commit file version, reconcile quota, emit RabbitMQ event.
- **Security**: `BearerAuth`.
- **Request Body**:
  ```json
  {
    "upload_id": "upl_8a92f038-5183-4a6c-941e-8219df0e318d",
    "etag": "\"5d41402abc4b2a76b9719d911017c592\""
  }
  ```
- **Response `201 Created`**: Returns `UploadCompleteResponse` containing `file` metadata.
- **Error Responses**:
  - `400 Bad Request`: `STORAGE_OBJECT_MISSING` (Bytes not uploaded to MinIO).
  - `400 Bad Request`: `CHECKSUM_MISMATCH` (ETag or length discrepancy).

---

#### `GET /api/v1/files/{id}`
- **Synopsis**: Get file metadata, current version, and classification tags.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Response `200 OK`**: Returns `FileDetail`.

---

#### `PATCH /api/v1/files/{id}`
- **Synopsis**: Rename file or move to another folder.
- **Security**: `BearerAuth` (Role: `editor`).
- **Request Body**: `{ "name": "presentation_final.key", "folder_id": 92 }`
- **Response `200 OK`**: Returns updated `File`.

---

#### `DELETE /api/v1/files/{id}`
- **Synopsis**: Move file to Trash (`permanent=false`) or purge immediately (`permanent=true`).
- **Security**: `BearerAuth` (Role: `editor` for soft-delete; `owner` for purge).
- **Query Parameters**: `permanent` (boolean, default: false).
- **Response `204 No Content`**.

---

#### `POST /api/v1/files/{id}/restore`
- **Synopsis**: Restore soft-deleted file from Trash.
- **Security**: `BearerAuth` (Role: `editor`).
- **Response `200 OK`**: Returns restored `File`.

---

#### `GET /api/v1/files/{id}/download-url`
- **Synopsis**: Generate pre-signed S3 GET URL (5-minute TTL) for direct binary download.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Query Parameters**:
  - `version_id` (integer, optional): Target specific historical version.
  - `disposition` (string, enum: `inline`, `attachment`, default: `attachment`).
- **Response `200 OK`**:
  ```json
  {
    "download_url": "https://s3.drivex.dev/drivex-blobs/blobs/105/2026-09/upl_8a92f038.key?X-Amz-Algorithm=...",
    "expires_at": "2026-09-17T15:10:00Z",
    "filename": "quarterly_presentation.key",
    "size_bytes": 26214400,
    "mime_type": "application/vnd.apple.keynote"
  }
  ```

---

#### `GET /api/v1/files/{id}/versions`
- **Synopsis**: List historical version revisions for file `{id}`.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Response `200 OK`**: Returns `FileVersionListResponse`.

---

#### `GET /api/v1/files/{id}/versions/{version_id}/download-url`
- **Synopsis**: Generate pre-signed S3 GET URL for a specific historical file version.
- **Security**: `BearerAuth` (Role: `viewer`).
- **Response `200 OK`**: Returns `DownloadUrlResponse`.

---

### 8.4 Permissions & RBAC Endpoints

---

#### `GET /api/v1/permissions`
- **Synopsis**: List direct permission grants on a file or folder.
- **Security**: `BearerAuth` (Role: `owner`).
- **Query Parameters**: `resource_type` (`file` | `folder`), `resource_id` (integer).
- **Response `200 OK`**: Returns `PermissionListResponse`.

---

#### `POST /api/v1/permissions`
- **Synopsis**: Grant access permission on a resource to a collaborator.
- **Security**: `BearerAuth` (Role: `owner`).
- **Request Body**:
  ```json
  {
    "resource_type": "folder",
    "resource_id": 88,
    "user_email": "collaborator@example.com",
    "role": "editor"
  }
  ```
- **Response `201 Created`**: Returns created `Permission`.

---

#### `GET /api/v1/permissions/{id}`
- **Synopsis**: Get details of an explicit permission grant.
- **Security**: `BearerAuth` (Role: `owner`).
- **Response `200 OK`**: Returns `Permission`.

---

#### `PATCH /api/v1/permissions/{id}`
- **Synopsis**: Update role (`viewer`, `editor`, `owner`) on an existing grant.
- **Security**: `BearerAuth` (Role: `owner`).
- **Request Body**: `{ "role": "viewer" }`
- **Response `200 OK`**: Returns updated `Permission`.

---

#### `DELETE /api/v1/permissions/{id}`
- **Synopsis**: Revoke access permission grant.
- **Security**: `BearerAuth` (Role: `owner`).
- **Response `204 No Content`**.

---

### 8.5 Share Links Endpoints

---

#### `POST /api/v1/share-links`
- **Synopsis**: Create public/expiring link with optional Argon2id password and download cap.
- **Security**: `BearerAuth` (Role: `editor` or `owner`).
- **Request Body**:
  ```json
  {
    "resource_type": "file",
    "resource_id": 412,
    "permission_role": "viewer",
    "password": "ClientPasscode2026!",
    "expires_at": "2026-09-24T15:00:00Z",
    "max_downloads": 10
  }
  ```
- **Response `201 Created`**: Returns `ShareLink`.

---

#### `GET /api/v1/share-links`
- **Synopsis**: List share links created by authenticated user.
- **Security**: `BearerAuth`.
- **Response `200 OK`**: Returns `ShareLinkListResponse`.

---

#### `GET /api/v1/share-links/{token}`
- **Synopsis**: Public endpoint to resolve share link status and password requirement.
- **Security**: None (Public).
- **Response `200 OK`**: Returns `ShareLinkPublicResponse`.
- **Error Responses**:
  - `404 Not Found`: `SHARE_LINK_NOT_FOUND`.
  - `410 Gone`: `SHARE_LINK_EXPIRED` (Expired or download limit exceeded).

---

#### `POST /api/v1/share-links/{token}/verify-password`
- **Synopsis**: Verify Argon2id password for protected share link; issue 5-minute download token.
- **Security**: None (Public).
- **Request Body**: `{ "password": "ClientPasscode2026!" }`
- **Response `200 OK`**:
  ```json
  {
    "download_token": "sc_99a8b12f710a48...",
    "expires_in": 300
  }
  ```
- **Error Responses**:
  - `401 Unauthorized`: `INVALID_CREDENTIALS`.

---

#### `GET /api/v1/share-links/{token}/download`
- **Synopsis**: Download shared file (requires `download_token` if password-protected).
- **Security**: `ShareTokenHeader` (`X-Share-Token`) or `download_token` query param.
- **Response `200 OK`**: Returns `DownloadUrlResponse`.
- **Response `302 Found`**: Direct redirect to MinIO S3 signed URL.

---

#### `DELETE /api/v1/share-links/{token}`
- **Synopsis**: Deactivate and revoke share link immediately.
- **Security**: `BearerAuth` (Role: `owner`).
- **Response `204 No Content`**.

---

### 8.6 Trash Management Endpoints

---

#### `GET /api/v1/trash`
- **Synopsis**: List trashed files and folders with days remaining until 30-day purge.
- **Security**: `BearerAuth`.
- **Response `200 OK`**: Returns `TrashListResponse`.

---

#### `POST /api/v1/trash/{item_type}/{id}/restore`
- **Synopsis**: Restore a specific file or folder from Trash.
- **Security**: `BearerAuth` (Role: `editor`).
- **Path Parameters**: `item_type` (`file` | `folder`), `id` (integer).
- **Response `200 OK`**: Returns `RestoreTrashResponse`.

---

#### `DELETE /api/v1/trash/{item_type}/{id}`
- **Synopsis**: Permanently purge single trashed item from database and MinIO.
- **Security**: `BearerAuth` (Role: `owner`).
- **Response `204 No Content`**.

---

#### `POST /api/v1/trash/empty`
- **Synopsis**: Permanently empty entire Trash, purge physical blobs, and reclaim storage quota.
- **Security**: `BearerAuth`.
- **Response `200 OK`**:
  ```json
  {
    "files_purged": 14,
    "folders_purged": 3,
    "bytes_reclaimed": 157286400
  }
  ```

---

### 8.7 AI & Search Endpoints

---

#### `GET /api/v1/search`
- **Synopsis**: Hybrid semantic vector search and MySQL FULLTEXT keyword search with RRF fusion.
- **Security**: `BearerAuth`.
- **Query Parameters**:
  - `q` (string, required): Search query.
  - `type` (string, enum: `hybrid`, `semantic`, `keyword`, default: `hybrid`).
  - `folder_id` (integer, optional): Scope search to subtree.
  - `limit` (integer, default: 20, max: 100).
  - `offset` (integer, default: 0).
- **Response `200 OK`**:
  ```json
  {
    "query": "quarterly financial results 2025",
    "search_type": "hybrid",
    "total_hits": 1,
    "results": [
      {
        "file_id": 304,
        "name": "Q3_Report_Final.pdf",
        "mime_type": "application/pdf",
        "size_bytes": 1048576,
        "score": 0.924,
        "snippet": "...consolidated revenue for Q3 grew 18% year-over-year to $4.2M...",
        "path": "/Finance/Reports/2025"
      }
    ]
  }
  ```
- **Error Responses**:
  - `503 Service Unavailable`: `AI_ASSISTANT_UNAVAILABLE` (Falls back to keyword search).

---

#### `POST /api/v1/chat`
- **Synopsis**: RAG "Chat with your Drive" conversational assistant streaming tokens via Server-Sent Events (SSE).
- **Security**: `BearerAuth`.
- **Request Headers**:
  - `Accept: text/event-stream`
  - `Content-Type: application/json`
- **Request Body**:
  ```json
  {
    "message": "What were the total marketing expenses reported in the Q3 summary?",
    "folder_id": 88,
    "history": [
      { "role": "user", "content": "Hello" },
      { "role": "assistant", "content": "How can I help you with your files today?" }
    ]
  }
  ```
- **Response `200 OK` (`text/event-stream; charset=utf-8`)**:
  - **Headers**:
    `X-Accel-Buffering: no`
    `Cache-Control: no-cache, no-transform`
    `Connection: keep-alive`
  - **Event Stream**:
    ```
    event: sources
    data: [{"file_id":304,"name":"Q3_Report_Final.pdf","page":4,"relevance":0.94}]

    event: token
    data: {"delta":"According to "}

    event: token
    data: {"delta":"page 4 of the Q3 Report, "}

    event: token
    data: {"delta":"marketing expenses were $450,000."}

    event: done
    data: {"finish_reason":"stop","total_tokens":82}
    ```
- **Error Responses**:
  - `503 Service Unavailable`: `AI_ASSISTANT_UNAVAILABLE` (LLM/Qdrant unreachable).
