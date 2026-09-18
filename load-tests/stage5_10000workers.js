// k6 load test — Stage 5: 10,000 concurrent workers, Full Mixed Enterprise Workload.
// Target: metadata p99 < 50ms, search p99 < 35ms, TTFT < 1000ms, error rate < 0.1%.
// Run with: k6 run load-tests/stage5_10000workers.js

import http from "k6/http";
import { check, sleep } from "k6";
import crypto from "k6/crypto";

export const options = {
  vus: 10000,
  duration: __ENV.TEST_DURATION || "300s", // 300s standard / 4h soak configurable via env
  thresholds: {
    http_req_failed: ["rate < 0.001"],
    "http_req_duration{type:metadata}": ["p(99) < 50"],
    "http_req_duration{type:search}": ["p(99) < 35"],
    "http_req_duration{type:chat_stream}": ["p(95) < 1000"],
    checks: ["rate > 0.99"],
  },
};

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const JWT = __ENV.TEST_JWT || "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.dummy";

export default function () {
  const authHeaders = {
    Authorization: `Bearer ${JWT}`,
    "Content-Type": "application/json",
  };

  const roll = Math.random();

  if (roll < 0.40) {
    // 1. 40% Directory Browsing & Folder Tree Navigation
    const page = Math.floor(Math.random() * 5) + 1;
    const res = http.get(`${BASE}/api/v1/folders?page=${page}&limit=20&sort=name`, {
      headers: authHeaders,
      tags: { type: "metadata" },
    });
    check(res, {
      "browsing status 200": (r) => r.status === 200,
    });
  } else if (roll < 0.65) {
    // 2. 25% File Metadata Retrieval & Permissions Resolution
    const targetFolderId = (__VU % 100) + 1;
    const res = http.get(`${BASE}/api/v1/permissions/effective?resource_type=folder&resource_id=${targetFolderId}`, {
      headers: authHeaders,
      tags: { type: "metadata" },
    });
    check(res, {
      "perm check status ok": (r) => r.status === 200 || r.status === 403,
    });
  } else if (roll < 0.80) {
    // 3. 15% Pre-Signed Upload Negotiation & Data Plane Streaming
    const fileName = `enterprise_bench_${__VU}_${Date.now()}.bin`;
    const dummyHash = crypto.sha256(`blob_${__VU}_${__ITER}`, "hex");
    const negPayload = JSON.stringify({
      name: fileName,
      folder_id: null,
      size_bytes: 1048576, // 1MB payload
      mime_type: "application/octet-stream",
      content_hash: dummyHash,
    });

    const negRes = http.post(`${BASE}/api/v1/files/upload-url`, negPayload, {
      headers: authHeaders,
      tags: { type: "metadata" },
    });

    const negOk = check(negRes, {
      "upload negotiation 200": (r) => r.status === 200,
    });

    if (negOk && negRes.json("upload_url")) {
      const uploadUrl = negRes.json("upload_url");
      const uploadId = negRes.json("upload_id");

      const putRes = http.put(uploadUrl, "DriveX 1MB Enterprise Benchmark Byte Payload", {
        headers: {
          "Content-Type": "application/octet-stream",
          "x-amz-content-sha256": dummyHash,
        },
        tags: { type: "storage_io" },
      });

      if (check(putRes, { "minio put 200": (r) => r.status === 200 })) {
        const etag = putRes.headers["ETag"] || `"${dummyHash.slice(0, 32)}"`;
        http.post(`${BASE}/api/v1/files/upload-complete`, JSON.stringify({ upload_id: uploadId, etag: etag }), {
          headers: authHeaders,
          tags: { type: "metadata" },
        });
      }
    }
  } else if (roll < 0.92) {
    // 4. 12% Semantic Vector Search Queries
    const queries = ["compliance policy", "cloud architecture", "vector embedding", "financial audit", "quarterly earnings"];
    const query = queries[Math.floor(Math.random() * queries.length)];
    const searchRes = http.get(`${BASE}/api/v1/search?q=${encodeURIComponent(query)}&limit=10`, {
      headers: authHeaders,
      tags: { type: "search" },
    });
    check(searchRes, {
      "search status 200": (r) => r.status === 200,
    });
  } else {
    // 5. 8% Interactive RAG Conversational Chat Streams over SSE
    const chatQueries = [
      "Summarize data retention guidelines",
      "What are the disaster recovery SLAs?",
      "Explain the pre-signed upload security invariant",
    ];
    const chatQuery = chatQueries[Math.floor(Math.random() * chatQueries.length)];
    const chatRes = http.post(`${BASE}/api/v1/chat`, JSON.stringify({ query: chatQuery, conversation_id: `conv_${__VU}` }), {
      headers: { ...authHeaders, Accept: "text/event-stream" },
      tags: { type: "chat_stream" },
      timeout: "10s",
    });
    check(chatRes, {
      "chat status 200": (r) => r.status === 200,
    });
  }

  sleep(1);
}
