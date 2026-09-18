// k6 load test — Stage 2: 500 workers, mixed metadata + small (<=1MB) file upload/download I/O.
// Target: p99 < 50ms on API negotiation endpoints, error rate < 0.1%.
// Run with: k6 run stage2_500workers.js

import http from "k6/http";
import { check, sleep } from "k6";
import crypto from "k6/crypto";

export const options = {
  vus: 500,
  duration: "120s",
  thresholds: {
    http_req_failed: ["rate < 0.001"],
    "http_req_duration{type:api_negotiation}": ["p(99) < 50"],
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

  // 1. Browse parent folder metadata
  const listRes = http.get(`${BASE}/api/v1/folders?page=1&limit=20&sort=name`, {
    headers: authHeaders,
    tags: { type: "api_negotiation" },
  });
  check(listRes, {
    "list status is 200": (r) => r.status === 200,
  });

  // 2. Generate 64KB random payload for upload simulation
  const payloadBytes = "DriveX benchmark simulated file content ".repeat(1600);
  const sizeBytes = payloadBytes.length;
  const sha256Hex = crypto.sha256(payloadBytes, "hex");
  const fileName = `bench_${__VU}_${__ITER}_${Date.now()}.txt`;

  // Step 1: Negotiate pre-signed upload URL
  const negPayload = JSON.stringify({
    name: fileName,
    folder_id: null,
    size_bytes: sizeBytes,
    mime_type: "text/plain",
    content_hash: sha256Hex,
  });

  const negRes = http.post(`${BASE}/api/v1/files/upload-url`, negPayload, {
    headers: authHeaders,
    tags: { type: "api_negotiation" },
  });

  const negOk = check(negRes, {
    "negotiate status is 200": (r) => r.status === 200,
    "upload_url present": (r) => r.json("upload_url") !== undefined,
  });

  if (negOk) {
    const uploadUrl = negRes.json("upload_url");
    const uploadId = negRes.json("upload_id");

    // Step 2: Stream binary bytes directly to MinIO
    const putRes = http.put(uploadUrl, payloadBytes, {
      headers: {
        "Content-Type": "text/plain",
        "x-amz-content-sha256": sha256Hex,
      },
      tags: { type: "s3_direct_stream" },
    });

    const putOk = check(putRes, {
      "minio put status is 200": (r) => r.status === 200,
    });

    if (putOk) {
      const etag = putRes.headers["ETag"] || putRes.headers["Etag"] || `"${sha256Hex.slice(0, 32)}"`;

      // Step 3: Confirm upload completion
      const compPayload = JSON.stringify({
        upload_id: uploadId,
        etag: etag,
      });

      const compRes = http.post(`${BASE}/api/v1/files/upload-complete`, compPayload, {
        headers: authHeaders,
        tags: { type: "api_negotiation" },
      });

      check(compRes, {
        "upload complete status is 201": (r) => r.status === 201,
      });
    }
  }

  sleep(0.5);
}
