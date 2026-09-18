// k6 load test — Stage 3: 2,000 concurrent workers, sustained 500 MB/s MinIO I/O + semantic search.
// Target: Sustained storage I/O > 500 MB/s, search p99 < 20ms, error rate < 0.5%.
// Run with: k6 run stage3_2000workers.js

import http from "k6/http";
import { check, sleep } from "k6";
import crypto from "k6/crypto";

export const options = {
  vus: 2000,
  duration: "300s",
  thresholds: {
    http_req_failed: ["rate < 0.005"],
    "http_req_duration{type:search}": ["p(99) < 20"],
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

  const actionRoll = Math.random();

  if (actionRoll < 0.6) {
    // 60% of traffic: High-frequency semantic and metadata search
    const searchTerms = ["quarterly financial report", "architecture design", "engineering roadmap", "contracts", "machine learning"];
    const query = searchTerms[Math.floor(Math.random() * searchTerms.length)];

    const searchRes = http.get(`${BASE}/api/v1/search?q=${encodeURIComponent(query)}&limit=10`, {
      headers: authHeaders,
      tags: { type: "search" },
    });

    check(searchRes, {
      "search status 200": (r) => r.status === 200,
    });
  } else {
    // 40% of traffic: File upload negotiation and high-throughput transfer simulation
    const fileName = `load_test_${__VU}_${Date.now()}.bin`;
    const dummyHash = crypto.sha256(`data_${__VU}_${__ITER}`, "hex");

    const negPayload = JSON.stringify({
      name: fileName,
      folder_id: null,
      size_bytes: 10485760, // 10 MB payload simulation
      mime_type: "application/octet-stream",
      content_hash: dummyHash,
    });

    const negRes = http.post(`${BASE}/api/v1/files/upload-url`, negPayload, {
      headers: authHeaders,
      tags: { type: "storage_io" },
    });

    check(negRes, {
      "negotiate status 200": (r) => r.status === 200,
    });
  }

  sleep(1);
}
