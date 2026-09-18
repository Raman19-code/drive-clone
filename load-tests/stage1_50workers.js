// k6 load test — Stage 1: 50 concurrent workers, metadata-only ops.
// Target: p99 < 15ms on metadata endpoints, error rate < 0.1%.
// Run with: k6 run stage1_50workers.js

import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 50,
  duration: "60s",
  thresholds: {
    http_req_failed: ["rate < 0.001"],
    "http_req_duration{type:metadata}": ["p(99) < 15"],
    checks: ["rate > 0.99"],
  },
};

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const JWT = __ENV.TEST_JWT || "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.dummy";

export default function () {
  const headers = {
    Authorization: `Bearer ${JWT}`,
    "Content-Type": "application/json",
  };

  // 1. Root folder contents listing
  const listRes = http.get(`${BASE}/api/v1/folders?page=1&limit=20&sort=name`, {
    headers: headers,
    tags: { type: "metadata" },
  });
  check(listRes, {
    "list status is 200": (r) => r.status === 200,
    "list response has items": (r) => r.body && r.body.includes("items"),
  });

  // 2. User profile and storage quota lookup
  const meRes = http.get(`${BASE}/api/v1/users/me`, {
    headers: headers,
    tags: { type: "metadata" },
  });
  check(meRes, {
    "user info status is 200": (r) => r.status === 200,
  });

  sleep(1);
}
