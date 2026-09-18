// k6 load test — Stage 2b: 500 workers, concurrent permission evaluation & ancestor tree inheritance.
// Target: p99 < 10ms on permission evaluation endpoints (Redis cache-aside), error rate < 0.1%.
// Run with: k6 run stage2b_permissions.js

import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 500,
  duration: "60s",
  thresholds: {
    http_req_failed: ["rate < 0.001"],
    "http_req_duration{type:permission_check}": ["p(99) < 10"],
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

  // Simulate concurrent queries on nested folder/file resources across deep hierarchies
  const targetFolderId = (__VU % 50) + 1;

  // 1. Check folder access & recursive breadcrumbs
  const folderRes = http.get(`${BASE}/api/v1/folders/${targetFolderId}`, {
    headers: headers,
    tags: { type: "permission_check" },
  });
  check(folderRes, {
    "folder permission check status ok": (r) => r.status === 200 || r.status === 403,
  });

  // 2. Check effective resource permissions
  const permRes = http.get(`${BASE}/api/v1/permissions/effective?resource_type=folder&resource_id=${targetFolderId}`, {
    headers: headers,
    tags: { type: "permission_check" },
  });
  check(permRes, {
    "effective permission check ok": (r) => r.status === 200 || r.status === 403,
  });

  sleep(0.1);
}
