// k6 load test — Stage 1: 50 concurrent workers, metadata-only ops.
// Run with: k6 run stage1_50workers.js
import http from "k6/http";
import { check, sleep } from "k6";

export const options = { vus: 50, duration: "60s" };
const BASE = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const res = http.get(`${BASE}/folders`, {
    headers: { Authorization: `Bearer ${__ENV.TEST_JWT}` },
  });
  check(res, { "status is 200": (r) => r.status === 200 });
  sleep(1);
}
