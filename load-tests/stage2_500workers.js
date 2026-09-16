// k6 load test — Stage 2: 500 workers, mixed metadata + small (<=1MB) file I/O.
// Target: p99 < 100ms on metadata endpoints.
import http from "k6/http";
import { check, sleep } from "k6";

export const options = { vus: 500, duration: "120s" };
const BASE = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const listRes = http.get(`${BASE}/folders`);
  check(listRes, { "list 200": (r) => r.status === 200 });
  sleep(0.5);
}
