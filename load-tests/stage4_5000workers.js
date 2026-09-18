// k6 load test — Stage 4: 5,000 concurrent workers, Conversational RAG & Streaming Chat.
// Target: Time-to-First-Token (TTFT) < 800ms, p99 < 800ms, error rate < 1.0%.
// Run with: k6 run stage4_5000workers.js

import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 5000,
  duration: "180s",
  thresholds: {
    http_req_failed: ["rate < 0.01"],
    "http_req_duration{type:chat_stream}": ["p(99) < 800"],
    checks: ["rate > 0.99"],
  },
};

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const JWT = __ENV.TEST_JWT || "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.dummy";

export default function () {
  const headers = {
    Authorization: `Bearer ${JWT}`,
    "Content-Type": "application/json",
    Accept: "text/event-stream",
  };

  const queries = [
    "Summarize the quarterly system architecture performance findings",
    "What are the encryption standards implemented for file versions?",
    "How does the direct client-to-MinIO pre-signed URL workflow operate?",
    "List all permission constraints for viewer roles in shared directories",
  ];

  const query = queries[Math.floor(Math.random() * queries.length)];

  const chatPayload = JSON.stringify({
    query: query,
    conversation_id: `conv_${__VU}`,
  });

  const res = http.post(`${BASE}/api/v1/chat`, chatPayload, {
    headers: headers,
    tags: { type: "chat_stream" },
    timeout: "10s",
  });

  check(res, {
    "chat status is 200": (r) => r.status === 200,
    "response is event-stream": (r) => r.headers["Content-Type"] && r.headers["Content-Type"].includes("text/event-stream"),
  });

  sleep(1);
}
