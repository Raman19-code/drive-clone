# Load Tests

Staged k6 benchmark suite, modeled on prior API/WebSocket load-testing work.
Stages 1-2 are stubbed here; add `stage3_2000workers.js` (medium files),
`stage4_5000workers.js` (large files + resume-on-failure), and
`stage5_10000workers.js` (full mixed workload) as the corresponding build
phases land — see `docs/architecture.md` for target metrics per stage.
