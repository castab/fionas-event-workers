# Bootstrap validation — 2026-10-14

Validated with Node.js 26.9.0, npm 11.18.0, Docker Desktop's Linux engine 29.6.2 and real NATS 2.14.7. The production image's official Node 26 Alpine runtime was also exercised.

| Command | Result |
|---|---|
| `npm install` | Pinned dependency install; no reported vulnerabilities |
| `npm run typecheck` | Passed |
| `npm test` | 47 tests passed; zero failed/skipped |
| `npm run build --workspace @fionas/inquiry-to-commerce` | Independent worker compilation passed |
| `npm run test:integration` | Disposable real-JetStream scenario passed; zero skipped |
| `npm run nats:up` | Local development broker healthy |
| `node scripts/nats-local-init.ts` | Local initializer passed |
| `npm run nats:setup` (twice, development operator environment) | Both matched; input configuration unchanged |
| `docker build -f workers/inquiry-to-commerce/Dockerfile -t fionas-inquiry-to-commerce:bootstrap .` | Production multistage image built |
| Compiled-image smoke with synthetic HTTP stub, local NATS and `docker stop --time 60` | Non-root `node` user; SIGTERM during outstanding HTTP → valid receipt → PubAck → confirmed ACK → `worker_stopped`; exit code 0 |
| `git diff --check` | Passed |

Unit tests cover deterministic projection/outcomes, additive field compatibility, malformed/unsupported events, exact decimal-string preservation, token exchange/cache/expiry/single-401 refresh, no 403 refresh, rejection classes, HTTP timeout including stalled response bodies, redirects, invalid token/receipt responses, HTTP aborts, ordered publication/ACK, duplicate PubAck, exhaustion logging and bounded lifecycle behavior.

The real-NATS scenario verifies disjoint streams, unchanged upstream input configuration, repeatable setup and mismatch refusal, restricted runtime INFO/NEXT/publish/ACK permissions, exclusive input filter, a lost HTTP response after simulated commit, outgoing failure, lost PubAck followed by duplicate success, deterministic identical HTTP keys/bodies and outcome bytes, independent future consumers, client interruption/redelivery, durable position across client and server restart, reconnection, and the actual worker entrypoint. Deliberate NAK delays and one auxiliary consumer's ACK wait are accelerated only in the test.

Native Windows process termination cannot emulate POSIX SIGTERM reliably. Portable lifecycle tests verify bounded completion/abort on all platforms; the actual Linux Docker smoke verified a real signal during in-flight work. Linux CI also exercises the entrypoint's SIGTERM cleanup.

No production services or secrets were used. The real commerce backend smoke remains optional and requires an operator-provisioned local SERVICE and backend; see `manual-smoke.md`. Production provisioning, credentials and deployment were intentionally not performed. HTTP/NATS atomicity and windowed deduplication limitations are documented in README and operations.
