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

## Interactive provisioning follow-up

The `.mjs` setup replacement was validated with `npm run typecheck`, the independent worker build, and `npm test` (58 passing tests, including 11 setup/terminal tests). `npm run test:integration` now builds the production image and tests the direct CLI plus npm option forwarding against disposable JetStream: first creation, repeat verification, missing input refusal, stream/consumer mismatch details, unchanged existing resources and unchanged input stream. The same scenario executes the packaged script in Docker, starts the normal compiled worker under its restricted identity, and verifies non-root runtime and clean SIGTERM exit.

A real interactive terminal smoke ran `docker run --rm -it --network container:fionas-event-workers-nats-1 --entrypoint node fionas-inquiry-to-commerce:test scripts/nats-setup.mjs` against the local development broker. Address and username/password selection succeeded, the password did not appear in terminal output, the complete managed configuration was displayed, and both resources reported verified with exit code 0. Synthetic public local-development credentials were used. The broker was stopped afterward with its existing data volume preserved.

Setup tests cover credentials-file defaults, isolated authentication selection, password defaults without disclosure, explicit unauthenticated selection, invalid options/credentials/embedded-credential URLs, raw terminal restoration, backspace, Ctrl+C/Ctrl+D/abort/EOF cancellation, nonterminal refusal and help output. Interactive setup is operator-only and remains separate from normal worker startup.

## Stream viewers follow-up

Validated `npm run typecheck`, `npm test` (62 passing tests), and `npm run test:integration` including the independent build and production Docker build. The real-NATS scenario runs both live viewers, proves they skip retained history by default, receives new messages from each stream, and verifies exact payload/metadata output and cleanup after `--limit 1`. It runs both exact npm commands with `--all`, `--non-interactive` and npm's silent mode, checking that stdout is parseable JSON without command banners. Stream configuration and the worker durable's ACK floor, delivered sequence and ACK-pending state remain unchanged. Docker checks cover packaged replay and SIGTERM cleanup of a live viewer with exit code 0 and no leftover temporary consumer.

Additional unit cases cover stream selection, option validation, help/nonterminal behavior, JSON-line formatting, multi-value headers, text and binary payloads. Operator viewers intentionally display stored payloads; they do not invoke commerce or acknowledge the business durable.

## Corrective reliability hardening

`npm run typecheck` passed. `npm test` passed 71 tests (zero failed/skipped). `npm run test:integration` passed four real-NATS scenarios and its production Docker build. That command already runs in CI and discovers the new failure-recovery test file; no dependency skips or release/deployment automation were added. `node --check scripts/nats-retry-inquiry.mjs` and `git diff --check` passed. Temporary ignored YAML/Ajv tools additionally validated AsyncAPI syntax/references, both message examples, generated payloads and publication headers against the owned schemas; no runtime dependency was added.

Before correction, the disposable NATS 2.14.7 experiment observed A in the sole ACK-pending slot and B pending behind it. A delayed NAK prevented B's fetch. Reconnecting did not help; deliveries 2–10 still returned A. After the final deadline/MaxDeliver check, B arrived automatically. At that point num_ack_pending was one for B and num_pending zero; B's ACK reduced ACK-pending to zero. A stayed retained. The same durable/configuration is used, with deadlines accelerated only in tests.

Corrected regressions prove failure PubAck before TERM, deterministic failure bytes through publication failure/lost PubAck/lost TERM/restart, duplicate PubAck acceptance, retained original inspection and immediate valid-message progress after disposition. Real HTTP tests cover permanent 422, transient 503 followed by recovery with identical idempotency/body, ten-attempt exhaustion followed by controlled halt and later progress, targeted recovery of both TERMed and exhausted originals, and missing-sequence refusal without processing another inquiry. Targeted recovery leaves primary delivered/ACK-floor state unchanged.

The actual entrypoint exits 1 on a real stub 403 after one inquiry POST, leaves both sources retained, emits no permanent failure or persistence event, and never processes the next queued customer. Existing lost-HTTP-response, success-PubAck-loss, duplicate publication, restart/reconnect, independent consumers and graceful-shutdown regressions still pass. Docker checks include the packaged recovery CLI and existing setup/tail/worker startup lifecycle.

Current upstream main refs were fetched and remained at the audited web/commerce SHAs. Their event, OpenAPI, SERVICE and receipt contracts were rechecked without upstream edits. The full real-commerce/web smoke was documented but not executed; it requires a privately provisioned local backend/SERVICE and web configuration. NATS production permissions also require operator application of the new explicit failure-publish subject. No consumer settings changed and no durable reset is needed.
