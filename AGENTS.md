# Contributor instructions

Keep this repository a small TypeScript/Node.js 26 npm-workspace collection of independently deployable integration workers.

- `fionas-web/asyncapi.yaml` owns incoming InquirySubmitted v1. Read the current contract before changing its transport view. `fionas-commerce` owns persistence, business validation, exact financial arithmetic and SERVICE authentication. Import neither commerce-domain nor commerce-runtime.
- Never reprice, compute financial totals, persist inquiries locally or create separate financial documents. Forward supported fields deterministically with the event UUID as `Idempotency-Key`.
- Preserve the pipeline: validated event → valid committed receipt → deterministic outcome → confirmed JetStream PubAck → input ACK. Failure, including malformed success receipts, must not invent success or ACK/TERM the input.
- Share only event contracts for now. No handler registries, routing framework, DI, workflow engine, empty packages or hypothetical workers. Each worker owns its durable consumer, credentials, build and Dockerfile.
- Provision explicitly with operator credentials. Runtime can inspect/pull its named durable and publish its outcome; it cannot administer streams. Never modify the upstream input stream in production setup.
- Ignore unknown additive incoming fields; project only currently supported destination fields. Preserve decimal strings and line order without floating-point arithmetic.
- Use native fetch, bounded timeouts, delayed NAKs and finite delivery attempts. Secrets and customer payloads never enter logs, images, fixtures or commits. Public synthetic development credentials are labeled local only.
- TypeScript must be erasable by native Node for development/tests; deployment compiles with tsc. Prefer standard-library code and built-in test runner.
- Run `npm run typecheck`, `npm test`, `npm run build --workspace @fionas/inquiry-to-commerce`, `npm run test:integration` (Docker), and the production Docker build. Integration tests create a disposable isolated NATS server; never point cleanup at a shared account.
- Keep README, versioned AsyncAPI and operator documentation synchronized with contract, configuration and delivery changes. Explain at-least-once delivery, the HTTP/NATS atomicity gap and windowed publish deduplication honestly.
