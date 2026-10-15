# Full-system smoke test (coordinated deployment)

Target flow, once the commerce outbox integration exists:

```text
fionas-web
  -> FIONAS_INQUIRIES
  -> inquiry-to-commerce
  -> fionas-commerce
  -> PostgreSQL business state + outbox
  -> commerce outbox publisher
  -> FIONAS_EVENTS
```

> **Status: not executed.** This procedure cannot be completed until `fionas-commerce` writes its `InquiryPersisted` outbox event in the same transaction as the inquiry and initial Estimate, and runs a NATS outbox publisher. The automated suites use an HTTP stub that *models* the atomic receipt contract; that is a test assumption and **must not be reported as validating** the real PostgreSQL outbox or end-to-end event delivery. Record this smoke test as passed only after running it against real applications.

Use synthetic customer data and prices. No production credentials or deployment access are required.

## Start and provision the local applications

1. Start the integrated fionas-commerce (migrations, bootstrap administrator, outbox tables and its NATS outbox publisher) according to its README. Provision a new `inquiry-to-commerce` SERVICE with exactly `fionas.inquiries.create`, its own role and credential. Store its UUID/secret privately in this repository's `.env` alongside COMMERCE_BASE_URL. Never use the former fionas-web SERVICE credential, a staff session or the backend signing key for the worker.
2. Use one local JetStream broker/account for all applications. Either start this repository's `npm run nats:up` and `node scripts/nats-local-init.ts`, or use the existing fionas-web broker with its own input-stream setup. Do not start two brokers competing for port 4222. Add this worker identity's failure-publication and named durable permissions from `infra/nats/nats-server.conf`; it must **not** be able to publish success subjects. Keep the public fionas-web identity publish-only and give commerce's outbox publisher its own identity that can publish only its event subjects.
3. Run `npm run nats:setup` as a separate NATS operator to create/verify FIONAS_EVENTS and the primary durable. Input must already exist. Normal worker runtime must not use operator credentials.
4. Configure the local fionas-web public application per its `docs/public-inquiry-submission.md`: booking enabled, the same NATS account, its public publish-only identity, synthetic private price file, replay secret and the development Turnstile configuration. The web application calls no commerce API. Keep local secrets out of source control.
5. Start `npm run dev --workspace @fionas/inquiry-to-commerce`. Check `durable_consumer_available`. In another terminal, run `npm run nats:tail:events` as the operator to watch `FIONAS_EVENTS` before submitting the form.

## Verify real persistence, the canonical Estimate and the outbox event

1. Submit one fictional `/book` request with a distinctive synthetic note. Keep the incoming envelope UUID and source stream sequence from the worker logs; do not confuse that UUID with the public `Nats-Msg-Id` submission key. An operator can inspect that retained input with `nats stream get FIONAS_INQUIRIES <sequence>`.
2. Confirm worker logs in order: `inquiry_received`, `commerce_persisted`, `inquiry_acknowledged`. There is no `outcome_published`: the worker publishes no success event. Record inquiryId. `inquiry_acknowledged` is the confirmed input ACK and follows the valid receipt directly.
3. Log into commerce as an authorized local staff user with `fionas.inquiries.read` and `commerce.financial-document.read`. Use the staff UI or the existing session-authenticated `GET /staff/requests/{inquiryId}`. Do not expand the worker's permission set for this inspection.
4. Assert the detail's `inquiry.id` equals the receipt's inquiryId. Its `financial` is the current canonical INITIAL_ESTIMATE lineage: require stage `ESTIMATE`, version `1`, and `financial.id` equal to the inquiry lifecycle's document ID. Confirm `financial.inquiryId` matches and `financial.linesAuthoredBy` identifies the dedicated worker SERVICE (`principalKind: SERVICE`, `principalId` equal to COMMERCE_SERVICE_ID). Optionally cross-check with `GET /inquiries/{inquiryId}/financial-documents` and `GET /financial-documents/{financial.id}`.
5. Compare every ordered line's descriptions, quantity, unit price, tax and currency with the original submission, preserving exact strings semantically even if commerce canonically formats decimal scale. Verify requestedService items/guest count/pricingReference and optional note through inquiry detail. Commerce derives totals and creates the initial Estimate; the worker never creates another financial document.
6. In commerce, confirm exactly one committed success outbox record for this inquiry, committed in the same transaction as the inquiry and Estimate (inspect with commerce's own tooling; this repository cannot see it).
7. Confirm the commerce publisher delivered it asynchronously: watch `FIONAS_EVENTS` (`npm run nats:tail:events -- --all`) for a **commerce-owned** `InquiryPersisted` event whose subject, schema and version are those defined by commerce, published by commerce's identity rather than this worker's. Verify its content against commerce's own contract; this repository defines none. Delivery may lag the worker's ACK; that is expected and the worker never waits for it.
8. Confirm that **only commerce** published the success event: there must be no event with `source: fionas-event-workers` other than processing-failed diagnostics, and the worker identity must be refused if it tries to publish a success subject.

## Verify replay and failure behavior

Stop normal workers before deliberately retrying the retained input with `npm run nats:retry:inquiry -- --sequence <sequence>`, using operator NATS and the same dedicated SERVICE environment. Confirm result `persisted` with its note, the same inquiry/Estimate IDs and original creation time, and **exactly one committed success outbox record** for the inquiry. Replaying the original submission must not create another outbox record. Any duplicate NATS delivery of that event (the outbox publisher is at-least-once, so an ambiguous PubAck or a retry beyond the deduplication window can repeat it) must retain the original event identity and payload; downstream consumers must deduplicate independently. The result confirms only that commerce acknowledged the original operation, not that the outbox publisher delivered; check delivery separately as above. The primary durable must not be reset. Restart the worker afterward.

Also exercise the lost-response path: with the integrated commerce, interrupt the worker's HTTP response after commit (for example by restarting the worker mid-request in a development environment). Redelivery must send the identical `Idempotency-Key` and body, return the existing receipt, and still leave exactly one committed outbox record. Any repeated NATS delivery must carry the original event identity and payload.

On an isolated development instance, revoke the SERVICE permission temporarily and submit two synthetic valid inquiries. The first `403` must halt the worker with operator attention, without InquiryProcessingFailed, TERM, or an input ACK. Restore the permission, retry the affected source sequence if needed, then restart. Both inquiries must eventually persist using their original UUIDs; no customer payloads or credentials should appear in runtime logs.

To exercise poison handling, use the synthetic unsupported-version publication in the real-NATS integration test as the model. A poison record followed by a valid form submission must produce a minimal InquiryProcessingFailed for the first input, followed by normal persistence for the valid one. The original poison source remains inspectable; its failure diagnostic contains only stream/sequence/code, optional UUID and deterministic timestamp. No success is fabricated for it. See `operations.md` for recovery and the limitations of expired source retention.

Stop processes cleanly and retain synthetic verification evidence privately. Remove local synthetic data through the applications' established development procedures; do not reset shared stream/durable state as ordinary cleanup.
