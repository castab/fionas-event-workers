# Operating inquiry-to-commerce

Use a separate operator NATS context in the same account. Runtime keeps named-consumer INFO/NEXT/ACK/TERM and permission to publish only `fionas.events.inquiry.processing-failed.v1`; it does not administer streams or consumers and cannot publish any success event.

## Responsibility split

`fionas-web` produces `InquirySubmitted`. This worker transports it to the commerce API and records the failures it owns. `fionas-commerce` atomically commits the inquiry, initial Estimate and its success event into its transactional outbox; a commerce outbox publisher later delivers that event to `FIONAS_EVENTS`. Downstream consumers deduplicate independently. The worker ACKs after a valid `201` receipt and does not wait for, poll or verify the outbox publication. NATS remains part of this worker's failure-disposition workflow (`InquiryProcessingFailed`, then TERM).

This relies on commerce guaranteeing that a valid `201` means the inquiry, Estimate and outbox event committed atomically, and that an idempotent replay returns the original receipt without a second outbox event. The worker cannot check this; see the cutover section below.

## Inspect and distinguish failure states

```sh
nats consumer info FIONAS_INQUIRIES inquiry-to-commerce --json
nats stream info FIONAS_INQUIRIES
nats stream info FIONAS_EVENTS
nats sub '$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.FIONAS_INQUIRIES.inquiry-to-commerce'
```

Consumer info reports pending, ACK-pending, redelivery, delivered and ACK-floor state. Capture maximum-delivery advisories with existing operator tooling: they contain the source sequence but are live notifications, not a durable failure log. A crash can hit the delivery limit without the worker observing its final attempt. `operator_attention_required` logs include code, kind, eventId when extractable, streamSequence and attempt; exhausted processing logs `exhausted:true` then `worker_halted` / `retry_exhausted`.

Inspect the source by exact sequence in a private terminal:

```sh
nats stream get FIONAS_INQUIRIES 42
npm run nats:tail:events -- --all
```

The source/tail includes customer data. Diagnostics on `fionas.events.inquiry.processing-failed.v1` include only input stream/sequence/error code, optional original UUID and a deterministic stored timestamp. Malformed JSON may have no UUID. A failure diagnostic never claims successful persistence, nor proves no inquiry exists. A successful input ACK requires a valid commerce receipt and confirms only that commerce acknowledged the operation; whether commerce's outbox publisher has delivered the success event is observable only on the commerce side (outbox state) or by watching `FIONAS_EVENTS` with `npm run nats:tail:events`.

Failure states differ:

- **Terminal per-message failure:** `failure_record_published` followed by `input_terminated`. The confirmed diagnostic is retained in FIONAS_EVENTS. The original remains in FIONAS_INQUIRIES under its existing limits retention. TERM releases this delivery's pending slot; later valid inputs can proceed immediately. A lost TERM can replay the same failure identity, safely deduplicated within the window.
- **Unresolved/retry-exhausted/systemic failure:** source remains retained and may already have committed in commerce. Credentials, permission, malformed receipts, timeouts and prolonged outages do not produce permanent-customer-failure diagnostics. Repair the environment and recover affected sequences. Exhausted sources are not automatically redelivered; absence of a failure diagnostic does not imply success.

Transient errors NAK with 10s / 30s / 2m / 5m / 15m backoff, at most ten deliveries. Observed exhaustion or systemic failure halts the worker nonzero. Use supervisor restart backoff/limits and alerting, not unlimited rapid restarts. `403` requires repairing SERVICE permissions; repeated `401` requires inspecting credentials/service state. Token refresh is retried once. Malformed success responses are contract problems, never grounds for inventing a receipt or rejecting all queued customers.

## Observed starvation and the correction

The real NATS 2.14.7 regression uses the actual durable/configuration with only retry/ACK times accelerated. Malformed A followed by valid B behaved as follows under the former delayed-NAK policy:

1. A occupied the only ACK-pending slot (`num_ack_pending=1`, `num_pending=1`). B could not be fetched during A's delay.
2. Reconnecting the worker did not release A. Attempts 2–10 still delivered A.
3. After the final ACK deadline, the server checked MaxDeliver, stopped delivering A and delivered B automatically. ACK-pending was then one for B; after B's ACK it became zero. A remained retrievable from the stream.

The former one-hour poison delay could therefore hold inquiries for hours. Increasing pending capacity would only postpone that problem. The correction retains one pending slot and sequential processing, records permanent failures durably, then TERMs them. Infrastructure outages still temporarily block processing by design. If failure publication is unavailable until MaxDeliver, the retained original and operational logs/advisories are recovery evidence; no durable diagnostic can be manufactured while publication is failing.

## Targeted recovery without resetting the durable

1. Stop normal worker processes, record failed sequence(s), and fix the underlying issue. Restore connectivity, repair/rotate SERVICE credentials or permissions, or correct the contract implementation. Do not edit retained inputs, reprice them, or bypass idempotency misuse with fabricated identities.
2. Load the dedicated commerce SERVICE environment and separate operator NATS credentials. Retry exactly one retained submission:

   ```sh
   npm run nats:retry:inquiry -- --sequence 42
   # Automation with NATS and commerce environment already configured:
   npm --silent run nats:retry:inquiry -- --sequence 42 --non-interactive
   ```

   The command uses a temporary consumer starting at that sequence, verifies the exact delivered sequence/subject, and runs the same pipeline once. HTTP body, event UUID and idempotency key are preserved, and no success event is published. A permanently invalid input may still publish its deterministic failure diagnostic, as in normal processing. It never rewinds, replaces, ACKs or modifies the primary durable. Non-success exits 1; retryable failures require another deliberate attempt after repair. Temporary state is deleted with inactivity expiry as fallback. Source payloads and secrets are never printed.
3. Confirm `result:persisted` and the backend inquiry/Estimate. `persisted` means commerce returned a valid receipt for the original idempotency key; the tool prints a note that outbox delivery is not verified. It does not prove commerce's outbox publisher already delivered an event to NATS, and replaying an already-committed inquiry never creates a new business event. Check the success event with commerce's outbox tooling or the tail command. Old failure diagnostics remain historical evidence. If original malformed/unsupported data remains invalid, recovery records the same deterministic failure and returns non-success. New/corrected business intent must go through its owning upstream workflow, not be silently rewritten here.
4. Restart the primary worker. Before MaxDeliver, an unresolved original may redeliver after its NAK/ACK deadline, returning the same receipt safely. TERMed/exhausted originals keep their primary disposition; targeted recovery's success is recorded independently. Observe subsequent normal processing.

The command is also packaged as `node scripts/nats-retry-inquiry.mjs --sequence 42` in the worker image. Mount secrets read-only and use container-reachable addresses as for setup. It needs operator source-message/temporary-consumer access plus the dedicated commerce SERVICE credential; no staff session or token-signing key is used.

Source retention bounds recovery. If the original expired, the command refuses to process a later record and cannot restore its bytes; use established archival/business recovery. Never delete/recreate the input stream for recovery. Outcomes and diagnostics expire under FIONAS_EVENTS retention. Publication deduplication lasts 24 hours; late replay can store another record with the same ID. Every future side-effect consumer needs its own durable operation deduplication.

## Provisioning and lifecycle

Stream and durable settings are unchanged; no reset/migration is required. The runtime identity's publish permission is `fionas.events.inquiry.processing-failed.v1` only, plus the named INFO/NEXT/ACK/TERM rights; the former `fionas.events.inquiry.persisted.v1` permission is removed at cutover. The existing `fionas.events.>` stream filter captures failure diagnostics and commerce-published events alike. Preserve the public publisher's permissions and the commerce outbox publisher's own identity. Operator setup continues verifying configuration and refusing mismatches.

A delayed NAK already scheduled by the old deployment keeps its deadline across upgrade/restart. The new policy applies when that source is next delivered; it does not silently rewrite durable state. Targeted recovery can repair the selected business operation immediately, but does not cancel that old primary delay. Account for its remaining bounded delay during rollout and inspect pending state; do not reset the whole durable merely to accelerate it.

Use a supervisor with bounded restart backoff and alerting. SIGTERM/SIGINT stops new pulls and allows 55 seconds for current work; set termination grace to at least 60 seconds. Unfinished operations remain replayable. A lost input ACK request or response is recoverable: the input is redelivered and the identical request (same idempotency key and body) is replayed, returning the existing commerce receipt. Failure TERM is sent only after its diagnostic PubAck; unconfirmed TERM can replay without false success.

## Cutover from the worker-published success event

Do not deploy this worker, or merge into an auto-deployed branch, before commerce meets its half of the contract. Sequence: (1) release the shared transactional outbox infrastructure; (2) integrate it into `fionas-commerce`; (3) add commerce-owned `InquiryPersisted` outbox writes in the inquiry transaction; (4) configure the commerce NATS outbox publisher; (5) verify outbox-to-NATS publication; (6) deploy this worker and remove its `fionas.events.inquiry.persisted.v1` publish permission; (7) confirm only commerce publishes the authoritative success event.

- Deploying earlier against a commerce `POST /inquiries` that commits without an outbox write would ACK inputs whose success event is never emitted. Deploying later leaves the old worker publishing a worker-owned event that commerce's event would duplicate. Coordinate both.
- Keep the old and new success events distinct. The commerce-owned event has its own versioned contract, defined in commerce; do not assume it equals the retired worker-owned `InquiryPersisted v1`.
- There is no legacy mode. If an interim dual-version deployment is unavoidable, document it explicitly rather than adding an implicit fallback.
- Streams and the `inquiry-to-commerce` durable are unchanged. Do not delete, recreate, rewind or reset them. Do not rename streams.
- Retained or in-flight inputs from before cutover are replayed with their original idempotency keys. For an inquiry already committed by the pre-outbox commerce, replay returns the existing receipt and must not create a new business event. If those older inquiries need a success event, that is a separate deliberate backfill owned by commerce.
