# Contract audit — 2026-10-14

Inspected upstream main before implementation:

- `castab/fionas-web` at `d7d059e5fdbd0b034d77ad655c438ab9e3d834bc`: `asyncapi.yaml`, `docs/public-inquiry-submission.md`, `scripts/lib/nats-stream.mjs`, `scripts/nats-setup.mjs`, `infra/nats/nats-server.conf`, and the current public publisher/event builder. [PR #14](https://github.com/castab/fionas-web/pull/14) is merged.
- `castab/fionas-commerce` at `a3ab6d90a38d2bb67ca7cf23d4699cd53566013b`: README, ARCHITECTURE, AGENTS, trusted-priced-lines migration, inquiry and requested-service transport DTOs, inquiry/idempotency implementation and SERVICE authentication tests. [PR #21](https://github.com/castab/fionas-commerce/pull/21) is merged and consumes commerce-runtime 0.0.23.
- Generated OpenAPI artifact from PR #21's successful [CI run 37889974150](https://github.com/castab/fionas-commerce/actions/runs/37889974150), head `3d48154a724829d1724144518f68f3b643c7828c`, cross-checked against merged source. The older published v0.0.1 release artifact predates trusted lines and is unsuitable for this integration.

No blocking contract incompatibility was found; neither upstream repository was changed.

Incoming v1 requires `schemaVersion,type,id,occurredAt,source,data`; source is `fionas-web`. Data requires name/email/ZIP/date/type, requestedService, priceRevision and lines. The stable envelope UUID is distinct from the public `Nats-Msg-Id` submission key. Requested service supports guestCount, optional guestCountIsMinimum, items with label/group/key and pricingReference. Lines support description, optional subDescription/quantity, unitPrice/taxAmount/currency as strings. The worker projects these fields in fixed order, discarding unknown additive fields and event-only metadata, with no repricing or decimal calculation.

Commerce requires a SERVICE with `fionas.inquiries.create`, a stable `Idempotency-Key`, and the already-priced body. `201 {id,createdAt}` is returned for both first commit and identical replay. Idempotency keys do not expire. Token exchange accepts `{serviceId,secret}` and returns `accessToken,tokenType,expiresAt,expiresIn`. Receipt timestamps may contain six fractional digits; the worker validates the receipt but publishes nothing derived from it.

Material clarifications:

- The upstream migration/OpenAPI examples contain `durationMinutes`, which commerce supports but the current web v1 schema does not publish. No duration is synthesized or forwarded as an unknown additive field. Supporting a newly documented incoming field requires an explicit mapping change.
- Commerce's generated OpenAPI distinguishes permanent `409 IDEMPOTENCY_KEY_REUSED` from retryable concurrent customer-creation `409 conflict`. The hardening pass reads only these known codes for classification; unknown/malformed conflict responses halt as a contract problem. No upstream response body is logged.
- Input stream retention belongs to its operator/producer; setup does not change it. `FIONAS_EVENTS` retention defaults to 30 days because it also holds commerce-published events that may contain contact information.

## Event ownership handoff (transactional outbox)

Success events belong to `fionas-commerce`; this worker owns delivery of incoming inquiries and diagnostics about failures to process them. `asyncapi.yaml` 2.0.0 therefore removed the worker-owned InquiryPersisted v1 and describes only InquiryProcessingFailed v1. The worker no longer constructs or publishes InquiryPersisted, does not wait for a success PubAck before the input ACK, and its NATS identity may publish only the failure subject. No replacement commerce-event contract is defined here: the commerce-owned success event will have its own version and schema, decided during commerce's integration. The retired v1 named `fionas-event-workers` as its source, so it cannot simply be republished by commerce unchanged.

New cross-repository assumption (not yet verifiable from this repository): a valid commerce `201` receipt means the inquiry, initial Estimate and the authoritative success outbox event committed atomically in one PostgreSQL transaction, and an idempotent replay returns the existing receipt without appending another outbox event. The worker validates the receipt (UUID `id`, timestamp `createdAt`) but does not and must not verify the outbox, poll for it or expose an endpoint for it. This document records the audit of the HTTP contract as of the SHAs above; the outbox guarantee is **unaudited** because commerce has not implemented it yet, and must be re-audited before the coordinated cutover (see README, Deployment cutover).

The corrective pass fetched both current main refs again; they remain exactly the SHAs above. Current AsyncAPI/public publisher documentation, merged inquiry DTOs/idempotency implementation and the matching generated OpenAPI/SERVICE schemas were rechecked. No incompatibility or upstream change was required. Incoming optional message/service/item/line fields, ignored additive properties, exact strings/order, exclusive SERVICE permission, token DTO, six-digit receipt timestamps and unchanged outgoing persistence facts remain covered by tests.

New owned diagnostic: InquiryProcessingFailed v1, subject `fionas.events.inquiry.processing-failed.v1`. Identity is `fionas.inquiry.processing-failed.v1:FIONAS_INQUIRIES:<sequence>`. It uses original NATS stored time for deterministic occurredAt and includes only source stream/sequence/stable error code, optionally the extractable UUID. It is not a business success or an assertion that no inquiry was committed. Only permanent per-message errors may publish it, and only a confirmed expected-stream PubAck permits TERM. It remains this worker's only published event.

The outbox handoff pass did not re-fetch upstream SHAs; the HTTP request/receipt contract and incoming v1 mapping above are unchanged.
