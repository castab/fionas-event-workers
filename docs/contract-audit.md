# Contract audit — 2026-10-14

Inspected upstream main before implementation:

- `castab/fionas-web` at `d7d059e5fdbd0b034d77ad655c438ab9e3d834bc`: `asyncapi.yaml`, `docs/public-inquiry-submission.md`, `scripts/lib/nats-stream.mjs`, `scripts/nats-setup.mjs`, `infra/nats/nats-server.conf`, and the current public publisher/event builder. [PR #14](https://github.com/castab/fionas-web/pull/14) is merged.
- `castab/fionas-commerce` at `a3ab6d90a38d2bb67ca7cf23d4699cd53566013b`: README, ARCHITECTURE, AGENTS, trusted-priced-lines migration, inquiry and requested-service transport DTOs, inquiry/idempotency implementation and SERVICE authentication tests. [PR #21](https://github.com/castab/fionas-commerce/pull/21) is merged and consumes commerce-runtime 0.0.23.
- Generated OpenAPI artifact from PR #21's successful [CI run 37889974150](https://github.com/castab/fionas-commerce/actions/runs/37889974150), head `3d48154a724829d1724144518f68f3b643c7828c`, cross-checked against merged source. The older published v0.0.1 release artifact predates trusted lines and is unsuitable for this integration.

No blocking contract incompatibility was found; neither upstream repository was changed.

Incoming v1 requires `schemaVersion,type,id,occurredAt,source,data`; source is `fionas-web`. Data requires name/email/ZIP/date/type, requestedService, priceRevision and lines. The stable envelope UUID is distinct from the public `Nats-Msg-Id` submission key. Requested service supports guestCount, optional guestCountIsMinimum, items with label/group/key and pricingReference. Lines support description, optional subDescription/quantity, unitPrice/taxAmount/currency as strings. The worker projects these fields in fixed order, discarding unknown additive fields and event-only metadata, with no repricing or decimal calculation.

Commerce requires a SERVICE with `fionas.inquiries.create`, a stable `Idempotency-Key`, and the already-priced body. `201 {id,createdAt}` is returned for both first commit and identical replay. Idempotency keys do not expire. Token exchange accepts `{serviceId,secret}` and returns `accessToken,tokenType,expiresAt,expiresIn`. Receipt timestamps may contain six fractional digits; the outcome preserves the original string rather than round-tripping through JavaScript Date.

Material clarifications:

- The upstream migration/OpenAPI examples contain `durationMinutes`, which commerce supports but the current web v1 schema does not publish. No duration is synthesized or forwarded as an unknown additive field. Supporting a newly documented incoming field requires an explicit mapping change.
- Commerce's generated OpenAPI distinguishes `409 IDEMPOTENCY_KEY_REUSED` from a generic concurrent customer-creation `409 conflict`, which may succeed on retry. This milestone conservatively classifies all 409 responses as operator-attention failures and still retries after a one-hour delayed NAK; it never ACKs or claims success from them. No upstream error body is logged.
- Input stream retention belongs to its operator/producer; setup does not change it. Outcome retention defaults to 30 days because the minimal success event contains contact information.

Owned output: `asyncapi.yaml`, InquiryPersisted v1. ID and Nats-Msg-Id are `fionas.inquiry.persisted.v1:<original-envelope-UUID>`, correlationId is that UUID, occurredAt is receipt.createdAt, source is `fionas-event-workers`. Data is only receipt inquiryId, original customer name/email and eventDate. Type/version and timestamp are stable on replay. Publication expects `FIONAS_EVENTS`, and duplicate PubAck is valid success.
