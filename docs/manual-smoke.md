# Optional local commerce smoke test

This test uses real local backend persistence. It is optional; automated tests require no backend/PostgreSQL. Use private development credentials, synthetic contact details and synthetic prices only.

1. Start local fionas-commerce according to its README, apply its normal migrations and provision the dedicated SERVICE/role described in this repository's README. Configure this worker's `.env` with that service ID/secret and backend origin.
2. Start the isolated local broker with `npm run nats:up`, then `node scripts/nats-local-init.ts` (or use the existing fionas-web broker and separate operator setup).
3. Start `npm run dev --workspace @fionas/inquiry-to-commerce` and check `durable_consumer_available`.
4. Submit a synthetic `/book` request through local fionas-web, using its own publish-only identity in the same account. The fixture in `workers/inquiry-to-commerce/tests/fixtures.ts` is an alternative synthetic payload for the NATS CLI:

   ```sh
   nats pub fionas.inquiries.submitted.v1 --jetstream --header 'Nats-Msg-Id:manual-synthetic-key' --header 'Nats-Expected-Stream:FIONAS_INQUIRIES' < synthetic-inquiry.json
   ```

   Keep one original UUID and byte-identical payload for replay. Publish only to the expected subject; the sample amounts are fictional.
5. Confirm received → commerce_persisted → outcome_published → inquiry_acknowledged logs. With an authorized staff session, read the inquiry/canonical Estimate in commerce and verify all priced lines and service details. With operator NATS credentials, inspect the latest `FIONAS_EVENTS` message and compare its inquiryId/occurredAt to the receipt and original contact details.
6. Stop the worker, republish the identical event UUID/body with a different synthetic submission-key header to intentionally exercise consumer replay, then restart. Confirm commerce still has one inquiry, the outgoing ID/bytes are unchanged and publication is a duplicate within 24 hours. A different event UUID represents new intent; do not use it for this replay test.
7. Stop the worker cleanly and inspect its durable position. Clean up synthetic data through your existing development environment procedure; do not run destructive stream reset commands against a shared account.
