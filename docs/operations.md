# Operating inquiry-to-commerce

Use a separate operator NATS context in the same account; never grant these inspection/administration permissions to the running worker. Examples below assume the NATS CLI's configured context contains the server and operator credentials.

## Inspect and diagnose

```sh
nats stream info FIONAS_INQUIRIES
nats stream info FIONAS_EVENTS
nats consumer info FIONAS_INQUIRIES inquiry-to-commerce
nats consumer info FIONAS_INQUIRIES inquiry-to-commerce --json
```

Consumer info reports pending messages, ACK-pending deliveries, redeliveries, delivered sequence and ACK floor. A pending count alone does not prove all exhausted messages were handled. Track `operator_attention_required` logs with input `streamSequence`, attempt and eventId where parsing succeeded. Malformed events are identified by retained stream sequence even when no valid event ID can be extracted. Log codes distinguish credential rejection, permission denial, malformed receipts, contract rejection and transient HTTP failures; response bodies and customer details are deliberately absent.

Maximum-delivery advisories are published to:

```sh
nats sub '$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.FIONAS_INQUIRIES.inquiry-to-commerce'
```

Capture these advisories in your existing operator tooling; they include the stream sequence. Advisories are live notifications and are not retained by this repository. At the tenth failed delivery the worker also logs `exhausted:true`. A final crash instead of a completed failure may require the server's subsequent delivery-limit check before its advisory appears.

Inspect a specific retained source in a private terminal:

```sh
nats stream get FIONAS_INQUIRIES 42
```

This command exposes customer data. Limit access and do not paste its output into public logs. Input limits retention keeps messages independent of ACKs; ACKed and failed records coexist until its own retention bounds expire. This worker does not alter those bounds. Outcomes expire after their configured finite retention. Preserve evidence/recover before input retention expires.

## Correct a problem and recover

For a transient outage or corrected permission/credential, allow the delayed redelivery to occur. No manual republish or new event ID is needed. Token cache refreshes before expiration or after a single inquiry 401. A 403 requires checking the SERVICE principal/role; changing signing keys on the worker cannot help.

For a malformed input or a `409 IDEMPOTENCY_KEY_REUSED`, determine the contract/problem with its owning application. Do not mutate retained events, invent a receipt, change prices or generate a new event ID just to bypass idempotency. The original must stay inspectable. If the source is irreparably invalid, record its disposition through your operator process before making an explicit skip decision; the runtime will not silently discard it.

Exhausted deliveries do not restart automatically. A straightforward deliberate recovery for this initial low-throughput deployment is to recreate only this worker's durable and replay all retained submissions:

1. Stop every inquiry-to-commerce process and record consumer info, failed sequences and the underlying failure.
2. Fix configuration/code or source contract problems before replay. Ensure downstream side-effect consumers deduplicate their own operations; a replay after 24 hours can store duplicate outcome records with the same ID.
3. With operator credentials, explicitly delete only the durable:

   ```sh
   nats consumer rm FIONAS_INQUIRIES inquiry-to-commerce
   npm run nats:setup -- --retention-days 30 --replicas 1
   ```

   Use the actual existing outcome retention/replica settings. Keep the CLI's confirmation prompt; this is an intentional consumer-position reset, not stream deletion. Setup creates the durable with DeliverAll and refuses outcome mismatches. Neither retained stream is deleted or recreated.
4. Restart the worker. It replays every retained matching input. Commerce returns the original receipt for committed UUIDs; deterministic outcomes deduplicate within the publish window. Check ACK logs and consumer pending state, and verify the originally failed sequence's eventual disposition.

This recovery is intentionally broad and may revisit many successful events. On larger installations use an operator-reviewed targeted consumer-position recovery appropriate to the deployed NATS version, keeping earlier unresolved messages accounted for. Do not alter DeliverAll silently in the setup script or simply reset a consumer past the failed message. An independent consumer can inspect the same source without affecting this durable.

Because max_ack_pending is one, a delayed unresolved delivery can block later work. This is a deliberate bounded first milestone, so alert promptly on permanent failures instead of using queue size as the sole signal. No dead-letter stream or permanent side-effect deduplication store is provided here.

## Process supervision

Use a restart policy for nonzero startup failures. Read-only named consumer INFO must be allowed, and setup must precede deployment. Worker logs `nats_connected`, `durable_consumer_available`, received/persisted/published/acknowledged stages, failure classification and delayed retry scheduling. Connection statuses include kind only, excluding URLs and raw errors. No HTTP health endpoint exists.

Set Docker `--stop-timeout 60` (or equivalent supervisor grace). Shutdown stops new pulls, allows up to 55 seconds for current work, then aborts/closes. Input ACK uses the confirmed ACK protocol; a lost ACK response is logged and the worker can safely replay if NATS did not receive it. A received ACK whose response was lost may already be complete, because its HTTP and outgoing publication were confirmed first.

The worker expects the provisioned durable to remain present. It retries pull errors at two-second intervals while the NATS client reconnects; persistent missing-resource/permission errors require operator repair. Stream and consumer administration remain operator-only.
