import { parseArgs } from 'node:util';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { connect } from '@nats-io/transport-node';
import { AckPolicy, DeliverPolicy, jetstream, jetstreamManager } from '@nats-io/jetstream';
import { collectConnection, SetupError } from './nats-setup.mjs';

export async function retryInquiry(argv = process.argv.slice(2), env = process.env) {
  let values;
  try { ({ values } = parseArgs({ args: argv, options: {
    sequence: { type: 'string' }, 'non-interactive': { type: 'boolean', default: false }, help: { type: 'boolean', short: 'h' },
  } })); } catch { throw new SetupError('Invalid retry options; use --help.'); }
  if (values.help) { console.log('Usage: node scripts/nats-retry-inquiry.mjs --sequence N [--non-interactive]\nRetry exactly one retained input with operator NATS and dedicated commerce SERVICE credentials. Stop normal worker processes before recovery.'); return; }
  const sequence = Number(values.sequence);
  if (!values.sequence || !/^[1-9]\d*$/.test(values.sequence) || !Number.isSafeInteger(sequence)) throw new SetupError('--sequence must be a positive whole number.');
  if (!values['non-interactive'] && (!process.stdin.isTTY || !process.stdout.isTTY)) throw new SetupError('Interactive recovery requires a terminal; use --non-interactive.');
  const base = '../workers/inquiry-to-commerce/dist/workers/inquiry-to-commerce/src/';
  const [{ workerConfig, connectionOptions }, { CommerceClient }, { processDelivery }, { failureFields }, { INPUT_STREAM, INPUT_SUBJECT }] = await Promise.all([
    import(`${base}config.js`), import(`${base}commerce.js`), import(`${base}process.js`), import(`${base}errors.js`),
    import('../workers/inquiry-to-commerce/dist/packages/events/src/index.js'),
  ]);
  const controller = new AbortController();
  let nc, manager, name;
  const cancel = () => { controller.abort(); };
  process.on('SIGINT', cancel); process.on('SIGTERM', cancel);
  try {
    const selected = values['non-interactive'] ? env : await collectConnection(env, undefined, controller.signal);
    const config = workerConfig({ ...env, NATS_URL: undefined, NATS_CREDS_FILE: undefined, NATS_USER: undefined, NATS_PASSWORD: undefined, NATS_TOKEN: undefined, ...selected });
    const secret = config.commerce.secret ?? (await readFile(config.commerce.secretFile, 'utf8')).trim();
    if (!secret.trim()) throw new SetupError('Commerce service secret is empty.');
    controller.signal.throwIfAborted();
    nc = await connect({ ...await connectionOptions(config.nats, 'fionas-inquiry-recovery'), reconnect: false });
    manager = await jetstreamManager(nc, { checkAPI: false, timeout: 5_000 });
    const stored = await manager.streams.getMessage(INPUT_STREAM, { seq: sequence });
    if (!stored || stored.subject !== INPUT_SUBJECT) throw new SetupError('Requested submission is absent, expired or on an unsupported subject; nothing processed.');
    name = `inquiry-recovery-${randomUUID()}`;
    await manager.consumers.add(INPUT_STREAM, { name, ack_policy: AckPolicy.Explicit,
      filter_subject: INPUT_SUBJECT, deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: sequence,
      ack_wait: 90_000_000_000, max_ack_pending: 1, max_deliver: 1, max_batch: 1, inactive_threshold: 60_000_000_000 });
    const js = jetstream(nc);
    const consumer = await js.consumers.get(INPUT_STREAM, name);
    const message = await consumer.next({ expires: 2_000 });
    if (!message || message.info.streamSequence !== sequence) throw new SetupError('Requested submission is no longer available; nothing processed.');
    const result = await processDelivery(message, {
      commerce: new CommerceClient({ baseUrl: config.commerce.baseUrl, serviceId: config.commerce.serviceId, secret }),
      publish: (subject, data, options) => js.publish(subject, data, options),
      log: (level, event, fields) => console.error(JSON.stringify({ level, event, ...fields })),
    }, controller.signal);
    await nc.flush();
    // 'persisted' means commerce returned a valid receipt for the original idempotency key and the
    // retained input was ACKed by this temporary consumer. It does not prove that commerce's outbox
    // publisher has delivered InquiryPersisted to NATS; verify that on the commerce side.
    const output = { streamSequence: sequence, result };
    if (result === 'persisted') output.note = 'Commerce confirmed the original operation; outbox event delivery is not verified by this tool.';
    console.log(JSON.stringify(output));
    if (result !== 'persisted') process.exitCode = 1;
  } catch (error) {
    console.error(JSON.stringify({ event: 'recovery_failed', ...failureFields(error) }));
    throw new SetupError('Recovery did not confirm success; original input and worker durable remain unchanged. Check identifiers, credentials and connectivity.');
  } finally {
    if (manager && name) { try { await manager.consumers.delete(INPUT_STREAM, name); } catch { console.error('Temporary recovery consumer cleanup unconfirmed; it expires after inactivity.'); } }
    await nc?.close();
    process.off('SIGINT', cancel); process.off('SIGTERM', cancel);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) retryInquiry().catch(error => {
  console.error(error instanceof SetupError ? error.message : 'Recovery failed; build the worker and check NATS/commerce configuration.'); process.exitCode = 1;
});
