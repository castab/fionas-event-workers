import { parseArgs } from 'node:util';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { pathToFileURL } from 'node:url';
import { connect } from '@nats-io/transport-node';
import { DeliverPolicy, jetstream } from '@nats-io/jetstream';
import { collectConnection, SetupError } from './nats-setup.mjs';

const streams = { events: 'FIONAS_EVENTS', inquiries: 'FIONAS_INQUIRIES' };
export function tailOptions(argv) {
  let parsed;
  try {
    parsed = parseArgs({ args: argv, allowPositionals: true, options: {
      all: { type: 'boolean', default: false }, limit: { type: 'string' },
      'non-interactive': { type: 'boolean', default: false }, help: { type: 'boolean', short: 'h' },
    } });
  } catch { throw new SetupError('Invalid tail options; use --help.'); }
  const { values, positionals } = parsed;
  if (values.help) return { help: true };
  const stream = Object.hasOwn(streams, positionals[0] ?? '') ? streams[positionals[0]] : undefined;
  if (!stream || positionals.length !== 1) throw new SetupError('Choose events or inquiries; use --help.');
  if (values.limit !== undefined && (!/^[1-9]\d*$/.test(values.limit) || !Number.isSafeInteger(Number(values.limit)))) {
    throw new SetupError('--limit must be a positive whole number.');
  }
  return { stream, all: values.all, limit: values.limit === undefined ? Infinity : Number(values.limit), nonInteractive: values['non-interactive'] };
}

export function messageRecord(stream, message) {
  let payload;
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(message.data);
    try { payload = JSON.parse(text); } catch { payload = text; }
  } catch { payload = { encoding: 'base64', data: Buffer.from(message.data).toString('base64') }; }
  const headers = Object.fromEntries(Array.from(message.headers?.keys() ?? [], key => [key, message.headers.values(key)]));
  return { stream, sequence: message.seq, subject: message.subject, timestamp: message.timestamp, headers, payload };
}

export async function tail(argv = process.argv.slice(2), env = process.env) {
  const options = tailOptions(argv);
  if (options.help) {
    console.log('Usage: node scripts/nats-tail.mjs events|inquiries [--all] [--limit N] [--non-interactive]\nWatch new stream messages; --all replays retained messages first. Output is one JSON record per line. Automation uses the NATS environment variables.');
    return;
  }
  if (!options.nonInteractive && (!process.stdin.isTTY || !process.stdout.isTTY)) {
    throw new SetupError('Interactive tail requires a terminal; use --non-interactive with NATS environment variables.');
  }
  let configModule;
  try { configModule = await import('../workers/inquiry-to-commerce/dist/workers/inquiry-to-commerce/src/config.js'); }
  catch { throw new SetupError('Compiled configuration is missing; run npm run build --workspace @fionas/inquiry-to-commerce.'); }
  const controller = new AbortController();
  let nc, consumer, messages, statusTask, closedTask;
  let connectionEnded = false, outputFailed = false;
  const stop = () => { controller.abort(); messages?.stop(); };
  const outputError = error => { outputFailed = error.code !== 'EPIPE'; stop(); };
  process.on('SIGINT', stop); process.on('SIGTERM', stop); process.stdout.on('error', outputError);
  try {
    const selected = options.nonInteractive ? env : await collectConnection(env, undefined, controller.signal);
    const config = configModule.natsConfig(selected);
    let connection;
    try { connection = await configModule.connectionOptions(config, 'fionas-stream-tail'); }
    catch { throw new SetupError('Cannot read operator credentials; check the mounted .creds file.'); }
    controller.signal.throwIfAborted();
    nc = await connect(connection);
    controller.signal.throwIfAborted();
    statusTask = (async () => {
      for await (const status of nc.status()) if (['disconnect', 'reconnect', 'error'].includes(status.type)) console.error(`NATS ${status.type}`);
    })();
    closedTask = nc.closed().then(() => { connectionEnded = !controller.signal.aborted; messages?.stop(); });
    // Independent, no-ACK ordered ephemeral consumer. Never bind to a business durable.
    consumer = await jetstream(nc, { timeout: 2_000 }).consumers.get(options.stream, {
      name_prefix: `fionas-tail-${randomUUID()}`,
      deliver_policy: options.all ? DeliverPolicy.All : DeliverPolicy.New,
      inactive_threshold: 60_000_000_000,
    });
    controller.signal.throwIfAborted();
    messages = await consumer.consume({ max_messages: 50, expires: 5_000, abort_on_missing_resource: true });
    console.error(`Watching ${options.stream} (${options.all ? 'retained and new' : 'new'} messages; Ctrl+C to stop)`);
    let count = 0;
    for await (const message of messages) {
      if (controller.signal.aborted) break;
      const line = `${JSON.stringify(messageRecord(options.stream, message))}\n`;
      if (!process.stdout.write(line)) await once(process.stdout, 'drain', { signal: controller.signal });
      if (++count >= options.limit) break;
    }
    if (outputFailed) throw new SetupError('Cannot write tail output.');
    if (connectionEnded) throw new SetupError('NATS connection closed before tail completed.');
  } catch (error) {
    if (!controller.signal.aborted || outputFailed) throw error;
  } finally {
    stop();
    await messages?.close();
    if (consumer && !nc.isClosed()) {
      try { await consumer.delete(); }
      catch { console.error('Temporary consumer cleanup could not be confirmed; inactive consumers expire after 60 seconds.'); }
    }
    await nc?.close();
    await Promise.all([statusTask, closedTask]);
    process.off('SIGINT', stop); process.off('SIGTERM', stop); process.stdout.off('error', outputError);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  tail().catch(error => {
    const authored = error instanceof SetupError || (error instanceof Error && /^(Missing NATS_URL|Invalid NATS_URL|NATS_|Choose NATS_)/.test(error.message));
    console.error(authored ? error.message : 'Tail failed; check the existing stream, connectivity and operator stream/consumer permissions.');
    process.exitCode = 1;
  });
}
