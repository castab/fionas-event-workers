import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '@nats-io/transport-node';
import { AckPolicy, DeliverPolicy, jetstream, jetstreamManager, RetentionPolicy, StorageType } from '@nats-io/jetstream';
import { INPUT_STREAM, INPUT_SUBJECT, OUTPUT_STREAM, OUTPUT_SUBJECT, CONSUMER, persistedEvent } from '../../../../packages/events/src/index.ts';
import { provision, verifyInputConsumer } from '../../src/provision.ts';
import { processDelivery } from '../../src/process.ts';
import { CommerceClient } from '../../src/commerce.ts';
import { bytes, fixture, receipt } from '../fixtures.ts';
import { httpStub, json, token } from '../http-stub.ts';
import type { NatsConnection } from '@nats-io/transport-node';
import type { JsMsg } from '@nats-io/jetstream';
const exec = promisify(execFile);
const image = 'nats:2.14.7-alpine';

async function waitUntil(check: () => Promise<boolean>, timeout = 10_000): Promise<void> {
  const end = Date.now() + timeout;
  while (Date.now() < end) { if (await check()) return; await delay(50); }
  assert.fail('Condition did not become true before deadline');
}

test('real JetStream pipeline: restricted identity, lost responses, redelivery, reconnect and independent consumers', { timeout: 120_000 }, async t => {
  // Entire server/account is disposable; never delete streams on a supplied NATS endpoint.
  const container = `fionas-workers-test-${randomUUID()}`;
  // Pin the allocated port: Docker can change an automatically assigned port on restart.
  const portReservation = createServer();
  await new Promise<void>(resolve => portReservation.listen(0, '127.0.0.1', resolve));
  const address = portReservation.address(); assert.ok(address && typeof address !== 'string');
  const port = address.port;
  await new Promise<void>(resolve => portReservation.close(() => resolve()));
  await exec('docker', ['run', '-d', '--name', container, '-p', `127.0.0.1:${port}:4222`,
    '--mount', `type=bind,source=${resolve('infra/nats/nats-server.conf')},target=/etc/nats/nats-server.conf,readonly`,
    '--mount', 'type=volume,destination=/data', image, '-c', '/etc/nats/nats-server.conf']);
  t.after(async () => { await exec('docker', ['rm', '-f', '-v', container]); });
  const url = `nats://127.0.0.1:${port}`;
  let admin: NatsConnection | undefined;
  await waitUntil(async () => {
    try { admin = await connect({ servers: url, user: 'fionas-admin', pass: 'fionas-admin-dev', timeout: 500, pingInterval: 200, reconnectTimeWait: 100, maxReconnectAttempts: -1 }); return true; }
    catch { return false; }
  });
  assert.ok(admin);
  const nc = admin;
  t.after(() => nc.close());
  const manager = await jetstreamManager(nc);
  await manager.streams.add({ name: INPUT_STREAM, subjects: ['fionas.inquiries.>'], storage: StorageType.File,
    retention: RetentionPolicy.Limits, max_age: 30 * 86_400_000_000_000, duplicate_window: 86_400_000_000_000 });
  const initialConfig = (await manager.streams.info(INPUT_STREAM)).config;
  await provision(manager); await provision(manager);
  assert.deepEqual((await manager.streams.info(INPUT_STREAM)).config, initialConfig);
  await assert.rejects(provision(manager, 31), /max_age/);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).config.max_age, 30 * 86_400_000_000_000);
  verifyInputConsumer((await manager.consumers.info(INPUT_STREAM, CONSUMER)).config);

  let committed = false, loseResponse = true;
  const requests: Array<{ key: string | string[] | undefined; body: string }> = [];
  const stub = await httpStub(t, (call, res) => {
    if (call.path === '/auth/service/token') { json(res, 200, token()); return; }
    requests.push({ key: call.headers['idempotency-key'], body: call.body });
    committed = true;
    if (loseResponse) { loseResponse = false; res.destroy(); }
    else json(res, 201, receipt);
  });
  const service = { serviceId: '11111111-1111-4111-8111-111111111111', secret: 'synthetic-secret' };
  const commerce = new CommerceClient({ baseUrl: stub.url, ...service });
  const worker = await connect({ servers: url, user: 'inquiry-to-commerce', pass: 'inquiry-to-commerce-dev', reconnectTimeWait: 100, maxReconnectAttempts: -1 });
  t.after(() => worker.close());
  const js = jetstream(worker, { timeout: 1000 });
  const consumer = await js.consumers.get(INPUT_STREAM, CONSUMER);
  verifyInputConsumer((await consumer.info(true)).config);
  const adminJs = jetstream(nc);
  const event = fixture();
  await adminJs.publish('fionas.inquiries.unsupported.v2', bytes({ unrelated: true }));
  await adminJs.publish(INPUT_SUBJECT, bytes(event), { msgID: 'public-submission-key-distinct-from-event-id' });
  const logs: string[] = [];
  const deps = { commerce, publish: (subject: string, data: Uint8Array, options: any) => js.publish(subject, data, options),
    log: (_level: unknown, kind: string) => { logs.push(kind); } };
  // Accelerate deliberate NAK delays only for the test; all ACKs still go to the real server.
  const fastDelivery = (message: JsMsg) => ({ data: message.data, info: message.info,
    ackAck: (options?: { timeout: number }) => message.ackAck(options), nak: () => message.nak(50) });
  const first = await consumer.next({ expires: 1000 }); assert.ok(first);
  assert.equal(first.subject, INPUT_SUBJECT);
  await processDelivery(fastDelivery(first), deps, new AbortController().signal);
  assert.equal(committed, true);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 0);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 1);
  assert.equal(logs.includes('inquiry_acknowledged'), false);

  // HTTP replay recovers committed receipt, but a failure before outgoing PubAck must not ACK.
  const second = await consumer.next({ expires: 1000 }); assert.ok(second);
  assert.ok(second.info.deliveryCount >= 2);
  await processDelivery(fastDelivery(second), { ...deps, publish: async () => { throw new Error('outgoing unavailable'); } }, new AbortController().signal);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 0);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 1);

  // Publication reaches NATS; simulate its PubAck being lost before the pipeline receives it.
  const third = await consumer.next({ expires: 1000 }); assert.ok(third);
  await processDelivery(fastDelivery(third), { ...deps, publish: async (subject, data, options) => {
    await js.publish(subject, data, options); throw new Error('PubAck lost');
  } }, new AbortController().signal);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 1);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 1);
  const fourth = await consumer.next({ expires: 1000 }); assert.ok(fourth);
  let duplicate = false;
  await processDelivery(fastDelivery(fourth), { ...deps, publish: async (subject, data, options) => {
    const ack = await js.publish(subject, data, options); duplicate = ack.duplicate; return ack;
  } }, new AbortController().signal);
  assert.equal(duplicate, true);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 0);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 1);
  assert.ok(requests.length >= 4);
  for (const request of requests) { assert.equal(request.key, event.id); assert.equal(request.body, requests[0]!.body); }
  const stored = await manager.streams.getMessage(OUTPUT_STREAM, { seq: 1 }); assert.ok(stored);
  assert.equal(stored.string(), JSON.stringify(persistedEvent(event, receipt)));
  assert.equal(stored.header.get('Nats-Msg-Id'), persistedEvent(event, receipt).id);

  // Independent future side-effect consumers each observe the same retained outcome.
  for (const name of ['future-email', 'future-analytics']) {
    await manager.consumers.add(OUTPUT_STREAM, { durable_name: name, filter_subject: OUTPUT_SUBJECT, ack_policy: AckPolicy.Explicit });
    const observer = await adminJs.consumers.get(OUTPUT_STREAM, name);
    const outcome = await observer.next({ expires: 1000 }); assert.ok(outcome);
    assert.equal(outcome.string(), stored.string()); await outcome.ackAck();
  }

  // Restart the connection: durable position must not replay an already ACKed input.
  await worker.close();
  const restarted = await connect({ servers: url, user: 'inquiry-to-commerce', pass: 'inquiry-to-commerce-dev', reconnectTimeWait: 100, maxReconnectAttempts: -1, pingInterval: 200 });
  t.after(() => restarted.close());
  const restartedConsumer = await jetstream(restarted).consumers.get(INPUT_STREAM, CONSUMER);
  assert.equal(await restartedConsumer.next({ expires: 1000 }), null);

  // Actual unacknowledged delivery survives client interruption (accelerated ack_wait in test consumer).
  await manager.consumers.add(INPUT_STREAM, { durable_name: 'interrupted-test', filter_subject: INPUT_SUBJECT,
    ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, ack_wait: 300_000_000 });
  const interruptedNc = await connect({ servers: url, user: 'fionas-admin', pass: 'fionas-admin-dev' });
  const interrupted = await jetstream(interruptedNc).consumers.get(INPUT_STREAM, 'interrupted-test');
  const unfinished = await interrupted.next({ expires: 1000 }); assert.ok(unfinished);
  await interruptedNc.close();
  const recovered = await (await adminJs.consumers.get(INPUT_STREAM, 'interrupted-test')).next({ expires: 1000 }); assert.ok(recovered);
  assert.equal(recovered.info.streamSequence, unfinished.info.streamSequence);
  assert.ok(recovered.info.deliveryCount > 1); await recovered.ackAck();

  // Restart the real server; same live client reconnects and retained durable state survives.
  let reconnected = false;
  const statusTask = (async () => { for await (const status of restarted.status()) if (status.type === 'reconnect') reconnected = true; })();
  await exec('docker', ['restart', container]);
  await waitUntil(async () => reconnected);
  assert.equal(await restartedConsumer.next({ expires: 1000 }), null);
  await restarted.close(); await statusTask;

  // Exercise the actual entrypoint and bounded SIGTERM cleanup with least privilege.
  const child = spawn(process.execPath, ['workers/inquiry-to-commerce/src/main.ts'], {
    env: { ...process.env, NATS_URL: url, NATS_USER: 'inquiry-to-commerce', NATS_PASSWORD: 'inquiry-to-commerce-dev',
      NATS_CREDS_FILE: '', COMMERCE_BASE_URL: stub.url, COMMERCE_SERVICE_ID: service.serviceId,
      COMMERCE_SERVICE_SECRET: service.secret, COMMERCE_SERVICE_SECRET_FILE: '' }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', chunk => { output += chunk.toString(); });
  child.stderr.on('data', chunk => { output += chunk.toString(); });
  const exit = new Promise<number | null>(resolve => child.once('exit', resolve));
  t.after(() => { if (child.exitCode === null) child.kill(); });
  await waitUntil(async () => { assert.equal(child.exitCode, null, output); return output.includes('durable_consumer_available'); });
  const nextEvent = fixture(); nextEvent.id = randomUUID();
  await adminJs.publish(INPUT_SUBJECT, bytes(nextEvent));
  await waitUntil(async () => output.includes('inquiry_acknowledged'));
  // On Windows Node's kill uses TerminateProcess. Graceful signal path is exercised on Linux CI
  // and by the platform-independent lifecycle tests on every OS.
  child.kill('SIGTERM');
  const code = await exit;
  if (process.platform !== 'win32') { assert.equal(code, 0, output); assert.match(output, /worker_stopped/); }
  assert.equal(output.includes('jane@example.com'), false);
  assert.equal(output.includes(service.secret), false);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 2);
});
