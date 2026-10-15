import { test } from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '@nats-io/transport-node';
import { jetstream, jetstreamManager, RetentionPolicy, StorageType } from '@nats-io/jetstream';
import { CONSUMER, INPUT_STREAM, INPUT_SUBJECT, OUTPUT_STREAM, FAILURE_SUBJECT, OUTPUT_SUBJECT } from '../../../../packages/events/src/index.ts';
import { inputConsumerConfig, provision } from '../../src/provision.ts';
import { bytes, fixture, receipt } from '../fixtures.ts';
import { processDelivery } from '../../src/process.ts';
import { CommerceClient } from '../../src/commerce.ts';
import { runLoop, Shutdown } from '../../src/runtime.ts';
import { httpStub, json, token } from '../http-stub.ts';
import type { JsMsg } from '@nats-io/jetstream';
const exec = promisify(execFile);

async function broker(t: TestContext) {
  const container = `fionas-failure-test-${randomUUID()}`;
  const reservation = createServer();
  await new Promise<void>(resolve => reservation.listen(0, '127.0.0.1', resolve));
  const address = reservation.address(); assert.ok(address && typeof address !== 'string');
  await new Promise<void>(resolve => reservation.close(() => resolve()));
  await exec('docker', ['run', '-d', '--name', container, '-p', `127.0.0.1:${address.port}:4222`,
    '--mount', `type=bind,source=${resolve('infra/nats/nats-server.conf')},target=/etc/nats/nats-server.conf,readonly`,
    '--mount', 'type=volume,destination=/data', 'nats:2.14.7-alpine', '-c', '/etc/nats/nats-server.conf']);
  t.after(async () => { await exec('docker', ['rm', '-f', '-v', container]); });
  const options = { servers: `nats://127.0.0.1:${address.port}`, user: 'fionas-admin', pass: 'fionas-admin-dev' };
  let admin;
  for (let i = 0; i < 50; i++) {
    try { admin = await connect({ ...options, timeout: 500 }); break; } catch { await delay(50); }
  }
  assert.ok(admin, 'Disposable NATS must start; no skipped integration tests');
  const nc = admin; t.after(() => nc.close());
  const manager = await jetstreamManager(admin);
  await manager.streams.add({ name: INPUT_STREAM, subjects: ['fionas.inquiries.>'], storage: StorageType.File,
    retention: RetentionPolicy.Limits, max_age: 30 * 86_400_000_000_000 });
  await provision(manager);
  return { admin, manager, options };
}

test('reproduce delayed NAK starvation and maximum-delivery behavior with one pending slot', { timeout: 30_000 }, async t => {
  const { admin, manager, options } = await broker(t);
  // Preserve the actual consumer configuration, changing only deadlines for the experiment.
  await manager.consumers.update(INPUT_STREAM, CONSUMER, { ack_wait: 100_000_000 });
  assert.equal(inputConsumerConfig().max_ack_pending, 1);
  const js = jetstream(admin);
  await js.publish(INPUT_SUBJECT, new TextEncoder().encode('{malformed'));
  await js.publish(INPUT_SUBJECT, bytes(fixture()));
  let worker = await connect(options); t.after(() => worker.close());
  let consumer = await jetstream(worker).consumers.get(INPUT_STREAM, CONSUMER);
  const first = await consumer.next({ expires: 1000 }); assert.ok(first);
  assert.equal(first.info.streamSequence, 1);
  first.nak(1500); await worker.flush();
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 1);
  assert.equal(await consumer.next({ expires: 1000 }), null, 'B cannot pass A during its delayed NAK');
  await worker.close(); worker = await connect(options);
  consumer = await jetstream(worker).consumers.get(INPUT_STREAM, CONSUMER);
  for (let attempt = 2; attempt <= 10; attempt++) {
    const delivery = await consumer.next({ expires: 1000 }); assert.ok(delivery);
    assert.equal(delivery.info.streamSequence, 1, 'restart does not release A or allow B past it');
    assert.equal(delivery.info.deliveryCount, attempt);
    if (attempt < 10) delivery.nak(10);
  }
  const beforeLimitCheck = await manager.consumers.info(INPUT_STREAM, CONSUMER);
  const following = await consumer.next({ expires: 1000 });
  const afterLimitCheck = await manager.consumers.info(INPUT_STREAM, CONSUMER);
  console.log(JSON.stringify({ observation: 'legacy-single-slot', beforeLimitCheck: {
    ackPending: beforeLimitCheck.num_ack_pending, pending: beforeLimitCheck.num_pending,
  }, afterLimitCheck: { ackPending: afterLimitCheck.num_ack_pending, pending: afterLimitCheck.num_pending },
  followingSequence: following?.info.streamSequence ?? null }));
  assert.ok(await manager.streams.getMessage(INPUT_STREAM, { seq: 1 }), 'failed source remains retained');
  assert.ok(following); assert.equal(following.info.streamSequence, 2);
  await following.ackAck();
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 0);
});

test('permanent disposition, restart, infrastructure recovery and targeted replay preserve forward progress', { timeout: 60_000 }, async t => {
  const { admin, manager, options } = await broker(t);
  await manager.consumers.update(INPUT_STREAM, CONSUMER, { ack_wait: 150_000_000 });
  const js = jetstream(admin);
  const creds = { serviceId: '11111111-1111-4111-8111-111111111111', secret: 'synthetic-service-secret' };
  let rejectKey = '', forbidden = false, unavailable = false;
  const stub = await httpStub(t, (call, res) => {
    if (call.path === '/auth/service/token') json(res, 200, token());
    else json(res, forbidden ? 403 : unavailable ? 503 : call.headers['idempotency-key'] === rejectKey ? 422 : 201, receipt);
  });
  const client = new CommerceClient({ baseUrl: stub.url, ...creds });
  let worker = await connect({ ...options, user: 'inquiry-to-commerce', pass: 'inquiry-to-commerce-dev' });
  t.after(() => worker.close());
  let consumer = await jetstream(worker).consumers.get(INPUT_STREAM, CONSUMER);
  const restart = async () => {
    await worker.close(); worker = await connect({ ...options, user: 'inquiry-to-commerce', pass: 'inquiry-to-commerce-dev' });
    consumer = await jetstream(worker).consumers.get(INPUT_STREAM, CONSUMER);
  };
  const deps = { commerce: client, publish: (subject: string, data: Uint8Array, opts: any) => jetstream(worker).publish(subject, data, opts), log() {} };
  const signal = new AbortController().signal;
  const fast = (message: JsMsg) => ({ data: message.data, timestamp: message.timestamp, info: message.info,
    ackAck: (opts?: { timeout: number }) => message.ackAck(opts), term: (reason?: string) => message.term(reason), nak: () => message.nak(10) });
  const receive = async () => { const m = await consumer.next({ expires: 1000 }); assert.ok(m); return m; };
  const submit = async (value = fixture()) => { value.id = randomUUID(); const ack = await js.publish(INPUT_SUBJECT, bytes(value)); return { value, seq: ack.seq }; };

  const invalid = await submit({ ...fixture(), schemaVersion: 2 } as any);
  const valid = await submit();
  const failureAttempts: Uint8Array[] = [];
  // Failure-record outage cannot release A or invent a success. The retry survives restart.
  await processDelivery(fast(await receive()), { ...deps, publish: async (_s, data) => { failureAttempts.push(data); throw new Error('NATS outage'); } }, signal);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 0);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 1);
  await restart();
  await processDelivery(fast(await receive()), { ...deps, publish: async (s, data, opts) => {
    failureAttempts.push(data); await deps.publish(s, data, opts); throw new Error('PubAck lost');
  } }, signal);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 1);
  await restart();
  let duplicate = false;
  const beforeLostTerm = fast(await receive()); beforeLostTerm.term = () => {};
  assert.equal(await processDelivery(beforeLostTerm, { ...deps, publish: async (s, data, opts) => {
    failureAttempts.push(data); const ack = await deps.publish(s, data, opts); duplicate = ack.duplicate; return ack;
  } }, signal), 'failed');
  await worker.flush();
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 1, 'lost TERM leaves original unresolved');
  await restart();
  assert.equal(await processDelivery(fast(await receive()), { ...deps, publish: async (s, data, opts) => {
    failureAttempts.push(data); return deps.publish(s, data, opts);
  } }, signal), 'failed');
  await worker.flush();
  assert.equal(duplicate, true);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_ack_pending, 0);
  assert.deepEqual(failureAttempts[0], failureAttempts[1]); assert.deepEqual(failureAttempts[1], failureAttempts[2]);
  assert.deepEqual(failureAttempts[2], failureAttempts[3]);
  const failedRecord = await manager.streams.getMessage(OUTPUT_STREAM, { seq: 1 }); assert.ok(failedRecord);
  assert.equal(failedRecord.subject, FAILURE_SUBJECT);
  assert.equal(failedRecord.json<any>().correlationId, invalid.value.id);
  assert.equal(failedRecord.json<any>().data.inputSequence, invalid.seq);
  assert.equal(failedRecord.string().includes('jane@example.com'), false);
  const b = await receive(); assert.equal(b.info.streamSequence, valid.seq);
  assert.equal(await processDelivery(b, deps, signal), 'persisted');
  assert.equal(stub.calls.filter(c => c.path === '/inquiries').length, 1, 'invalid A never reaches commerce');
  assert.ok(await manager.streams.getMessage(INPUT_STREAM, { seq: invalid.seq }));

  // Definitive backend rejection also frees the single slot for the following valid message.
  const rejected = await submit(); rejectKey = rejected.value.id;
  const afterRejected = await submit();
  assert.equal(await processDelivery(await receive(), deps, signal), 'failed');
  await worker.flush();
  const d = await receive(); assert.equal(d.info.streamSequence, afterRejected.seq);
  assert.equal(await processDelivery(d, deps, signal), 'persisted');

  // Environmental failure stops the loop before taking or rejecting another customer inquiry.
  const misconfigured = await submit(); const afterConfig = await submit();
  forbidden = true;
  const beforeConfig = (await manager.streams.info(OUTPUT_STREAM)).state.messages;
  const shutdown = new Shutdown(() => {});
  try { await assert.rejects(runLoop(consumer, deps, shutdown), /commerce_permission_denied/); }
  finally { shutdown.dispose(); }
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, beforeConfig);
  assert.equal((await manager.consumers.info(INPUT_STREAM, CONSUMER)).num_pending, 1);
  await restart(); forbidden = false;
  const e = await receive(); assert.equal(e.info.streamSequence, misconfigured.seq);
  assert.equal(await processDelivery(e, deps, signal), 'persisted');
  const f = await receive(); assert.equal(f.info.streamSequence, afterConfig.seq);
  assert.equal(await processDelivery(f, deps, signal), 'persisted');

  // A temporary backend outage recovers the identical HTTP request without a false event.
  const temporary = await submit(); unavailable = true;
  const beforeOutage = (await manager.streams.info(OUTPUT_STREAM)).state.messages;
  assert.equal(await processDelivery(fast(await receive()), deps, signal), 'retry');
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, beforeOutage);
  await restart(); unavailable = false;
  assert.equal(await processDelivery(await receive(), deps, signal), 'persisted');
  const retriedCalls = stub.calls.filter(c => c.headers['idempotency-key'] === temporary.value.id);
  assert.equal(retriedCalls.length, 2); assert.equal(retriedCalls[0]!.body, retriedCalls[1]!.body);

  // Prolonged outages exhaust finite automatic attempts, halt, and retain the source for recovery.
  const exhausted = await submit(); const afterExhaustion = await submit(); unavailable = true;
  for (let i = 1; i <= 10; i++) {
    const m = await receive(); assert.equal(m.info.streamSequence, exhausted.seq);
    if (i < 10) assert.equal(await processDelivery(fast(m), deps, signal), 'retry');
    else await assert.rejects(processDelivery(m, deps, signal), /retry_exhausted/);
  }
  await restart(); unavailable = false;
  const next = await receive(); assert.equal(next.info.streamSequence, afterExhaustion.seq);
  await processDelivery(next, deps, signal);
  assert.ok(await manager.streams.getMessage(INPUT_STREAM, { seq: exhausted.seq }));

  // Targeted recovery retries the retained sequence without resetting primary consumer state.
  const primary = await manager.consumers.info(INPUT_STREAM, CONSUMER);
  rejectKey = '';
  for (const selected of [rejected, exhausted]) {
    const result = await exec(process.execPath, ['scripts/nats-retry-inquiry.mjs', '--sequence', String(selected.seq), '--non-interactive'], {
      env: { ...process.env, NATS_URL: options.servers, NATS_USER: options.user, NATS_PASSWORD: options.pass, NATS_CREDS_FILE: '',
        COMMERCE_BASE_URL: stub.url, COMMERCE_SERVICE_ID: creds.serviceId, COMMERCE_SERVICE_SECRET: creds.secret, COMMERCE_SERVICE_SECRET_FILE: '' },
    });
    assert.equal(JSON.parse(result.stdout).result, 'persisted');
    assert.equal(result.stderr.includes(creds.secret), false);
    const attempts = stub.calls.filter(c => c.headers['idempotency-key'] === selected.value.id);
    for (const attempt of attempts) assert.equal(attempt.body, attempts[0]!.body);
  }
  const callsBeforeMissing = stub.calls.length;
  await assert.rejects(exec(process.execPath, ['scripts/nats-retry-inquiry.mjs', '--sequence', '99999', '--non-interactive'], {
    env: { ...process.env, NATS_URL: options.servers, NATS_USER: options.user, NATS_PASSWORD: options.pass, NATS_CREDS_FILE: '',
      COMMERCE_BASE_URL: stub.url, COMMERCE_SERVICE_ID: creds.serviceId, COMMERCE_SERVICE_SECRET: creds.secret, COMMERCE_SERVICE_SECRET_FILE: '' },
  }));
  assert.equal(stub.calls.length, callsBeforeMissing, 'missing recovery source cannot process a later inquiry');
  const afterRecovery = await manager.consumers.info(INPUT_STREAM, CONSUMER);
  assert.deepEqual(afterRecovery.ack_floor, primary.ack_floor);
  assert.equal(afterRecovery.delivered.stream_seq, primary.delivered.stream_seq);
  const outcomes = await manager.streams.info(OUTPUT_STREAM);
  assert.ok(outcomes.state.messages > 0);
  const final = await manager.streams.getMessage(OUTPUT_STREAM, { seq: outcomes.state.last_seq });
  assert.equal(final?.subject, OUTPUT_SUBJECT);
});

test('actual worker exits on systemic authorization failure without rejecting the queued customer behind it', { timeout: 20_000 }, async t => {
  const { admin, manager, options } = await broker(t);
  const stub = await httpStub(t, (call, res) => json(res, call.path === '/auth/service/token' ? 200 : 403,
    call.path === '/auth/service/token' ? token() : { code: 'forbidden' }));
  const js = jetstream(admin);
  const first = fixture(), second = fixture(); first.id = randomUUID(); second.id = randomUUID();
  await js.publish(INPUT_SUBJECT, bytes(first)); await js.publish(INPUT_SUBJECT, bytes(second));
  await assert.rejects(exec(process.execPath, ['workers/inquiry-to-commerce/src/main.ts'], {
    env: { ...process.env, NATS_URL: options.servers, NATS_USER: 'inquiry-to-commerce', NATS_PASSWORD: 'inquiry-to-commerce-dev', NATS_CREDS_FILE: '',
      COMMERCE_BASE_URL: stub.url, COMMERCE_SERVICE_ID: '11111111-1111-4111-8111-111111111111', COMMERCE_SERVICE_SECRET: 'synthetic-secret', COMMERCE_SERVICE_SECRET_FILE: '' },
    timeout: 10_000,
  }), error => {
    const result = error as { code: number; stdout: string; stderr: string };
    assert.equal(result.code, 1);
    assert.match(result.stderr, /worker_halted/);
    assert.equal((result.stdout + result.stderr).includes('input_terminated'), false);
    assert.equal((result.stdout + result.stderr).includes('inquiry_acknowledged'), false);
    assert.equal((result.stdout + result.stderr).includes('jane@example.com'), false);
    return true;
  });
  assert.equal(stub.calls.filter(c => c.path === '/inquiries').length, 1);
  assert.equal((await manager.streams.info(OUTPUT_STREAM)).state.messages, 0);
  const state = await manager.consumers.info(INPUT_STREAM, CONSUMER);
  assert.equal(state.num_ack_pending, 1); assert.equal(state.num_pending, 1);
  assert.equal((await manager.streams.info(INPUT_STREAM)).state.messages, 2);
});
