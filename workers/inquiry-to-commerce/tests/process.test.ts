import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processDelivery } from '../src/process.ts';
import type { Delivery, Dependencies } from '../src/process.ts';
import { ProcessingError } from '../src/errors.ts';
import { bytes, fixture, receipt } from './fixtures.ts';

function harness(attempt = 1, event: unknown = fixture()) {
  const order: string[] = [], logs: unknown[] = [], keys: string[] = [], bodies: unknown[] = [], outputs: Array<{ bytes: Uint8Array; options: unknown }> = [];
  const message: Delivery = {
    data: bytes(event), timestamp: '2026-10-14T18:30:00.123456789Z', info: { deliveryCount: attempt, streamSequence: 7 },
    async ackAck() { order.push('ack'); return true; },
    nak(delay) { order.push(`nak:${delay}`); },
    term() { order.push('term'); },
  };
  const deps: Dependencies = {
    commerce: { async createInquiry(id, body) { order.push('http'); keys.push(id); bodies.push(body); return receipt; } },
    async publish(subject, data, options) {
      order.push('publish'); outputs.push({ bytes: data, options });
      assert.equal(subject, 'fionas.events.inquiry.processing-failed.v1', 'only failure diagnostics may be published');
      return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: false };
    },
    log(level, event, fields) { logs.push({ level, event, fields }); },
  };
  return { order, logs, keys, bodies, outputs, message, deps, signal: new AbortController().signal };
}
test('valid receipt then confirmed input ACK, with no event publication; identifiers only in logs', async () => {
  const h = harness();
  assert.equal(await processDelivery(h.message, h.deps, h.signal), 'persisted');
  assert.deepEqual(h.order, ['http', 'ack']);
  assert.deepEqual(h.outputs, []);
  assert.deepEqual(h.keys, [fixture().id]);
  const logged = JSON.stringify(h.logs);
  assert.match(logged, /commerce_persisted/); assert.match(logged, /inquiry_acknowledged/);
  assert.equal(logged.includes('outcome_published'), false);
  assert.equal(logged.includes('jane@example.com'), false);
  assert.equal(logged.includes('Jane Doe'), false);
});
test('HTTP failure never ACKs and uses delayed NAK', async () => {
  const h = harness();
  h.deps.commerce.createInquiry = async () => { h.order.push('http'); throw new ProcessingError('http_timeout', 'transient'); };
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, ['http', 'nak:10000']);
});
test('invalid success receipt is a systemic contract failure: no ACK, TERM, NAK or diagnostic', async () => {
  const h = harness();
  h.deps.commerce.createInquiry = async () => { h.order.push('http'); throw new ProcessingError('invalid_inquiry_receipt', 'systemic'); };
  await assert.rejects(processDelivery(h.message, h.deps, h.signal), /invalid_inquiry_receipt/);
  assert.deepEqual(h.order, ['http']);
  assert.deepEqual(h.outputs, []);
});
test('HTTP response lost after commit is retried with the identical key and body, then ACKed', async () => {
  const first = harness(), second = harness(2);
  first.deps.commerce.createInquiry = async (id, body) => {
    first.order.push('http'); first.keys.push(id); first.bodies.push(body);
    throw new ProcessingError('http_connection_failure', 'transient'); // Commerce may already have committed.
  };
  assert.equal(await processDelivery(first.message, first.deps, first.signal), 'retry');
  assert.equal(await processDelivery(second.message, second.deps, second.signal), 'persisted');
  assert.deepEqual(first.order, ['http', 'nak:10000']);
  assert.deepEqual(second.order, ['http', 'ack']);
  assert.deepEqual(first.keys, second.keys);
  assert.equal(JSON.stringify(first.bodies[0]), JSON.stringify(second.bodies[0]));
  assert.deepEqual(first.outputs, []); assert.deepEqual(second.outputs, []);
});
test('lost or unconfirmed input ACK stays recoverable and replays the identical request', async () => {
  const first = harness(), second = harness(2);
  first.message.ackAck = async () => { first.order.push('ack'); throw new Error('ACK response lost'); };
  assert.equal(await processDelivery(first.message, first.deps, first.signal), 'retry');
  assert.deepEqual(first.order, ['http', 'ack', 'nak:10000']);
  assert.match(JSON.stringify(first.logs), /input_ack_unconfirmed/);
  const unconfirmed = harness();
  unconfirmed.message.ackAck = async () => { unconfirmed.order.push('ack'); return false; };
  assert.equal(await processDelivery(unconfirmed.message, unconfirmed.deps, unconfirmed.signal), 'retry');
  assert.deepEqual(unconfirmed.order, ['http', 'ack', 'nak:10000'], 'an unconfirmed ACK is never treated as success');
  assert.equal(await processDelivery(second.message, second.deps, second.signal), 'persisted');
  assert.deepEqual(second.order, ['http', 'ack']);
  assert.deepEqual(first.keys, second.keys);
  assert.equal(JSON.stringify(first.bodies[0]), JSON.stringify(second.bodies[0]));
});
test('duplicate failure PubAck permits TERM; wrong stream or sequence does not', async () => {
  for (const ack of [
    { stream: 'FIONAS_EVENTS', seq: 1, duplicate: true },
    { stream: 'WRONG', seq: 1, duplicate: false },
    { stream: 'FIONAS_EVENTS', seq: 0, duplicate: false },
  ]) {
    const h = harness(1, null); h.deps.publish = async () => { h.order.push('publish'); return ack; };
    await processDelivery(h.message, h.deps, h.signal);
    assert.equal(h.order.includes('term'), ack.stream === 'FIONAS_EVENTS' && ack.seq > 0);
    assert.equal(h.order.includes('ack'), false);
  }
});
test('unsupported events are durably recorded then TERMed; systemic permission errors halt without disposition', async () => {
  const event = { ...fixture(), schemaVersion: 2 };
  const h = harness(1, event);
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, ['publish', 'term']);
  assert.deepEqual(h.keys, [], 'unsupported input never reaches commerce');
  const failure = JSON.parse(new TextDecoder().decode(h.outputs[0]!.bytes));
  assert.equal(failure.type, 'fionas.inquiry.processing-failed');
  assert.equal(failure.correlationId, fixture().id);
  assert.equal(failure.occurredAt, h.message.timestamp);
  assert.equal(JSON.stringify(failure).includes('jane@example.com'), false);
  assert.match(JSON.stringify(h.logs), /operator_attention_required/);
  const rejected = harness();
  rejected.deps.commerce.createInquiry = async () => { throw new ProcessingError('commerce_permission_denied', 'systemic', 403); };
  await assert.rejects(processDelivery(rejected.message, rejected.deps, rejected.signal), /commerce_permission_denied/);
  assert.deepEqual(rejected.order, []);
});
test('permanent commerce rejection publishes its diagnostic before TERM and never ACKs', async () => {
  const h = harness();
  h.deps.commerce.createInquiry = async () => { h.order.push('http'); throw new ProcessingError('commerce_input_rejected', 'permanent', 422); };
  assert.equal(await processDelivery(h.message, h.deps, h.signal), 'failed');
  assert.deepEqual(h.order, ['http', 'publish', 'term']);
  assert.equal(JSON.parse(new TextDecoder().decode(h.outputs[0]!.bytes)).data.errorCode, 'commerce_input_rejected');
});
test('exhausted transient delivery halts for recovery, no ACK/TERM or NAK', async () => {
  const h = harness(10);
  h.deps.commerce.createInquiry = async () => { throw new ProcessingError('http_timeout', 'transient'); };
  await assert.rejects(processDelivery(h.message, h.deps, h.signal), /retry_exhausted/);
  assert.deepEqual(h.order, []);
  assert.match(JSON.stringify(h.logs), /"exhausted":true/);
});
test('failure PubAck must precede TERM, including replay after lost PubAck or TERM', async () => {
  const first = harness(1, null), second = harness(2, null), third = harness(3, null);
  first.deps.publish = async (_subject, data, options) => { first.outputs.push({ bytes: data, options }); throw new Error('PubAck lost'); };
  await processDelivery(first.message, first.deps, first.signal);
  assert.deepEqual(first.order, ['nak:10000']);
  second.message.term = () => { throw new Error('TERM interrupted'); };
  await processDelivery(second.message, second.deps, second.signal);
  assert.deepEqual(second.order, ['publish', 'nak:30000']);
  third.deps.publish = async (_subject, data, options) => { third.outputs.push({ bytes: data, options }); return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: true }; };
  await processDelivery(third.message, third.deps, third.signal);
  assert.deepEqual(third.order, ['term']);
  assert.deepEqual(first.outputs, second.outputs); assert.deepEqual(first.outputs, third.outputs);
  const id = JSON.parse(new TextDecoder().decode(first.outputs[0]!.bytes)).id;
  assert.equal(id, 'fionas.inquiry.processing-failed.v1:FIONAS_INQUIRIES:7');
  assert.deepEqual(first.outputs[0]!.options, { msgID: id, expect: { streamName: 'FIONAS_EVENTS' }, timeout: 5000 });
});
test('shutdown before the failure PubAck never TERMs', async () => {
  const h = harness(1, null), controller = new AbortController();
  let complete!: () => void;
  const pending = new Promise<void>(resolve => { complete = resolve; });
  h.deps.publish = async (subject) => {
    assert.equal(subject, 'fionas.events.inquiry.processing-failed.v1');
    h.order.push('publish'); await pending; return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: false };
  };
  const operation = processDelivery(h.message, h.deps, controller.signal);
  await new Promise(resolve => setImmediate(resolve)); assert.deepEqual(h.order, ['publish']);
  controller.abort(); complete();
  assert.equal(await operation, 'interrupted'); assert.deepEqual(h.order, ['publish']);
});
test('wrapped NATS permission denial of a failure diagnostic is systemic: no TERM and no in-process retry', async () => {
  const h = harness(1, null);
  h.deps.publish = async () => { throw new Error('Request failed', { cause: Object.assign(new Error('private subject'), { name: 'PermissionViolationError' }) }); };
  await assert.rejects(processDelivery(h.message, h.deps, h.signal), (e: ProcessingError) => e.kind === 'systemic');
  assert.deepEqual(h.order, []);
  assert.equal(JSON.stringify(h.logs).includes('private subject'), false);
});
test('ACK cannot run while the commerce receipt is still pending', async () => {
  const h = harness();
  let confirm!: () => void;
  const pending = new Promise<void>(resolve => { confirm = resolve; });
  h.deps.commerce.createInquiry = async () => { h.order.push('http'); await pending; return receipt; };
  const processing = processDelivery(h.message, h.deps, h.signal);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(h.order, ['http']);
  confirm(); await processing;
  assert.deepEqual(h.order, ['http', 'ack']);
});
test('aborted work around commerce confirmation remains unacknowledged for replay', async () => {
  for (const stage of ['during', 'after']) {
    const h = harness(), controller = new AbortController();
    h.deps.commerce.createInquiry = async () => {
      h.order.push('http'); controller.abort();
      if (stage === 'during') throw new ProcessingError('shutdown_aborted', 'transient');
      return receipt;
    };
    assert.equal(await processDelivery(h.message, h.deps, controller.signal), 'interrupted');
    assert.deepEqual(h.order, ['http']);
  }
});
test('abort while the input ACK is unconfirmed never reports success', async () => {
  const h = harness(), controller = new AbortController();
  h.message.ackAck = async () => { h.order.push('ack'); controller.abort(); throw new Error('connection closed'); };
  assert.equal(await processDelivery(h.message, h.deps, controller.signal), 'interrupted');
  assert.deepEqual(h.order, ['http', 'ack']);
});
