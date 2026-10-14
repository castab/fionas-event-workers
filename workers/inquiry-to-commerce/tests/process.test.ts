import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processDelivery } from '../src/process.ts';
import type { Delivery, Dependencies } from '../src/process.ts';
import { ProcessingError } from '../src/errors.ts';
import { bytes, fixture, receipt } from './fixtures.ts';

function harness(attempt = 1, event: unknown = fixture()) {
  const order: string[] = [], logs: unknown[] = [], keys: string[] = [], outputs: Array<{ bytes: Uint8Array; options: unknown }> = [];
  const message: Delivery = {
    data: bytes(event), info: { deliveryCount: attempt, streamSequence: 7 },
    async ackAck() { order.push('ack'); return true; },
    nak(delay) { order.push(`nak:${delay}`); },
  };
  const deps: Dependencies = {
    commerce: { async createInquiry(id) { order.push('http'); keys.push(id); return receipt; } },
    async publish(subject, data, options) {
      order.push('publish'); outputs.push({ bytes: data, options });
      assert.equal(subject, 'fionas.events.inquiry.persisted.v1');
      return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: false };
    },
    log(level, event, fields) { logs.push({ level, event, fields }); },
  };
  return { order, logs, keys, outputs, message, deps, signal: new AbortController().signal };
}
test('HTTP then confirmed PubAck then confirmed input ACK; identifiers only in logs', async () => {
  const h = harness();
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, ['http', 'publish', 'ack']);
  const output = JSON.parse(new TextDecoder().decode(h.outputs[0]!.bytes));
  assert.deepEqual(h.outputs[0]!.options, { msgID: output.id, expect: { streamName: 'FIONAS_EVENTS' }, timeout: 5000 });
  assert.equal(JSON.stringify(h.logs).includes('jane@example.com'), false);
  assert.equal(JSON.stringify(h.logs).includes('Jane Doe'), false);
});
test('HTTP failure never publishes or ACKs and uses delayed NAK', async () => {
  const h = harness();
  h.deps.commerce.createInquiry = async () => { h.order.push('http'); throw new ProcessingError('http_timeout', false); };
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, ['http', 'nak:10000']);
});
test('outgoing failure never ACKs; redelivery uses identical HTTP key and outgoing bytes', async () => {
  const first = harness(), second = harness(2);
  first.deps.publish = async (_subject, data, options) => {
    first.order.push('publish'); first.outputs.push({ bytes: data, options }); throw new Error('publish failed');
  };
  await processDelivery(first.message, first.deps, first.signal);
  await processDelivery(second.message, second.deps, second.signal);
  assert.deepEqual(first.order, ['http', 'publish', 'nak:10000']);
  assert.deepEqual(first.keys, second.keys);
  assert.deepEqual(first.outputs, second.outputs);
  assert.deepEqual(second.order, ['http', 'publish', 'ack']);
});
test('duplicate PubAck is accepted, wrong stream/sequence are rejected', async () => {
  for (const ack of [
    { stream: 'FIONAS_EVENTS', seq: 1, duplicate: true },
    { stream: 'WRONG', seq: 1, duplicate: false },
    { stream: 'FIONAS_EVENTS', seq: 0, duplicate: false },
  ]) {
    const h = harness(); h.deps.publish = async () => { h.order.push('publish'); return ack; };
    await processDelivery(h.message, h.deps, h.signal);
    assert.equal(h.order.includes('ack'), ack.stream === 'FIONAS_EVENTS' && ack.seq > 0);
  }
});
test('unsupported and permanent failures surface operator attention without ACK or success event', async () => {
  const event = { ...fixture(), schemaVersion: 2 };
  const h = harness(1, event);
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, ['nak:3600000']);
  assert.match(JSON.stringify(h.logs), /operator_attention_required/);
  const rejected = harness();
  rejected.deps.commerce.createInquiry = async () => { throw new ProcessingError('commerce_permission_denied', true, 403); };
  await processDelivery(rejected.message, rejected.deps, rejected.signal);
  assert.deepEqual(rejected.order, ['nak:3600000']);
});
test('exhausted delivery is left in retained stream, no ACK or NAK', async () => {
  const h = harness(10, null);
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, []);
  assert.match(JSON.stringify(h.logs), /"exhausted":true/);
});
test('ACK cannot run while publication is still pending', async () => {
  const h = harness();
  let confirm!: () => void;
  const pending = new Promise<void>(resolve => { confirm = resolve; });
  h.deps.publish = async () => { h.order.push('publish'); await pending; return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: false }; };
  const processing = processDelivery(h.message, h.deps, h.signal);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(h.order, ['http', 'publish']);
  confirm(); await processing;
  assert.deepEqual(h.order, ['http', 'publish', 'ack']);
});
test('lost input ACK response is surfaced only after successful HTTP/publication', async () => {
  const h = harness();
  h.message.ackAck = async () => { h.order.push('ack'); throw new Error('ACK response lost'); };
  await processDelivery(h.message, h.deps, h.signal);
  assert.deepEqual(h.order, ['http', 'publish', 'ack', 'nak:10000']);
  assert.match(JSON.stringify(h.logs), /input_ack_unconfirmed/);
});
test('aborted work after persistence or publication remains unacknowledged for replay', async () => {
  for (const stage of ['http', 'publish']) {
    const h = harness(), controller = new AbortController();
    if (stage === 'http') h.deps.commerce.createInquiry = async () => { h.order.push('http'); controller.abort(); return receipt; };
    else h.deps.publish = async () => { h.order.push('publish'); controller.abort(); return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: false }; };
    await processDelivery(h.message, h.deps, controller.signal);
    assert.deepEqual(h.order, stage === 'http' ? ['http'] : ['http', 'publish']);
  }
});
