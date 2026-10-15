import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runLoop, Shutdown } from '../src/runtime.ts';
import { bytes, fixture, receipt } from './fixtures.ts';
import type { JsMsg } from '@nats-io/jetstream';
import { ProcessingError } from '../src/errors.ts';

test('systemic configuration failure stops before pulling or terminally rejecting another inquiry', async () => {
  let pulls = 0;
  const shutdown = new Shutdown(() => {});
  const message = { data: bytes(fixture()), timestamp: receipt.createdAt, info: { streamSequence: 1, deliveryCount: 1 },
    term: () => assert.fail('no TERM'), ackAck: () => assert.fail('no ACK'), nak: () => assert.fail('no NAK') } as unknown as JsMsg;
  try {
    await assert.rejects(runLoop({ next: async () => { pulls++; return message; } }, {
      commerce: { createInquiry: async () => { throw new ProcessingError('commerce_permission_denied', 'systemic', 403); } },
      publish: async () => assert.fail('no customer failure event for environment failures'), log() {},
    }, shutdown), /commerce_permission_denied/);
    assert.equal(pulls, 1);
  } finally { shutdown.dispose(); }
});

test('shutdown stops new pulls and lets the current operation confirm publication/ACK', async () => {
  let pulls = 0, acknowledged = false, published = false;
  const shutdown = new Shutdown(() => assert.fail('deadline should not fire'), 1000);
  const message = { data: bytes(fixture()), info: { streamSequence: 1, deliveryCount: 1 },
    ackAck: async () => { acknowledged = true; return true; }, nak: () => assert.fail('no failure expected') } as unknown as JsMsg;
  try {
    await runLoop({ next: async () => { pulls++; return message; } }, {
      commerce: { createInquiry: async () => { shutdown.stop(); await new Promise(resolve => setTimeout(resolve, 10)); return receipt; } },
      publish: async () => { published = true; return { stream: 'FIONAS_EVENTS', seq: 1, duplicate: false }; }, log() {},
    }, shutdown);
    assert.equal(pulls, 1); assert.equal(published, true); assert.equal(acknowledged, true);
  } finally { shutdown.dispose(); }
});
test('deadline aborts unfinished HTTP and leaves the input unacknowledged', async () => {
  let acknowledged = false, deadline = false;
  const shutdown = new Shutdown(() => { deadline = true; }, 20);
  const message = { data: bytes(fixture()), info: { streamSequence: 1, deliveryCount: 1 },
    ackAck: async () => { acknowledged = true; return true; }, nak: () => assert.fail('aborted input must await redelivery') } as unknown as JsMsg;
  try {
    await runLoop({ next: async () => message }, {
      commerce: { createInquiry: async (_id, _body, signal) => {
        shutdown.stop();
        await new Promise((_resolve, reject) => {
          const keepAlive = setTimeout(() => reject(new Error('deadline did not abort')), 500);
          signal.addEventListener('abort', () => { clearTimeout(keepAlive); reject(signal.reason); }, { once: true });
        });
        return receipt;
      } },
      publish: async () => assert.fail('cannot publish without receipt'), log() {},
    }, shutdown);
    assert.equal(deadline, true); assert.equal(acknowledged, false);
  } finally { shutdown.dispose(); }
});
test('transient pull failure recovers without resetting consumer position', async () => {
  let pulls = 0;
  const shutdown = new Shutdown(() => {}, 1000);
  try {
    await runLoop({ next: async () => { if (++pulls === 1) throw new Error('disconnected'); shutdown.stop(); return null; } }, {
      commerce: { createInquiry: async () => assert.fail('no message') },
      publish: async () => assert.fail('no message'), log() {},
    }, shutdown);
    assert.equal(pulls, 2);
  } finally { shutdown.dispose(); }
});
