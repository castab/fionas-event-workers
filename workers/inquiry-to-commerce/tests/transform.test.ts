import { test } from 'node:test';
import assert from 'node:assert/strict';
import { commerceRequest, parseSubmitted } from '../src/transform.ts';
import { processingFailedEvent } from '../../../packages/events/src/index.ts';
import { bytes, fixture } from './fixtures.ts';

test('maps every supported field and preserves line order and exact strings without event metadata', () => {
  const event = fixture();
  event.data.lines.push({ description: 'Precise synthetic rate', quantity: '8.000000', unitPrice: '0.125000000000', taxAmount: '0.10', currency: 'USD' });
  const parsed = parseSubmitted(bytes(event));
  const request = commerceRequest(parsed);
  const { priceRevision: _ignored, ...expected } = event.data;
  assert.deepEqual(request, expected);
  assert.deepEqual(Object.keys(request).sort(), ['name', 'email', 'zipCode', 'eventDate', 'eventType', 'message', 'requestedService', 'lines'].sort());
  assert.equal(request.lines[2]?.unitPrice, '0.125000000000');
});
test('optional fields are omitted, never synthesized', () => {
  const event = fixture();
  delete event.data.message;
  event.data.requestedService = { guestCount: 75 };
  const request = commerceRequest(parseSubmitted(bytes(event)));
  assert.equal('message' in request, false);
  assert.deepEqual(request.requestedService, { guestCount: 75 });
});
test('unknown additive fields at every level are ignored and cannot change replay bodies', () => {
  const original = fixture();
  const added = JSON.parse(JSON.stringify(original));
  added.future = 'value'; added.data.future = 'value';
  added.data.requestedService.future = 'value'; added.data.requestedService.items[0].future = 'value';
  added.data.lines[0].future = 'value';
  assert.equal(JSON.stringify(commerceRequest(parseSubmitted(bytes(added)))), JSON.stringify(commerceRequest(parseSubmitted(bytes(original)))));
});
test('rejects malformed JSON/UTF8, unsupported versions/types, malformed envelope and required shapes', () => {
  for (const bad of [new TextEncoder().encode('{'), new Uint8Array([0xff]), bytes(null)]) assert.throws(() => parseSubmitted(bad));
  const mutations = [
    (e: any) => e.schemaVersion = 2,
    (e: any) => e.type = 'other',
    (e: any) => e.id = 'not-a-uuid',
    (e: any) => e.source = 'other',
    (e: any) => e.occurredAt = '2026-02-30T00:00:00Z',
    (e: any) => e.data = [],
    (e: any) => delete e.data.name,
    (e: any) => delete e.data.priceRevision,
    (e: any) => e.data.requestedService = null,
    (e: any) => e.data.requestedService.guestCount = '75',
    (e: any) => e.data.requestedService.guestCountIsMinimum = 1,
    (e: any) => e.data.lines = [],
    (e: any) => e.data.lines[0].unitPrice = 101,
    (e: any) => e.data.eventDate = '2026-02-30',
    (e: any) => e.data.message = null,
  ];
  for (const mutate of mutations) { const event = fixture(); mutate(event); assert.throws(() => parseSubmitted(bytes(event))); }
});
test('financial business validation remains with commerce', () => {
  const event = fixture();
  event.data.lines[0]!.unitPrice = '0.125'; // structurally usable but flat USD price fails backend settlement rules
  assert.equal(commerceRequest(parseSubmitted(bytes(event))).lines[0]?.unitPrice, '0.125');
});
test('HTTP projection is byte-identical across retries of the same input', () => {
  const first = JSON.stringify(commerceRequest(parseSubmitted(bytes(fixture()))));
  assert.equal(first, JSON.stringify(commerceRequest(parseSubmitted(bytes(fixture())))));
});
test('processing-failure event is deterministic and minimal; this worker defines no success event', () => {
  const event = fixture();
  const failure = processingFailedEvent(42, '2026-10-14T18:30:00.123456789Z', 'invalid_event_json', event.id);
  assert.deepEqual(failure, {
    schemaVersion: 1, type: 'fionas.inquiry.processing-failed', id: 'fionas.inquiry.processing-failed.v1:FIONAS_INQUIRIES:42',
    correlationId: event.id, occurredAt: '2026-10-14T18:30:00.123456789Z', source: 'fionas-event-workers',
    data: { inputStream: 'FIONAS_INQUIRIES', inputSequence: 42, errorCode: 'invalid_event_json' },
  });
  assert.equal(JSON.stringify(failure), JSON.stringify(processingFailedEvent(42, '2026-10-14T18:30:00.123456789Z', 'invalid_event_json', event.id)));
  assert.equal('correlationId' in processingFailedEvent(42, '2026-10-14T18:30:00Z', 'invalid_event_json'), false);
});
