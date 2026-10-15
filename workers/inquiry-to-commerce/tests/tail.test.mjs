import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { tailOptions, messageRecord } from '../../../scripts/nats-tail.mjs';
const exec = promisify(execFile);

test('tail maps only the two supported streams and defaults to new messages', () => {
  assert.deepEqual(tailOptions(['events']), { stream: 'FIONAS_EVENTS', all: false, limit: Infinity, nonInteractive: false });
  assert.deepEqual(tailOptions(['inquiries', '--all', '--limit', '5', '--non-interactive']),
    { stream: 'FIONAS_INQUIRIES', all: true, limit: 5, nonInteractive: true });
  for (const argv of [[], ['other'], ['constructor'], ['events', 'inquiries'], ['events', '--limit', '0'], ['events', '--limit', '1.5'], ['events', '--limit', 'Infinity'], ['events', '--bad']]) {
    assert.throws(() => tailOptions(argv));
  }
});
test('tail emits complete JSON-line records with parsed payload and multi-value headers', () => {
  const message = { data: new TextEncoder().encode('{"schemaVersion":1,"note":"line\\nnext"}'), seq: 7,
    subject: 'fionas.events.example.v1', timestamp: '2026-10-14T18:30:00Z',
    headers: { keys: () => ['Example'], values: () => ['one', 'two'] } };
  const record = messageRecord('FIONAS_EVENTS', message);
  assert.deepEqual(record.payload, { schemaVersion: 1, note: 'line\nnext' });
  assert.deepEqual(record.headers, { Example: ['one', 'two'] });
  assert.equal(record.sequence, 7);
  assert.equal(JSON.stringify(record).includes('\n'), false);
});
test('non-JSON text and binary messages can be inspected without crashing the viewer', () => {
  const message = { seq: 1, subject: 'example', timestamp: '2026-10-14T18:30:00Z' };
  assert.equal(messageRecord('FIONAS_EVENTS', { ...message, data: new TextEncoder().encode('hello') }).payload, 'hello');
  assert.deepEqual(messageRecord('FIONAS_EVENTS', { ...message, data: new Uint8Array([255]) }).payload, { encoding: 'base64', data: '/w==' });
});
test('tail CLI help works and piped input requires explicit non-interactive mode', async () => {
  assert.match((await exec(process.execPath, ['scripts/nats-tail.mjs', 'events', '--help'])).stdout, /--all/);
  await assert.rejects(exec(process.execPath, ['scripts/nats-tail.mjs', 'events']), error => /requires a terminal/.test(error.stderr));
  await assert.rejects(exec(process.execPath, ['scripts/nats-tail.mjs', 'events', '--limit', '0', '--non-interactive']), error => /positive whole number/.test(error.stderr));
});
