import { test } from 'node:test';
import assert from 'node:assert/strict';
import { workerConfig, natsConfig } from '../src/config.ts';
import { differences, outcomeStreamConfig, inputConsumerConfig } from '../src/provision.ts';
test('configuration validates required values and avoids embedded credentials', () => {
  const env = { NATS_URL: 'nats://localhost:4222', COMMERCE_BASE_URL: 'https://commerce.example.com',
    COMMERCE_SERVICE_ID: '11111111-1111-4111-8111-111111111111', COMMERCE_SERVICE_SECRET: 'synthetic-secret' };
  assert.equal(workerConfig(env).commerce.baseUrl, env.COMMERCE_BASE_URL);
  for (const changes of [{ NATS_URL: 'nats://user:secret@localhost' }, { COMMERCE_BASE_URL: 'https://user:secret@commerce.example.com' },
    { COMMERCE_BASE_URL: 'https://commerce.example.com/path' }, { COMMERCE_SERVICE_ID: 'bad' }, { COMMERCE_SERVICE_SECRET: '' },
    { COMMERCE_SERVICE_SECRET_FILE: '/secret' }]) assert.throws(() => workerConfig({ ...env, ...changes }));
  assert.throws(() => natsConfig({ NATS_URL: env.NATS_URL, NATS_USER: 'user' }));
  assert.throws(() => natsConfig({ NATS_URL: env.NATS_URL, NATS_CREDS_FILE: '/creds', NATS_USER: 'user', NATS_PASSWORD: 'secret' }));
});
test('provisioning defaults are finite, distinct and bounded; differences report rather than overwrite', () => {
  const stream = outcomeStreamConfig();
  assert.equal(stream.max_age, 30 * 86_400_000_000_000);
  assert.equal(stream.duplicate_window, 86_400_000_000_000);
  assert.deepEqual(stream.subjects, ['fionas.events.>']);
  assert.equal(inputConsumerConfig().max_ack_pending, 1);
  assert.equal(inputConsumerConfig().max_deliver, 10);
  assert.deepEqual(differences({ ...stream, storage: 'memory' }, stream), ['storage']);
  assert.throws(() => outcomeStreamConfig(0)); assert.throws(() => outcomeStreamConfig(30, 0));
});
