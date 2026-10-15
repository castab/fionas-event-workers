import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CommerceClient } from '../src/commerce.ts';
import { ProcessingError } from '../src/errors.ts';
import { commerceRequest } from '../src/transform.ts';
import { fixture, receipt } from './fixtures.ts';
import { httpStub, json, token } from './http-stub.ts';
const signal = () => new AbortController().signal;
const event = fixture();
const body = commerceRequest(event);
const credentials = { serviceId: '11111111-1111-4111-8111-111111111111', secret: 'synthetic-secret' };

test('exchanges SERVICE credentials, caches tokens, sends exact body/key/bearer, parses receipt', async t => {
  const stub = await httpStub(t, (call, res) => json(res, call.path === '/inquiries' ? 201 : 200, call.path === '/inquiries' ? receipt : token()));
  const client = new CommerceClient({ baseUrl: stub.url, ...credentials });
  assert.deepEqual(await client.createInquiry(event.id, body, signal()), receipt);
  await client.createInquiry(event.id, body, signal());
  assert.equal(stub.calls.length, 3);
  assert.deepEqual(JSON.parse(stub.calls[0]!.body), credentials);
  for (const call of stub.calls.slice(1)) {
    assert.equal(call.headers.authorization, 'Bearer test-token-1');
    assert.equal(call.headers['idempotency-key'], event.id);
    assert.equal(call.headers['content-type'], 'application/json');
    assert.equal(call.body, JSON.stringify(body));
  }
});
test('refreshes shortly before expiry using expiresAt and preserves the request', async t => {
  let now = Date.now(), auth = 0;
  const stub = await httpStub(t, (call, res) => call.path === '/inquiries' ? json(res, 201, receipt) : json(res, 200, token(++auth, now + 900_000)));
  const client = new CommerceClient({ baseUrl: stub.url, ...credentials, now: () => now });
  await client.createInquiry(event.id, body, signal());
  now += 869_000; await client.createInquiry(event.id, body, signal()); assert.equal(auth, 1);
  now += 2_000; await client.createInquiry(event.id, body, signal()); assert.equal(auth, 2);
});
for (const repeated401 of [false, true]) test(`401 replaces the token exactly once; repeated401=${repeated401}`, async t => {
  let auth = 0, posts = 0;
  const stub = await httpStub(t, (call, res) => {
    if (call.path !== '/inquiries') json(res, 200, token(++auth));
    else json(res, ++posts === 1 || repeated401 ? 401 : 201, receipt);
  });
  const client = new CommerceClient({ baseUrl: stub.url, ...credentials });
  if (repeated401) await assert.rejects(client.createInquiry(event.id, body, signal()), (e: ProcessingError) => e.kind === 'systemic' && e.status === 401);
  else await client.createInquiry(event.id, body, signal());
  assert.equal(auth, 2); assert.equal(posts, 2);
  const inquiries = stub.calls.filter(call => call.path === '/inquiries');
  assert.equal(inquiries[0]!.body, inquiries[1]!.body);
  assert.equal(inquiries[1]!.headers.authorization, 'Bearer test-token-2');
  assert.equal(inquiries[1]!.headers['idempotency-key'], event.id);
});
for (const status of [400, 403, 408, 409, 422, 429, 500, 503]) test(`classifies HTTP ${status}, no refresh or in-process retry`, async t => {
  const stub = await httpStub(t, (call, res) => json(res, call.path === '/inquiries' ? status : 200, call.path === '/inquiries' ? {} : token()));
  const client = new CommerceClient({ baseUrl: stub.url, ...credentials });
  const kind = status === 400 || status === 422 ? 'permanent' : status === 403 || status === 409 ? 'systemic' : 'transient';
  await assert.rejects(client.createInquiry(event.id, body, signal()), (e: ProcessingError) => e.status === status && e.kind === kind);
  assert.equal(stub.calls.length, 2);
});
test('token exchange rejection is bounded and never reaches inquiries', async t => {
  const stub = await httpStub(t, (_call, res) => json(res, 401, {}));
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, signal()), (e: ProcessingError) => e.kind === 'systemic' && e.code === 'service_auth_rejected');
  assert.equal(stub.calls.length, 1);
});
for (const invalid of [
  {}, { ...token(), tokenType: 'Other' }, { ...token(), expiresAt: 'invalid' },
  { ...token(), accessToken: 'invalid token' }, { ...token(), expiresIn: 1.5 }, token(1, Date.now() - 1000),
]) test('malformed or expired tokens never reach inquiry POST', async t => {
  const stub = await httpStub(t, (_call, res) => json(res, 200, invalid));
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, signal()), (e: ProcessingError) => e.kind === 'systemic');
  assert.equal(stub.calls.length, 1);
});
test('abort during HTTP never invents a receipt', async t => {
  const controller = new AbortController();
  const stub = await httpStub(t, (call, res) => {
    if (call.path !== '/inquiries') json(res, 200, token());
    else controller.abort();
  });
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, controller.signal), (e: ProcessingError) => e.code === 'shutdown_aborted' && e.kind === 'transient');
});
for (const invalid of [{}, { ...receipt, id: 'bad' }, { ...receipt, createdAt: 'yesterday' }, { ...receipt, createdAt: '2026-02-30T00:00:00Z' }]) test(`malformed 201 receipt ${JSON.stringify(invalid)}`, async t => {
  const stub = await httpStub(t, (call, res) => json(res, call.path === '/inquiries' ? 201 : 200, call.path === '/inquiries' ? invalid : token()));
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, signal()), (e: ProcessingError) => e.code === 'invalid_inquiry_receipt');
});
test('invalid JSON receipt and unexpected 200 do not confirm persistence', async t => {
  let count = 0;
  const stub = await httpStub(t, (call, res) => {
    if (call.path !== '/inquiries') json(res, 200, token());
    else if (++count === 1) { res.writeHead(201); res.end('invalid-json'); }
    else json(res, 200, receipt);
  });
  const client = new CommerceClient({ baseUrl: stub.url, ...credentials });
  await assert.rejects(client.createInquiry(event.id, body, signal()), (e: ProcessingError) => e.code === 'invalid_inquiry_receipt');
  await assert.rejects(client.createInquiry(event.id, body, signal()), (e: ProcessingError) => e.status === 200);
});
test('timeout includes a stalled response body; lost response is transient', async t => {
  const stub = await httpStub(t, (call, res) => {
    if (call.path !== '/inquiries') json(res, 200, token());
    else { res.writeHead(201, { 'Content-Type': 'application/json' }); res.write('{'); }
  });
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials, timeoutMs: 100 }).createInquiry(event.id, body, signal()), (e: ProcessingError) => e.kind === 'transient' && e.code === 'http_timeout');
});
test('connection interruption after commit cannot invent a receipt', async t => {
  const stub = await httpStub(t, (call, res) => call.path !== '/inquiries' ? json(res, 200, token()) : res.destroy());
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, signal()), (e: ProcessingError) => e.kind === 'transient' && e.code === 'http_connection_failure');
});
test('redirects never forward credentials to another location', async t => {
  const stub = await httpStub(t, (call, res) => {
    if (call.path !== '/inquiries') json(res, 200, token());
    else { res.writeHead(307, { Location: '/another-endpoint' }); res.end(); }
  });
  await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, signal()), (e: ProcessingError) => e.kind === 'systemic' && e.status === 307);
  assert.equal(stub.calls.length, 2);
});

for (const [code, kind] of [['conflict', 'transient'], ['IDEMPOTENCY_KEY_REUSED', 'permanent'], ['unknown', 'systemic']] as const) {
  test(`409 ${code} is ${kind}, without exposing response details`, async t => {
    const stub = await httpStub(t, (call, res) => json(res, call.path === '/inquiries' ? 409 : 200,
      call.path === '/inquiries' ? { code, message: 'sensitive-backend-detail' } : token()));
    await assert.rejects(new CommerceClient({ baseUrl: stub.url, ...credentials }).createInquiry(event.id, body, signal()), (e: ProcessingError) => {
      assert.equal(e.kind, kind); assert.equal(JSON.stringify(e).includes('sensitive-backend-detail'), false); return true;
    });
    assert.equal(stub.calls.length, 2);
  });
}
