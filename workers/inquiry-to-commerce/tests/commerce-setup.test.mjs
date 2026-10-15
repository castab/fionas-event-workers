import { test } from 'node:test';
import assert from 'node:assert/strict';
import { collectTarget, commerceClient, plan, provision, parseBaseUrl, ROLE_KEY, PERMISSION } from '../../../scripts/commerce-service-setup.mjs';

function answers(values) {
  const questions = [];
  return { questions, ask: async (label, options) => { questions.push({ label, options }); return values.shift() ?? ''; } };
}
function fakeCommerce(state = {}) {
  const calls = [];
  const json = (status, body) => new Response(body === undefined ? null : JSON.stringify(body), { status });
  const fetchImpl = async (url, init) => {
    const { pathname } = new URL(url);
    calls.push({ method: init.method, pathname, headers: init.headers, body: init.body });
    if (pathname === '/auth/login') return new Response(null, { status: 204, headers: { 'Set-Cookie': '__Host-fionas_session=abc; Secure; HttpOnly; Path=/' } });
    if (pathname === '/auth/logout') return json(204);
    if (pathname === '/admin/access/services' && init.method === 'GET') return json(200, { services: state.services ?? [] });
    if (pathname === '/admin/access/services') return json(201, { id: 'svc-1', name: 'inquiry-to-commerce', status: 'ACTIVE', roles: [] });
    if (pathname === `/admin/access/roles/${ROLE_KEY}`) return state.role ? json(200, state.role) : json(404, { error: 'missing' });
    if (pathname === '/admin/access/roles') return json(201, {});
    if (pathname.endsWith(`/roles/${ROLE_KEY}`)) return json(204);
    if (pathname.endsWith('/credentials')) return json(201, { credentialId: 'cred-1', secret: 'synthetic-secret' });
    return json(500);
  };
  return { calls, client: commerceClient({ baseUrl: 'http://commerce.test', origin: 'http://web.test' }, fetchImpl) };
}

test('base URL must be a bare HTTP(S) origin', () => {
  assert.equal(parseBaseUrl(' https://commerce.example.com/ '), 'https://commerce.example.com');
  for (const bad of ['', 'ftp://x', 'https://u:p@x', 'https://x/path', 'https://x/?q=1']) assert.throws(() => parseBaseUrl(bad));
});
test('prompts for base URL first, defaults from environment, and hides the password', async () => {
  const prompt = answers(['https://commerce.example.com', '', 'admin', 'pw']);
  const target = await collectTarget({ FIONAS_TRUSTED_ORIGIN: 'https://web.example.com' }, prompt.ask);
  assert.deepEqual(target, { baseUrl: 'https://commerce.example.com', origin: 'https://web.example.com', username: 'admin', password: 'pw' });
  assert.match(prompt.questions[0].label, /base URL/);
  assert.equal(prompt.questions[3].options.secret, true);
  await assert.rejects(collectTarget({}, answers(['', '', '', '']).ask));
});
test('provisions a new service, role, assignment and credential using the session cookie and Origin', async () => {
  const { client, calls } = fakeCommerce();
  await client.login('admin', 'pw');
  const result = await provision(client, await plan(client), 'label');
  assert.deepEqual([result.serviceId, result.secret], ['svc-1', 'synthetic-secret']);
  const roleCall = calls.find(c => c.pathname === '/admin/access/roles');
  assert.deepEqual(JSON.parse(roleCall.body).permissions, [PERMISSION]);
  assert.ok(calls.slice(1).every(c => c.headers.Cookie === '__Host-fionas_session=abc' && c.headers.Origin === 'http://web.test'));
});
test('reuses an existing service and correct role; refuses a role with other permissions', async () => {
  const state = { services: [{ id: 'svc-9', name: 'inquiry-to-commerce', status: 'ACTIVE' }], role: { permissions: [PERMISSION] } };
  const { client, calls } = fakeCommerce(state);
  await client.login('admin', 'pw');
  const result = await provision(client, await plan(client), 'label');
  assert.equal(result.serviceId, 'svc-9');
  assert.equal(calls.some(c => c.method === 'POST' && c.pathname === '/admin/access/services'), false);
  const broad = fakeCommerce({ role: { permissions: [PERMISSION, 'fionas.inquiries.read'] } });
  await broad.client.login('admin', 'pw');
  await assert.rejects(plan(broad.client), /exactly/);
});
test('failures report status only, never a response body', async () => {
  const client = commerceClient({ baseUrl: 'http://x.test', origin: 'http://x.test' }, async () => new Response('do-not-echo', { status: 403 }));
  await assert.rejects(client.login('a', 'b'), error => /403/.test(error.message) && !error.message.includes('do-not-echo'));
});
