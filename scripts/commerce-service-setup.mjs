import { parseArgs } from 'node:util';
import { writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { SetupError, terminalQuestion } from './nats-setup.mjs';

export const SERVICE_NAME = 'inquiry-to-commerce';
export const ROLE_KEY = 'fionas.inquiry-worker';
export const PERMISSION = 'fionas.inquiries.create';
const SESSION_COOKIE = '__Host-fionas_session';
const TIMEOUT_MS = 10_000;

export function parseBaseUrl(value) {
  let url;
  try { url = new URL(value.trim()); } catch { throw new SetupError('The commerce base URL must be an absolute HTTP(S) origin.'); }
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
    throw new SetupError('The commerce base URL must be an HTTP(S) origin without credentials, path, query or fragment.');
  }
  return url.origin;
}

export async function collectTarget(env, ask = terminalQuestion, signal) {
  const withDefault = async (label, value) => (await ask(label, { defaultValue: value, signal })).trim() || value;
  const baseUrl = parseBaseUrl(await withDefault('fionas-commerce base URL', env.COMMERCE_BASE_URL ?? ''));
  const origin = parseBaseUrl(await withDefault('Trusted browser Origin (a value in FIONAS_TRUSTED_ORIGINS)', env.FIONAS_TRUSTED_ORIGIN ?? baseUrl));
  const username = (await ask('Commerce administrator username', { signal })).trim();
  const password = await ask('Commerce administrator password', { secret: true, signal });
  if (!username || !password) throw new SetupError('An administrator username and password are required.');
  return { baseUrl, origin, username, password };
}

// A thin client over the commerce admin API. Bodies are only returned to the caller; failures name
// the operation and status but never echo a response body, which could carry credential material.
export function commerceClient({ baseUrl, origin }, fetchImpl = fetch) {
  let cookie;
  async function call(method, path, body, { expect, allow = [] } = {}) {
    let response;
    try {
      response = await fetchImpl(new URL(path, baseUrl), {
        method,
        redirect: 'manual',
        signal: AbortSignal.timeout(TIMEOUT_MS),
        headers: { Origin: origin, ...(cookie ? { Cookie: cookie } : {}), ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
    } catch { throw new SetupError(`${method} ${path}: commerce is unreachable or timed out.`); }
    if (allow.includes(response.status)) return { status: response.status };
    if (response.status !== expect) throw new SetupError(`${method} ${path}: unexpected status ${response.status}.`);
    const text = await response.text();
    return { status: response.status, response, json: text ? JSON.parse(text) : undefined };
  }
  return {
    async login(username, password) {
      const { response } = await call('POST', '/auth/login', { username, password }, { expect: 204 });
      const pair = response.headers.getSetCookie().map(c => c.split(';')[0]).find(c => c.startsWith(`${SESSION_COOKIE}=`));
      if (!pair) throw new SetupError('Login did not return a staff session cookie.');
      cookie = pair;
    },
    async logout() { try { await call('POST', '/auth/logout', undefined, { expect: 204 }); } catch { /* best effort */ } cookie = undefined; },
    call,
  };
}

export async function plan(client) {
  const { json: services } = await client.call('GET', '/admin/access/services', undefined, { expect: 200 });
  const service = services.services.find(s => s.name === SERVICE_NAME);
  const role = await client.call('GET', `/admin/access/roles/${ROLE_KEY}`, undefined, { expect: 200, allow: [404] });
  if (role.json) {
    const permissions = role.json.permissions;
    if (permissions.length !== 1 || permissions[0] !== PERMISSION) {
      throw new SetupError(`Role ${ROLE_KEY} exists with permissions other than exactly ${PERMISSION}; reconcile it manually. Nothing was changed.`);
    }
  }
  if (service && service.status !== 'ACTIVE') throw new SetupError(`Service ${SERVICE_NAME} exists but is ${service.status}; reconcile it manually. Nothing was changed.`);
  return { service, roleExists: !!role.json };
}

export async function provision(client, found, label) {
  const steps = [];
  let serviceId = found.service?.id;
  if (serviceId) steps.push(`service ${SERVICE_NAME}: verified`);
  else {
    const { json } = await client.call('POST', '/admin/access/services', { name: SERVICE_NAME }, { expect: 201 });
    serviceId = json.id; steps.push(`service ${SERVICE_NAME}: created`);
  }
  if (found.roleExists) steps.push(`role ${ROLE_KEY}: verified`);
  else {
    await client.call('POST', '/admin/access/roles', {
      key: ROLE_KEY, displayName: 'Inquiry-to-commerce worker', description: `Submits priced inquiries only (${PERMISSION}).`, permissions: [PERMISSION],
    }, { expect: 201 });
    steps.push(`role ${ROLE_KEY}: created`);
  }
  await client.call('PUT', `/admin/access/services/${serviceId}/roles/${ROLE_KEY}`, undefined, { expect: 204 });
  steps.push('role assignment: ensured');
  const { json } = await client.call('POST', `/admin/access/services/${serviceId}/credentials`, { label }, { expect: 201 });
  steps.push(`credential ${json.credentialId}: created`);
  return { serviceId, credentialId: json.credentialId, secret: json.secret, steps };
}

export async function setup(argv = process.argv.slice(2), env = process.env, ask = terminalQuestion) {
  let values;
  try {
    ({ values } = parseArgs({ args: argv, options: {
      'secret-out': { type: 'string', default: 'commerce-service.secret' },
      'print-secret': { type: 'boolean', default: false },
      yes: { type: 'boolean', default: false }, help: { type: 'boolean', short: 'h' },
    } }));
  } catch { throw new SetupError('Invalid options; use --help.'); }
  if (values.help) {
    console.log(`Usage: node scripts/commerce-service-setup.mjs [--secret-out commerce-service.secret] [--print-secret] [--yes]
Prompts for the fionas-commerce base URL, a trusted Origin and administrator credentials, then creates (or verifies) the
${SERVICE_NAME} SERVICE, the ${ROLE_KEY} role granting exactly ${PERMISSION}, and issues a new credential.
The secret is written to a private file (never overwritten) or printed with --print-secret. Each run issues a new credential.`);
    return;
  }
  if (!process.stdin.isTTY || !process.stdout.isTTY) throw new SetupError('Setup requires a terminal.');
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.on('SIGINT', cancel); process.on('SIGTERM', cancel);
  let client;
  try {
    const target = await collectTarget(env, ask, controller.signal);
    console.log(`Target: ${target.baseUrl}`);
    client = commerceClient(target);
    await client.login(target.username, target.password);
    const found = await plan(client);
    console.log(`Will ensure service "${SERVICE_NAME}" (${found.service ? 'exists' : 'new'}), role "${ROLE_KEY}" (${found.roleExists ? 'exists' : 'new'}) with only ${PERMISSION}, and issue a NEW credential.`);
    if (!values.yes) {
      const answer = (await ask('Proceed? (y/N)', { signal: controller.signal })).trim().toLowerCase();
      if (answer !== 'y' && answer !== 'yes') throw new SetupError('Setup cancelled. Nothing was changed.');
    }
    const label = `${SERVICE_NAME} ${new Date().toISOString()}`;
    const result = await provision(client, found, label);
    // The secret exists only in this response, so deliver it before anything else can fail.
    if (values['print-secret']) console.log(`COMMERCE_SERVICE_SECRET=${result.secret}`);
    else {
      await writeFile(values['secret-out'], result.secret, { flag: 'wx', mode: 0o600 });
      console.log(`Secret written to ${values['secret-out']} (private; do not commit).`);
    }
    console.log(`${result.steps.join('\n')}\nCOMMERCE_SERVICE_ID=${result.serviceId}\nSet COMMERCE_SERVICE_ID and COMMERCE_SERVICE_SECRET (or COMMERCE_SERVICE_SECRET_FILE) in the worker's environment.`);
  } catch (error) {
    if (controller.signal.aborted) throw new SetupError('Setup cancelled.');
    throw error;
  } finally {
    await client?.logout();
    process.off('SIGINT', cancel); process.off('SIGTERM', cancel);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  setup().catch(error => {
    console.error(error instanceof SetupError ? error.message : error.code === 'EEXIST'
      ? 'The secret file already exists; choose another --secret-out. The credential was issued but its secret was lost: revoke it in commerce and rerun.'
      : 'Commerce service setup failed.');
    process.exitCode = 1;
  });
}
