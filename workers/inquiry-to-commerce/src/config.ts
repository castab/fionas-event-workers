import { readFile } from 'node:fs/promises';
import { credsAuthenticator } from '@nats-io/transport-node';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import { isUuid } from './transform.ts';

type Env = Record<string, string | undefined>;
function required(env: Env, key: string): string {
  const value = env[key];
  if (!value?.trim()) throw new Error(`Missing ${key}`);
  return value;
}
export function natsConfig(env: Env): { url: string; credsFile?: string; user?: string; pass?: string; token?: string } {
  const url = required(env, 'NATS_URL');
  let parsed: URL;
  try { parsed = new URL(url); } catch { throw new Error('Invalid NATS_URL'); }
  if (!['nats:', 'tls:'].includes(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) {
    throw new Error('NATS_URL must use nats:// or tls:// without embedded credentials');
  }
  const methods = [env.NATS_CREDS_FILE, env.NATS_USER || env.NATS_PASSWORD, env.NATS_TOKEN].filter(Boolean).length;
  if (methods > 1) throw new Error('Choose NATS_CREDS_FILE, NATS_USER/NATS_PASSWORD or NATS_TOKEN');
  if (!!env.NATS_USER !== !!env.NATS_PASSWORD) throw new Error('NATS_USER and NATS_PASSWORD must be supplied together');
  return {
    url,
    ...(env.NATS_CREDS_FILE ? { credsFile: env.NATS_CREDS_FILE } : {}),
    ...(env.NATS_USER ? { user: env.NATS_USER, pass: env.NATS_PASSWORD! } : {}),
    ...(env.NATS_TOKEN ? { token: env.NATS_TOKEN } : {}),
  };
}
export function workerConfig(env: Env = process.env) {
  const nats = natsConfig(env);
  let base: URL;
  try { base = new URL(required(env, 'COMMERCE_BASE_URL')); }
  catch { throw new Error('COMMERCE_BASE_URL must be an absolute HTTP(S) origin'); }
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash || base.pathname !== '/') {
    throw new Error('COMMERCE_BASE_URL must be an HTTP(S) origin without credentials, path, query or fragment');
  }
  const serviceId = required(env, 'COMMERCE_SERVICE_ID');
  if (!isUuid(serviceId)) throw new Error('COMMERCE_SERVICE_ID must be a UUID');
  const secret = env.COMMERCE_SERVICE_SECRET;
  const secretFile = env.COMMERCE_SERVICE_SECRET_FILE;
  if (!!secret === !!secretFile) throw new Error('Supply exactly one of COMMERCE_SERVICE_SECRET or COMMERCE_SERVICE_SECRET_FILE');
  return { nats, commerce: { baseUrl: base.origin, serviceId, ...(secret ? { secret } : {}), ...(secretFile ? { secretFile } : {}) } };
}
export async function connectionOptions(config: ReturnType<typeof natsConfig>, name: string): Promise<NodeConnectionOptions> {
  return {
    servers: config.url, name, timeout: 5_000,
    maxReconnectAttempts: -1, reconnectTimeWait: 2_000,
    ...(config.credsFile ? { authenticator: credsAuthenticator(await readFile(config.credsFile)) } : {}),
    ...(config.user ? { user: config.user, pass: config.pass! } : {}),
    ...(config.token ? { token: config.token } : {}),
  };
}
