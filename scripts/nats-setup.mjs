import { parseArgs } from 'node:util';
import { createInterface, emitKeypressEvents } from 'node:readline';
import { pathToFileURL } from 'node:url';
import { connect } from '@nats-io/transport-node';
import { jetstreamManager } from '@nats-io/jetstream';

export class SetupError extends Error {}

// No output is attached to password input: neither characters nor defaults are echoed.
export function hiddenInput(input, output, label, signal) {
  if (!input.isTTY || typeof input.setRawMode !== 'function') {
    return Promise.reject(new SetupError('Hidden password input requires a terminal.'));
  }
  return new Promise((resolve, reject) => {
    let value = '';
    let settled = false;
    const wasRaw = !!input.isRaw;
    const wasPaused = input.isPaused();
    const finish = (error) => {
      if (settled) return;
      settled = true;
      input.off('keypress', keypress);
      input.off('end', ended);
      signal?.removeEventListener('abort', aborted);
      input.setRawMode(wasRaw);
      if (wasPaused) input.pause();
      output.write('\n');
      if (error) reject(error); else resolve(value);
    };
    const cancelled = () => new SetupError('Setup cancelled.');
    const aborted = () => finish(cancelled());
    const ended = () => finish(cancelled());
    const keypress = (text, key = {}) => {
      if (key.ctrl && ['c', 'd'].includes(key.name)) finish(cancelled());
      else if (['return', 'enter'].includes(key.name)) finish();
      else if (key.name === 'backspace') value = [...value].slice(0, -1).join('');
      else if (text && !key.ctrl && !key.meta && !/[\x00-\x1f\x7f]/.test(text)) value += text;
    };
    if (signal?.aborted) { reject(cancelled()); return; }
    emitKeypressEvents(input);
    output.write(label);
    input.setRawMode(true);
    input.on('keypress', keypress);
    input.once('end', ended);
    signal?.addEventListener('abort', aborted, { once: true });
    input.resume();
  });
}

export async function terminalQuestion(label, { defaultValue, secret = false, signal } = {}) {
  if (secret) return hiddenInput(process.stdin, process.stdout, `${label}: `, signal);
  const prompt = `${label}${defaultValue ? ` [${defaultValue}]` : ''}: `;
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  let abort;
  try {
    return await new Promise((resolve, reject) => {
      rl.once('SIGINT', () => reject(new SetupError('Setup cancelled.')));
      rl.once('close', () => reject(new SetupError('Setup cancelled.')));
      abort = () => reject(new SetupError('Setup cancelled.'));
      signal?.addEventListener('abort', abort, { once: true });
      if (signal?.aborted) abort();
      else rl.question(prompt, answer => { signal?.removeEventListener('abort', abort); resolve(answer); });
    });
  } finally { signal?.removeEventListener('abort', abort); rl.close(); }
}

export async function collectConnection(env, ask = terminalQuestion, signal) {
  // Never display an environment URL containing embedded credentials.
  let defaultUrl = 'nats://localhost:4222';
  if (env.NATS_URL) {
    try {
      const url = new URL(env.NATS_URL);
      if (url.username || url.password || url.search || url.hash) throw new Error();
      if (!['nats:', 'tls:'].includes(url.protocol)) throw new Error();
      defaultUrl = env.NATS_URL;
    } catch { throw new SetupError('Invalid NATS_URL; use nats:// or tls:// without embedded credentials.'); }
  }
  const withDefault = async (label, value) => (await ask(label, { defaultValue: value, signal })).trim() || value;
  const url = await withDefault('NATS server address', defaultUrl);
  const defaultMethod = env.NATS_CREDS_FILE ? '1' : env.NATS_USER ? '2' : '1';
  const method = await withDefault('Operator authentication: 1 = .creds file, 2 = username/password, 3 = none (local development only)', defaultMethod);
  const result = { NATS_URL: url };
  if (method === '1') {
    result.NATS_CREDS_FILE = await withDefault('Operator .creds file path', env.NATS_CREDS_FILE ?? '');
    if (!result.NATS_CREDS_FILE) throw new SetupError('An operator .creds file path is required.');
  } else if (method === '2') {
    result.NATS_USER = await withDefault('Operator username', env.NATS_USER ?? '');
    const label = env.NATS_PASSWORD ? 'Operator password (Enter keeps configured password)' : 'Operator password';
    result.NATS_PASSWORD = await ask(label, { secret: true, signal }) || env.NATS_PASSWORD || '';
    if (!result.NATS_USER || !result.NATS_PASSWORD) throw new SetupError('Operator username and password are required.');
  } else if (method !== '3') throw new SetupError('Choose authentication option 1, 2 or 3.');
  return result;
}

export async function setup(argv = process.argv.slice(2), env = process.env) {
  let values;
  try {
    ({ values } = parseArgs({ args: argv, options: {
      'retention-days': { type: 'string', default: '30' }, replicas: { type: 'string', default: '1' },
      'non-interactive': { type: 'boolean', default: false }, help: { type: 'boolean', short: 'h' },
    } }));
  } catch { throw new SetupError('Invalid setup options; use --help.'); }
  if (values.help) {
    console.log('Usage: node scripts/nats-setup.mjs [--retention-days 30] [--replicas 1] [--non-interactive]\nInteractive setup prompts for a server and operator credentials. Automation uses NATS_URL and NATS_CREDS_FILE or NATS_USER/NATS_PASSWORD.');
    return;
  }
  if (!values['non-interactive'] && (!process.stdin.isTTY || !process.stdout.isTTY)) {
    throw new SetupError('Interactive setup requires a terminal; use --non-interactive with NATS environment variables.');
  }
  let configModule, provisionModule;
  try {
    [configModule, provisionModule] = await Promise.all([
      import('../workers/inquiry-to-commerce/dist/workers/inquiry-to-commerce/src/config.js'),
      import('../workers/inquiry-to-commerce/dist/workers/inquiry-to-commerce/src/provision.js'),
    ]);
  } catch { throw new SetupError('Compiled setup modules are missing; run npm run build --workspace @fionas/inquiry-to-commerce.'); }
  const { connectionOptions, natsConfig } = configModule;
  const { provision, outcomeStreamConfig, inputConsumerConfig, ProvisioningMismatch } = provisionModule;
  const days = Number(values['retention-days']), replicas = Number(values.replicas);
  const stream = outcomeStreamConfig(days, replicas); // Validate before prompting or connecting.
  const controller = new AbortController();
  let nc;
  const cancel = () => { controller.abort(); void nc?.close(); };
  process.on('SIGINT', cancel); process.on('SIGTERM', cancel);
  try {
    const selected = values['non-interactive'] ? env : await collectConnection(env, terminalQuestion, controller.signal);
    const config = natsConfig(selected);
    console.log(`Target: ${config.url}\nFIONAS_INQUIRIES must already exist and will remain unchanged.`);
    console.log(`Intended configuration (durations in nanoseconds; ${days} days retention, 24 hours deduplication):\n${JSON.stringify({ stream, consumer: inputConsumerConfig() }, null, 2)}`);
    let options;
    try { options = await connectionOptions(config, 'fionas-workers-setup'); }
    catch { throw new SetupError('Cannot read operator credentials; check the mounted .creds file.'); }
    controller.signal.throwIfAborted();
    // Setup is a bounded one-shot operator command, not the reconnecting runtime process.
    nc = await connect({ ...options, reconnect: false });
    controller.signal.throwIfAborted();
    const result = await provision(await jetstreamManager(nc, { checkAPI: false, timeout: 5_000 } ), days, replicas);
    console.log(`FIONAS_EVENTS: ${result.stream}. inquiry-to-commerce: ${result.consumer}. FIONAS_INQUIRIES unchanged.`);
  } catch (error) {
    if (controller.signal.aborted) throw new SetupError('Setup cancelled.');
    if (error instanceof ProvisioningMismatch) {
      console.error(`${error.message}\n${JSON.stringify(error.details, null, 2)}`);
      throw new SetupError('Existing resources were not changed.');
    }
    throw error;
  } finally {
    await nc?.close();
    process.off('SIGINT', cancel); process.off('SIGTERM', cancel);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  setup().catch(error => {
    const authored = error instanceof SetupError || (error instanceof Error && /^(Missing NATS_URL|Invalid NATS_URL|NATS_|Choose NATS_|Retention must|Replicas must|FIONAS_INQUIRIES must already exist)/.test(error.message));
    console.error(authored ? error.message : 'Provisioning failed; check operator credentials, NATS availability and the existing input stream.');
    process.exitCode = 1;
  });
}
