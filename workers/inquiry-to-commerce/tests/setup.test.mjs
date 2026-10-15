import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { collectConnection, hiddenInput } from '../../../scripts/nats-setup.mjs';
const exec = promisify(execFile);

function answers(values) {
  const questions = [];
  return { questions, ask: async (label, options) => { questions.push({ label, options }); return values.shift() ?? ''; } };
}
test('interactive setup selects address and credentials file using environment defaults', async () => {
  const prompt = answers(['', '', '']);
  const result = await collectConnection({ NATS_URL: 'tls://nats.example.com:4222', NATS_CREDS_FILE: '/mounted/operator.creds' }, prompt.ask);
  assert.deepEqual(result, { NATS_URL: 'tls://nats.example.com:4222', NATS_CREDS_FILE: '/mounted/operator.creds' });
  assert.equal(prompt.questions.length, 3);
});
test('authentication choice isolates selected credentials; passwords have no visible default', async () => {
  const prompt = answers(['nats://localhost:4222', '2', 'operator', '']);
  const secret = 'synthetic-private-password';
  assert.deepEqual(await collectConnection({ NATS_CREDS_FILE: '/old.creds', NATS_PASSWORD: secret }, prompt.ask),
    { NATS_URL: 'nats://localhost:4222', NATS_USER: 'operator', NATS_PASSWORD: secret });
  assert.equal(prompt.questions[3].options.secret, true);
  assert.equal(JSON.stringify(prompt.questions).includes(secret), false);
  const none = answers(['', '3']);
  assert.deepEqual(await collectConnection({ NATS_USER: 'operator', NATS_PASSWORD: secret }, none.ask), { NATS_URL: 'nats://localhost:4222' });
});
test('invalid authentication/credentials and embedded secrets fail without being echoed', async () => {
  for (const replies of [['', 'bad'], ['', '1', ''], ['', '2', '', 'password'], ['', '2', 'operator', '']]) {
    await assert.rejects(collectConnection({}, answers(replies).ask));
  }
  const prompt = answers([]);
  await assert.rejects(collectConnection({ NATS_URL: 'nats://operator:do-not-echo@example.com:4222' }, prompt.ask), error => !error.message.includes('do-not-echo'));
  assert.equal(prompt.questions.length, 0);
});

function fakeTerminal(raw = false) {
  const input = new PassThrough();
  input.isTTY = true; input.isRaw = raw;
  const modes = [];
  input.setRawMode = value => { modes.push(value); input.isRaw = value; };
  input.pause();
  let output = '';
  return { input, modes, output: { write: value => { output += value; } }, text: () => output };
}
test('hidden password handles backspace without echo and restores terminal state/listeners', async () => {
  const terminal = fakeTerminal();
  const password = hiddenInput(terminal.input, terminal.output, 'Password: ');
  terminal.input.emit('keypress', 'secretx', {});
  terminal.input.emit('keypress', '', { name: 'backspace' });
  terminal.input.emit('keypress', '', { name: 'return' });
  assert.equal(await password, 'secret');
  assert.equal(terminal.text(), 'Password: \n');
  assert.deepEqual(terminal.modes, [true, false]);
  assert.equal(terminal.input.isPaused(), true);
  assert.equal(terminal.input.listenerCount('keypress'), 0);
  terminal.input.destroy();
});
test('hidden password decodes actual keypress data and preserves an already raw terminal', async () => {
  const terminal = fakeTerminal(true);
  const password = hiddenInput(terminal.input, terminal.output, 'Password: ');
  terminal.input.write('synthetic-secret\r');
  assert.equal(await password, 'synthetic-secret');
  assert.deepEqual(terminal.modes, [true, true]);
  assert.equal(terminal.text().includes('synthetic-secret'), false);
  terminal.input.destroy();
});
for (const cancellation of ['ctrl-c', 'ctrl-d', 'signal', 'end']) test(`hidden password cancellation: ${cancellation}`, async () => {
  const terminal = fakeTerminal();
  const controller = new AbortController();
  const password = hiddenInput(terminal.input, terminal.output, 'Password: ', controller.signal);
  terminal.input.emit('keypress', 'secret', {});
  if (cancellation === 'signal') controller.abort();
  else if (cancellation === 'end') terminal.input.emit('end');
  else terminal.input.emit('keypress', '', { ctrl: true, name: cancellation === 'ctrl-c' ? 'c' : 'd' });
  await assert.rejects(password, /cancelled/);
  assert.equal(terminal.input.isRaw, false);
  assert.equal(terminal.input.listenerCount('keypress'), 0);
  assert.equal(terminal.text().includes('secret'), false);
  terminal.input.destroy();
});
test('nonterminal password input fails without attempting to echo', async () => {
  const input = new PassThrough();
  await assert.rejects(hiddenInput(input, { write: () => assert.fail('must not echo') }, 'Password'), /requires a terminal/);
  input.destroy();
});
test('CLI help and missing terminal have bounded, actionable behavior without a build', async () => {
  const help = await exec(process.execPath, ['scripts/nats-setup.mjs', '--help']);
  assert.match(help.stdout, /--non-interactive/);
  await assert.rejects(exec(process.execPath, ['scripts/nats-setup.mjs']), error => {
    assert.equal(error.code, 1); assert.match(error.stderr, /requires a terminal/); return true;
  });
  await assert.rejects(exec(process.execPath, ['scripts/nats-setup.mjs', '--bad']), error => /Invalid setup options/.test(error.stderr));
});
