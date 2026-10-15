import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const exec = promisify(execFile);
test('recovery CLI documents targeted replay and rejects absent/invalid sequence before side effects', async () => {
  assert.match((await exec(process.execPath, ['scripts/nats-retry-inquiry.mjs', '--help'])).stdout, /exactly one retained input/);
  for (const sequence of ['', '0', '-1', '1.5', 'Infinity', '9007199254740992']) {
    await assert.rejects(exec(process.execPath, ['scripts/nats-retry-inquiry.mjs', `--sequence=${sequence}`, '--non-interactive']), error => {
      assert.match(error.stderr, /positive whole number/); return true;
    });
  }
});
