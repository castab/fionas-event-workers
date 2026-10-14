import { readFile } from 'node:fs/promises';
import { connect } from '@nats-io/transport-node';
import { jetstream } from '@nats-io/jetstream';
import { CONSUMER, INPUT_STREAM } from '../../../packages/events/src/index.ts';
import { connectionOptions, workerConfig } from './config.ts';
import { CommerceClient } from './commerce.ts';
import { verifyInputConsumer } from './provision.ts';
import { runLoop, Shutdown } from './runtime.ts';
import { log } from './log.ts';

async function main(): Promise<void> {
  const config = workerConfig();
  const secret = config.commerce.secret ?? (await readFile(config.commerce.secretFile!, 'utf8')).trim();
  if (!secret.trim()) throw new Error('Commerce service secret is empty');
  const nc = await connect(await connectionOptions(config.nats, CONSUMER));
  const shutdown = new Shutdown(() => { void nc.close(); });
  const stop = () => { log('info', 'shutdown_requested'); shutdown.stop(); };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  const statusTask = (async () => {
    for await (const status of nc.status()) log('info', 'nats_connection_status', { kind: status.type });
  })();
  const closedTask = nc.closed().then((error) => {
    if (!shutdown.stopping.signal.aborted) {
      log('error', 'nats_connection_closed', { code: error ? 'connection_failure' : 'unexpected_close' });
      process.exitCode = 1;
      shutdown.stop();
      shutdown.operation.abort();
    }
  });
  try {
    log('info', 'nats_connected');
    const js = jetstream(nc, { timeout: 5_000 });
    // Only named consumer INFO/NEXT access; no stream manager or consumer creation.
    const consumer = await js.consumers.get(INPUT_STREAM, CONSUMER);
    verifyInputConsumer((await consumer.info(true)).config);
    log('info', 'durable_consumer_available', { stream: INPUT_STREAM, consumer: CONSUMER });
    const commerce = new CommerceClient({ baseUrl: config.commerce.baseUrl, serviceId: config.commerce.serviceId, secret });
    await runLoop(consumer, { commerce, publish: (subject, bytes, options) => js.publish(subject, bytes, options), log }, shutdown);
  } finally {
    await nc.close();
    shutdown.dispose();
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
    await Promise.all([statusTask, closedTask]);
    log('info', 'worker_stopped');
  }
}

main().catch((error: unknown) => {
  // Configuration errors are locally authored; upstream/network errors are never printed raw.
  const isConfig = error instanceof Error && /^(Missing |Invalid NATS_URL|NATS_|Choose NATS_|COMMERCE_|Supply exactly|Commerce service secret|Input consumer configuration mismatch)/.test(error.message);
  log('error', 'startup_failed', { code: isConfig ? error.message : 'check_credentials_connectivity_and_provisioned_consumer' });
  process.exitCode = 1;
});
