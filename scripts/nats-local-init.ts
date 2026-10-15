// Explicit local-only initializer. Production setup NEVER creates the input stream.
import { connect } from '@nats-io/transport-node';
import { jetstreamManager, StorageType, RetentionPolicy } from '@nats-io/jetstream';
import { provision } from '../workers/inquiry-to-commerce/src/provision.ts';

const nc = await connect({ servers: 'nats://localhost:4222', user: 'fionas-admin', pass: 'fionas-admin-dev' });
try {
  const manager = await jetstreamManager(nc);
  try { await manager.streams.info('FIONAS_INQUIRIES'); }
  catch (error) {
    if (!(error instanceof Error) || error.name !== 'StreamNotFoundError') throw error;
    await manager.streams.add({ name: 'FIONAS_INQUIRIES', subjects: ['fionas.inquiries.>'],
      storage: StorageType.File, retention: RetentionPolicy.Limits, max_age: 30 * 86_400_000_000_000,
      duplicate_window: 86_400_000_000_000, deny_delete: true });
  }
  await provision(manager);
  console.log('Local development streams/consumer ready.');
} finally { await nc.close(); }
