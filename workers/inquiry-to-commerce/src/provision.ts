import { AckPolicy, DeliverPolicy, DiscardPolicy, ReplayPolicy, RetentionPolicy, StorageType } from '@nats-io/jetstream';
import type { JetStreamManager, StreamConfig, ConsumerConfig } from '@nats-io/jetstream';
import { CONSUMER, INPUT_STREAM, INPUT_SUBJECT, OUTPUT_STREAM } from '../../../packages/events/src/index.ts';
import { MAX_DELIVER } from './process.ts';

export const ACK_WAIT_NS = 90_000_000_000;
export function outcomeStreamConfig(retentionDays = 30, replicas = 1): Partial<StreamConfig> & { name: string } {
  if (!Number.isInteger(retentionDays) || retentionDays < 1 || !Number.isSafeInteger(retentionDays * 86_400_000_000_000)) {
    throw new Error('Retention must be a finite positive whole number of days');
  }
  if (!Number.isInteger(replicas) || replicas < 1 || replicas > 5) throw new Error('Replicas must be 1 to 5');
  return {
    name: OUTPUT_STREAM, subjects: ['fionas.events.>'], storage: StorageType.File,
    retention: RetentionPolicy.Limits, discard: DiscardPolicy.Old,
    max_age: retentionDays * 86_400_000_000_000, duplicate_window: 86_400_000_000_000,
    num_replicas: replicas, deny_delete: true, deny_purge: true,
    max_msgs: -1, max_bytes: -1, max_msgs_per_subject: -1, max_msg_size: 1_048_576,
    allow_rollup_hdrs: false,
  };
}
export function inputConsumerConfig(): Partial<ConsumerConfig> {
  return {
    durable_name: CONSUMER, name: CONSUMER, filter_subject: INPUT_SUBJECT,
    ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
    ack_wait: ACK_WAIT_NS, max_deliver: MAX_DELIVER, max_ack_pending: 1,
    max_waiting: 2, max_batch: 1, max_expires: 5_000_000_000,
    // Timeout/crash redelivery uses ack_wait; deliberate failures use delayed NAK.
  };
}
export function differences(existing: object, desired: object): string[] {
  const actual = existing as Record<string, unknown>;
  return Object.entries(desired).flatMap(([key, value]) =>
    JSON.stringify(actual[key]) === JSON.stringify(value) ? [] : [key]);
}
export class ProvisioningMismatch extends Error {
  readonly details: Array<{ property: string; existing: unknown; desired: unknown }>;
  constructor(resource: string, existing: object, desired: object, keys: string[]) {
    super(`${resource} configuration mismatch: ${keys.join(', ')}. Nothing updated.`);
    const actual = existing as Record<string, unknown>;
    const expected = desired as Record<string, unknown>;
    this.details = keys.map(property => ({ property, existing: actual[property] ?? null, desired: expected[property] ?? null }));
  }
}
export function verifyInputConsumer(config: ConsumerConfig): void {
  const mismatches = differences(config, inputConsumerConfig());
  if (config.deliver_subject) mismatches.push('deliver_subject');
  if (config.filter_subjects?.length) mismatches.push('filter_subjects');
  if (config.backoff?.length) mismatches.push('backoff');
  if (config.inactive_threshold) mismatches.push('inactive_threshold');
  if (mismatches.length) throw new ProvisioningMismatch('Input consumer', config, inputConsumerConfig(), mismatches);
}

export async function provision(manager: JetStreamManager, retentionDays = 30, replicas = 1): Promise<{ stream: 'created' | 'verified'; consumer: 'created' | 'verified' }> {
  const desiredStream = outcomeStreamConfig(retentionDays, replicas);
  // Verify existing input only; never create, update or replace the producer's stream.
  const input = await manager.streams.info(INPUT_STREAM);
  if (input.config.storage !== StorageType.File || input.config.retention !== RetentionPolicy.Limits ||
    JSON.stringify(input.config.subjects) !== JSON.stringify(['fionas.inquiries.>'])) {
    throw new Error('FIONAS_INQUIRIES must already exist with file storage, limits retention and fionas.inquiries.>');
  }
  let existingStream;
  try { existingStream = await manager.streams.info(OUTPUT_STREAM); }
  catch (error) {
    if (!(error instanceof Error) || error.name !== 'StreamNotFoundError') throw error;
  }
  if (existingStream) {
    const diff = differences(existingStream.config, desiredStream);
    if (diff.length) throw new ProvisioningMismatch(OUTPUT_STREAM, existingStream.config, desiredStream, diff);
  }
  let existingConsumer;
  try { existingConsumer = await manager.consumers.info(INPUT_STREAM, CONSUMER); }
  catch (error) {
    if (!(error instanceof Error) || error.name !== 'ConsumerNotFoundError') throw error;
  }
  if (existingConsumer) verifyInputConsumer(existingConsumer.config);
  // Preflight all existing resources before creating anything; never update a resource.
  if (!existingStream) await manager.streams.add(desiredStream);
  if (!existingConsumer) await manager.consumers.add(INPUT_STREAM, inputConsumerConfig());
  return { stream: existingStream ? 'verified' : 'created', consumer: existingConsumer ? 'verified' : 'created' };
}
