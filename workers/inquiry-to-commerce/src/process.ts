import { processingFailedEvent, FAILURE_SUBJECT, OUTPUT_STREAM } from '../../../packages/events/src/index.ts';
import type { InquiryReceipt } from '../../../packages/events/src/index.ts';
import type { CommerceRequest } from './transform.ts';
import { commerceRequest, parseSubmitted, isObject, isUuid } from './transform.ts';
import { failureFields, ProcessingError } from './errors.ts';
import type { Log } from './log.ts';

export const MAX_DELIVER = 10;
export const RETRY_DELAYS_MS = [10_000, 30_000, 120_000, 300_000, 900_000];
export interface Delivery {
  data: Uint8Array;
  timestamp: string;
  info: { deliveryCount: number; streamSequence: number };
  ackAck(options?: { timeout: number }): Promise<boolean>;
  nak(delay: number): void;
  term(reason?: string): void;
}
export interface Dependencies {
  commerce: { createInquiry(id: string, body: CommerceRequest, signal: AbortSignal): Promise<InquiryReceipt> };
  publish(subject: string, bytes: Uint8Array, options: { msgID: string; expect: { streamName: string }; timeout: number }): Promise<{ stream: string; seq: number; duplicate: boolean }>;
  log: Log;
}

export async function processDelivery(message: Delivery, deps: Dependencies, signal: AbortSignal): Promise<'persisted' | 'failed' | 'retry' | 'interrupted'> {
  let eventId: string | undefined;
  const identifiers = () => ({ eventId, streamSequence: message.info.streamSequence, attempt: message.info.deliveryCount });
  try {
    signal.throwIfAborted();
    const event = parseSubmitted(message.data);
    eventId = event.id;
    deps.log('info', 'inquiry_received', identifiers());
    const receipt = await deps.commerce.createInquiry(event.id, commerceRequest(event), signal);
    deps.log('info', 'commerce_persisted', { ...identifiers(), inquiryId: receipt.id });
    // A valid 201 means commerce committed the inquiry, Estimate and its own success outbox event
    // atomically. This worker neither verifies nor publishes that event.
    signal.throwIfAborted();
    let acknowledged = false;
    try { acknowledged = await message.ackAck({ timeout: 5_000 }); }
    catch { throw new ProcessingError('input_ack_unconfirmed', 'transient'); }
    if (!acknowledged) throw new ProcessingError('input_ack_unconfirmed', 'transient');
    deps.log('info', 'inquiry_acknowledged', identifiers());
    return 'persisted';
  } catch (error) {
    if (signal.aborted) return 'interrupted';
    let failure = error;
    if (failure instanceof ProcessingError && failure.kind === 'permanent') {
      if (!eventId) {
        try { const value: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(message.data));
          if (isObject(value) && isUuid(value.id)) eventId = value.id;
        } catch { /* Malformed sources are still identified by stream sequence. */ }
      }
      const outcome = processingFailedEvent(message.info.streamSequence, message.timestamp, failure.code, eventId);
      deps.log('error', 'operator_attention_required', { ...identifiers(), ...failureFields(failure) });
      try {
        await publishConfirmed(FAILURE_SUBJECT, outcome, deps);
        deps.log('info', 'failure_record_published', { ...identifiers(), failureId: outcome.id });
        signal.throwIfAborted();
        // TERM is distinct from a success ACK. Lost TERM is safe: replay the same failure ID.
        message.term(failure.code);
        deps.log('warn', 'input_terminated', { ...identifiers(), failureId: outcome.id });
        return 'failed';
      } catch (dispositionError) { failure = dispositionError; }
    }
    if (signal.aborted) return 'interrupted';
    const exhausted = message.info.deliveryCount >= MAX_DELIVER;
    const fields = failureFields(failure);
    deps.log('error', fields.kind === 'systemic' || exhausted ? 'operator_attention_required' : 'delivery_failed', { ...identifiers(), ...fields, exhausted });
    if (failure instanceof ProcessingError && failure.kind === 'systemic') throw failure;
    if (exhausted) throw new ProcessingError('retry_exhausted', 'systemic');
    const delay = RETRY_DELAYS_MS[Math.min(Math.max(message.info.deliveryCount - 1, 0), RETRY_DELAYS_MS.length - 1)]!;
    message.nak(delay);
    deps.log('warn', 'delivery_retry_scheduled', { ...identifiers(), delayMs: delay });
    return 'retry';
  }
}

async function publishConfirmed(subject: string, event: { id: string }, deps: Dependencies) {
  let ack;
  try { ack = await deps.publish(subject, new TextEncoder().encode(JSON.stringify(event)), {
    msgID: event.id, expect: { streamName: OUTPUT_STREAM }, timeout: 5_000,
  }); } catch (error) {
    const denied = error instanceof Error && [error, error.cause].some(value => value instanceof Error && value.name === 'PermissionViolationError');
    throw new ProcessingError('outcome_publish_failed', denied ? 'systemic' : 'transient');
  }
  if (ack.stream !== OUTPUT_STREAM || !Number.isSafeInteger(ack.seq) || ack.seq < 1) throw new ProcessingError('invalid_outcome_puback', 'transient');
  return ack;
}
