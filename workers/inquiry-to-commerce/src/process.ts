import { persistedEvent, OUTPUT_STREAM, OUTPUT_SUBJECT } from '../../../packages/events/src/index.ts';
import type { InquiryReceipt } from '../../../packages/events/src/index.ts';
import type { CommerceRequest } from './transform.ts';
import { commerceRequest, parseSubmitted } from './transform.ts';
import { failureFields, ProcessingError } from './errors.ts';
import type { Log } from './log.ts';

export const MAX_DELIVER = 10;
export const RETRY_DELAYS_MS = [10_000, 30_000, 120_000, 300_000, 900_000];
export interface Delivery {
  data: Uint8Array;
  info: { deliveryCount: number; streamSequence: number };
  ackAck(options?: { timeout: number }): Promise<boolean>;
  nak(delay: number): void;
}
export interface Dependencies {
  commerce: { createInquiry(id: string, body: CommerceRequest, signal: AbortSignal): Promise<InquiryReceipt> };
  publish(subject: string, bytes: Uint8Array, options: { msgID: string; expect: { streamName: string }; timeout: number }): Promise<{ stream: string; seq: number; duplicate: boolean }>;
  log: Log;
}

export async function processDelivery(message: Delivery, deps: Dependencies, signal: AbortSignal): Promise<void> {
  let eventId: string | undefined;
  const identifiers = () => ({ eventId, streamSequence: message.info.streamSequence, attempt: message.info.deliveryCount });
  try {
    signal.throwIfAborted();
    const event = parseSubmitted(message.data);
    eventId = event.id;
    deps.log('info', 'inquiry_received', identifiers());
    const receipt = await deps.commerce.createInquiry(event.id, commerceRequest(event), signal);
    deps.log('info', 'commerce_persisted', { ...identifiers(), inquiryId: receipt.id });
    signal.throwIfAborted();
    const outcome = persistedEvent(event, receipt);
    let pubAck;
    try {
      pubAck = await deps.publish(OUTPUT_SUBJECT, new TextEncoder().encode(JSON.stringify(outcome)), {
        msgID: outcome.id, expect: { streamName: OUTPUT_STREAM }, timeout: 5_000,
      });
    } catch { throw new ProcessingError('outcome_publish_failed', false); }
    if (pubAck.stream !== OUTPUT_STREAM || !Number.isSafeInteger(pubAck.seq) || pubAck.seq < 1) {
      throw new ProcessingError('invalid_outcome_puback', false);
    }
    deps.log('info', 'outcome_published', { ...identifiers(), outcomeId: outcome.id, duplicate: pubAck.duplicate });
    signal.throwIfAborted();
    let acknowledged = false;
    try { acknowledged = await message.ackAck({ timeout: 5_000 }); }
    catch { throw new ProcessingError('input_ack_unconfirmed', false); }
    if (!acknowledged) throw new ProcessingError('input_ack_unconfirmed', false);
    deps.log('info', 'inquiry_acknowledged', identifiers());
  } catch (error) {
    const fields = failureFields(error);
    const exhausted = message.info.deliveryCount >= MAX_DELIVER;
    deps.log('error', fields.permanent || exhausted ? 'operator_attention_required' : 'delivery_failed', { ...identifiers(), ...fields, exhausted });
    // No ACK or TERM for failures: the limits-retained source remains inspectable.
    if (!signal.aborted && !exhausted) {
      const delay = fields.permanent ? 3_600_000 : RETRY_DELAYS_MS[Math.min(Math.max(message.info.deliveryCount - 1, 0), RETRY_DELAYS_MS.length - 1)]!;
      message.nak(delay);
      deps.log('warn', 'delivery_retry_scheduled', { ...identifiers(), delayMs: delay });
    }
  }
}
