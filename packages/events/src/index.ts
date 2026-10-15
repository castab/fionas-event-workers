export const INPUT_STREAM = 'FIONAS_INQUIRIES';
export const INPUT_SUBJECT = 'fionas.inquiries.submitted.v1';
export const CONSUMER = 'inquiry-to-commerce';
export const OUTPUT_STREAM = 'FIONAS_EVENTS';
export const OUTPUT_SUBJECT = 'fionas.events.inquiry.persisted.v1';
export const FAILURE_SUBJECT = 'fionas.events.inquiry.processing-failed.v1';

// This is a narrow transport view, not a copy of the producer's complete schema.
// Authority: castab/fionas-web/asyncapi.yaml.
export interface RequestedItem { label: string; group?: string; key?: string }
export interface RequestedService {
  guestCount: number;
  guestCountIsMinimum?: boolean;
  items?: RequestedItem[];
  pricingReference?: string;
}
export interface PricedLine {
  description: string;
  subDescription?: string;
  quantity?: string;
  unitPrice: string;
  taxAmount: string;
  currency: string;
}
export interface InquiryData {
  name: string;
  email: string;
  zipCode: string;
  eventDate: string;
  eventType: string;
  message?: string;
  requestedService: RequestedService;
  priceRevision: string;
  lines: PricedLine[];
}
export interface InquirySubmitted {
  schemaVersion: 1;
  type: 'fionas.inquiry.submitted';
  id: string;
  occurredAt: string;
  source: 'fionas-web';
  data: InquiryData;
}
export interface InquiryReceipt { id: string; createdAt: string }
export interface InquiryProcessingFailed {
  schemaVersion: 1;
  type: 'fionas.inquiry.processing-failed';
  id: string;
  correlationId?: string;
  occurredAt: string;
  source: 'fionas-event-workers';
  data: { inputStream: 'FIONAS_INQUIRIES'; inputSequence: number; errorCode: string };
}

export function processingFailedEvent(sequence: number, storedAt: string, code: string, eventId?: string): InquiryProcessingFailed {
  return {
    schemaVersion: 1, type: 'fionas.inquiry.processing-failed',
    id: `fionas.inquiry.processing-failed.v1:${INPUT_STREAM}:${sequence}`,
    ...(eventId ? { correlationId: eventId } : {}),
    occurredAt: storedAt, source: 'fionas-event-workers',
    data: { inputStream: INPUT_STREAM, inputSequence: sequence, errorCode: code },
  };
}
export interface InquiryPersisted {
  schemaVersion: 1;
  type: 'fionas.inquiry.persisted';
  id: string;
  correlationId: string;
  occurredAt: string;
  source: 'fionas-event-workers';
  data: { inquiryId: string; customer: { name: string; email: string }; eventDate: string };
}

export function persistedEvent(input: InquirySubmitted, receipt: InquiryReceipt): InquiryPersisted {
  return {
    schemaVersion: 1,
    type: 'fionas.inquiry.persisted',
    id: `fionas.inquiry.persisted.v1:${input.id}`,
    correlationId: input.id,
    occurredAt: receipt.createdAt,
    source: 'fionas-event-workers',
    data: {
      inquiryId: receipt.id,
      customer: { name: input.data.name, email: input.data.email },
      eventDate: input.data.eventDate,
    },
  };
}
