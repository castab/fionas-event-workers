import type { InquirySubmitted, InquiryData, PricedLine, RequestedService } from '../../../packages/events/src/index.ts';
import { ProcessingError } from './errors.ts';

export type CommerceRequest = Omit<InquiryData, 'priceRevision'>;
export const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);
export const isUuid = (value: unknown): value is string => typeof value === 'string' &&
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
export const isTimestamp = (value: unknown): value is string => typeof value === 'string' &&
  /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?Z$/.test(value) &&
  Number.isFinite(Date.parse(value)) && validDate(value.slice(0, 10));
function validDate(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) && !value.startsWith('0000') &&
    Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value;
}
function requireShape(condition: unknown): asserts condition {
  if (!condition) throw new ProcessingError('invalid_inquiry_submitted', true);
}
function requiredText(object: Record<string, unknown>, key: string): string {
  const value = object[key];
  requireShape(typeof value === 'string' && value.length > 0);
  return value;
}
function optionalText(object: Record<string, unknown>, key: string): string | undefined {
  const value = object[key];
  requireShape(value === undefined || typeof value === 'string');
  return value as string | undefined;
}

export function parseSubmitted(bytes: Uint8Array): InquirySubmitted {
  let event: unknown;
  try { event = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { throw new ProcessingError('invalid_event_json', true); }
  requireShape(isObject(event));
  if (event.schemaVersion !== 1 || event.type !== 'fionas.inquiry.submitted') {
    throw new ProcessingError('unsupported_event_type_or_version', true);
  }
  requireShape(isUuid(event.id) && isTimestamp(event.occurredAt) && event.source === 'fionas-web');
  const data = event.data;
  requireShape(isObject(data));
  const service = data.requestedService;
  requireShape(isObject(service) && Number.isInteger(service.guestCount));
  requireShape(service.guestCountIsMinimum === undefined || typeof service.guestCountIsMinimum === 'boolean');
  const requestedService: RequestedService = { guestCount: service.guestCount as number };
  if (service.guestCountIsMinimum !== undefined) requestedService.guestCountIsMinimum = service.guestCountIsMinimum as boolean;
  const pricingReference = optionalText(service, 'pricingReference');
  if (pricingReference !== undefined) requestedService.pricingReference = pricingReference;
  if (service.items !== undefined) {
    requireShape(Array.isArray(service.items));
    requestedService.items = service.items.map((item: unknown) => {
      requireShape(isObject(item));
      const label = requiredText(item, 'label');
      const group = optionalText(item, 'group');
      const key = optionalText(item, 'key');
      return { label, ...(group !== undefined ? { group } : {}), ...(key !== undefined ? { key } : {}) };
    });
  }
  requireShape(Array.isArray(data.lines) && data.lines.length > 0);
  const lines: PricedLine[] = data.lines.map((line: unknown) => {
    requireShape(isObject(line));
    const description = requiredText(line, 'description');
    const subDescription = optionalText(line, 'subDescription');
    const quantity = optionalText(line, 'quantity');
    // Preserve strings; exact-decimal and financial business validation belong to commerce.
    return {
      description,
      ...(subDescription !== undefined ? { subDescription } : {}),
      ...(quantity !== undefined ? { quantity } : {}),
      unitPrice: requiredText(line, 'unitPrice'),
      taxAmount: requiredText(line, 'taxAmount'),
      currency: requiredText(line, 'currency'),
    };
  });
  const eventDate = requiredText(data, 'eventDate');
  requireShape(validDate(eventDate));
  const message = optionalText(data, 'message');
  return {
    schemaVersion: 1, type: 'fionas.inquiry.submitted', id: event.id,
    occurredAt: event.occurredAt, source: 'fionas-web',
    data: {
      name: requiredText(data, 'name'), email: requiredText(data, 'email'),
      zipCode: requiredText(data, 'zipCode'), eventDate, eventType: requiredText(data, 'eventType'),
      ...(message !== undefined ? { message } : {}), requestedService,
      priceRevision: requiredText(data, 'priceRevision'), lines,
    },
  };
}

export function commerceRequest(event: InquirySubmitted): CommerceRequest {
  const { priceRevision: _revision, ...request } = event.data;
  return request;
}
