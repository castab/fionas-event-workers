import type { InquiryReceipt } from '../../../packages/events/src/index.ts';
import type { CommerceRequest } from './transform.ts';
import { isObject, isTimestamp, isUuid } from './transform.ts';
import { ProcessingError } from './errors.ts';

export const HTTP_TIMEOUT_MS = 10_000;
export interface CommerceOptions {
  baseUrl: string;
  serviceId: string;
  secret: string;
  timeoutMs?: number;
  now?: () => number;
}

// Native fetch; redirects are refused so authorization and idempotency cannot move hosts.
export class CommerceClient {
  private token: { value: string; refreshAt: number } | undefined;
  private readonly now: () => number;
  private readonly options: CommerceOptions;
  constructor(options: CommerceOptions) { this.options = options; this.now = options.now ?? Date.now; }

  private async request(path: string, body: string, headers: Record<string, string>, signal: AbortSignal): Promise<{ status: number; json: unknown }> {
    const bounded = AbortSignal.any([signal, AbortSignal.timeout(this.options.timeoutMs ?? HTTP_TIMEOUT_MS)]);
    try {
      const response = await fetch(new URL(path, this.options.baseUrl), {
        method: 'POST', headers: { 'Content-Type': 'application/json', ...headers },
        body, redirect: 'error', signal: bounded,
      });
      // The timeout includes consumption of the response, not just response headers.
      if (response.status !== 200 && response.status !== 201) {
        await response.body?.cancel();
        return { status: response.status, json: undefined };
      }
      let json: unknown;
      try { json = await response.json(); }
      catch {
        if (bounded.aborted) throw new ProcessingError(signal.aborted ? 'shutdown_aborted' : 'http_timeout', false);
        throw new ProcessingError(path === '/inquiries' ? 'invalid_inquiry_receipt' : 'invalid_token_response', true);
      }
      return { status: response.status, json };
    } catch (error) {
      if (error instanceof ProcessingError) throw error;
      throw new ProcessingError(signal.aborted ? 'shutdown_aborted' : bounded.aborted ? 'http_timeout' : 'http_connection_failure', false);
    }
  }

  private httpError(status: number, auth: boolean): ProcessingError {
    const transient = status === 429 || status >= 500;
    return new ProcessingError(auth ? 'service_auth_rejected' : status === 403 ? 'commerce_permission_denied' : 'commerce_request_rejected', !transient, status);
  }

  private async accessToken(signal: AbortSignal): Promise<string> {
    if (this.token && this.now() < this.token.refreshAt) return this.token.value;
    this.token = undefined;
    const result = await this.request('/auth/service/token', JSON.stringify({ serviceId: this.options.serviceId, secret: this.options.secret }), {}, signal);
    if (result.status !== 200) throw this.httpError(result.status, true);
    const dto = result.json;
    if (!isObject(dto) || typeof dto.accessToken !== 'string' || !dto.accessToken || /\s/.test(dto.accessToken) ||
      dto.tokenType !== 'Bearer' || !isTimestamp(dto.expiresAt) || typeof dto.expiresIn !== 'number' || !Number.isSafeInteger(dto.expiresIn) || dto.expiresIn <= 0) {
      throw new ProcessingError('invalid_token_response', true);
    }
    const expiresAt = Date.parse(dto.expiresAt);
    if (expiresAt <= this.now()) throw new ProcessingError('expired_token_response', true);
    const lifetime = expiresAt - this.now();
    this.token = { value: dto.accessToken, refreshAt: expiresAt - Math.min(30_000, lifetime / 10) };
    return this.token.value;
  }

  async createInquiry(eventId: string, request: CommerceRequest, signal: AbortSignal): Promise<InquiryReceipt> {
    const body = JSON.stringify(request);
    for (let attempt = 0; attempt < 2; attempt++) {
      const token = await this.accessToken(signal);
      const result = await this.request('/inquiries', body, { Authorization: `Bearer ${token}`, 'Idempotency-Key': eventId }, signal);
      if (result.status === 401) {
        this.token = undefined;
        if (attempt === 0) continue;
      }
      if (result.status !== 201) throw this.httpError(result.status, false);
      const receipt = result.json;
      if (!isObject(receipt) || !isUuid(receipt.id) || !isTimestamp(receipt.createdAt)) {
        throw new ProcessingError('invalid_inquiry_receipt', true);
      }
      return { id: receipt.id, createdAt: receipt.createdAt };
    }
    throw new ProcessingError('service_auth_rejected', true, 401);
  }
}
