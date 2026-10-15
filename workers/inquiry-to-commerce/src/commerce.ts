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
        body, redirect: 'manual', signal: bounded,
      });
      // The timeout includes consumption of the response, not just response headers.
      if (response.status !== 200 && response.status !== 201 && response.status !== 409) {
        await response.body?.cancel();
        return { status: response.status, json: undefined };
      }
      let json: unknown;
      try { json = await response.json(); }
      catch {
        if (bounded.aborted) throw new ProcessingError(signal.aborted ? 'shutdown_aborted' : 'http_timeout', 'transient');
        if (response.status === 409) return { status: 409, json: undefined };
        throw new ProcessingError(path === '/inquiries' ? 'invalid_inquiry_receipt' : 'invalid_token_response', 'systemic');
      }
      // Only known, contract-owned conflict codes leave this boundary. Never log response bodies.
      if (response.status === 409) json = isObject(json) && typeof json.code === 'string' && ['conflict', 'IDEMPOTENCY_KEY_REUSED'].includes(json.code) ? { code: json.code } : undefined;
      return { status: response.status, json };
    } catch (error) {
      if (error instanceof ProcessingError) throw error;
      throw new ProcessingError(signal.aborted ? 'shutdown_aborted' : bounded.aborted ? 'http_timeout' : 'http_connection_failure', 'transient');
    }
  }

  private httpError(status: number, auth: boolean, json?: unknown): ProcessingError {
    if (status === 408 || status === 429 || status >= 500) return new ProcessingError(auth ? 'service_auth_unavailable' : 'commerce_unavailable', 'transient', status);
    if (auth) return new ProcessingError('service_auth_rejected', 'systemic', status);
    if (status === 401 || status === 403) return new ProcessingError('commerce_permission_denied', 'systemic', status);
    if (status === 400 || status === 422) return new ProcessingError('commerce_input_rejected', 'permanent', status);
    if (status === 409 && isObject(json)) {
      if (json.code === 'conflict') return new ProcessingError('commerce_concurrent_conflict', 'transient', status);
      if (json.code === 'IDEMPOTENCY_KEY_REUSED') return new ProcessingError('idempotency_key_reused', 'permanent', status);
    }
    return new ProcessingError('commerce_unexpected_response', 'systemic', status);
  }

  private async accessToken(signal: AbortSignal): Promise<string> {
    if (this.token && this.now() < this.token.refreshAt) return this.token.value;
    this.token = undefined;
    const result = await this.request('/auth/service/token', JSON.stringify({ serviceId: this.options.serviceId, secret: this.options.secret }), {}, signal);
    if (result.status !== 200) throw this.httpError(result.status, true);
    const dto = result.json;
    if (!isObject(dto) || typeof dto.accessToken !== 'string' || !dto.accessToken || /\s/.test(dto.accessToken) ||
      dto.tokenType !== 'Bearer' || !isTimestamp(dto.expiresAt) || typeof dto.expiresIn !== 'number' || !Number.isSafeInteger(dto.expiresIn) || dto.expiresIn <= 0) {
      throw new ProcessingError('invalid_token_response', 'systemic');
    }
    const expiresAt = Date.parse(dto.expiresAt);
    if (expiresAt <= this.now()) throw new ProcessingError('expired_token_response', 'systemic');
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
      if (result.status !== 201) throw this.httpError(result.status, false, result.json);
      const receipt = result.json;
      if (!isObject(receipt) || !isUuid(receipt.id) || !isTimestamp(receipt.createdAt)) {
        throw new ProcessingError('invalid_inquiry_receipt', 'systemic');
      }
      return { id: receipt.id, createdAt: receipt.createdAt };
    }
    throw new ProcessingError('service_auth_rejected', 'systemic', 401);
  }
}
