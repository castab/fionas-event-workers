export class ProcessingError extends Error {
  readonly code: string;
  readonly kind: 'permanent' | 'transient' | 'systemic';
  readonly status: number | undefined;
  constructor(code: string, kind: ProcessingError['kind'], status?: number) {
    // Static codes only: upstream response bodies and connection URLs may contain secrets/PII.
    super(code);
    this.code = code;
    this.kind = kind;
    this.status = status;
  }
}
export function failureFields(error: unknown): Record<string, unknown> {
  return error instanceof ProcessingError
    ? { code: error.code, kind: error.kind, status: error.status }
    : { code: 'unexpected_failure', kind: 'transient' };
}
