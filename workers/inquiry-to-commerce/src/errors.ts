export class ProcessingError extends Error {
  readonly code: string;
  readonly permanent: boolean;
  readonly status: number | undefined;
  constructor(code: string, permanent: boolean, status?: number) {
    // Static codes only: upstream response bodies and connection URLs may contain secrets/PII.
    super(code);
    this.code = code;
    this.permanent = permanent;
    this.status = status;
  }
}
export function failureFields(error: unknown): Record<string, unknown> {
  return error instanceof ProcessingError
    ? { code: error.code, permanent: error.permanent, status: error.status }
    : { code: 'unexpected_failure', permanent: false };
}
