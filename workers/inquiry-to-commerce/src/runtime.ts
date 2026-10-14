import type { Consumer, JsMsg } from '@nats-io/jetstream';
import { setTimeout as delay } from 'node:timers/promises';
import type { Dependencies } from './process.ts';
import { processDelivery } from './process.ts';
import { failureFields } from './errors.ts';

export const SHUTDOWN_GRACE_MS = 55_000;
type PullConsumer = Pick<Consumer, 'next'>;

// A signal stops pulls immediately. The current operation gets a bounded completion period.
export class Shutdown {
  readonly operation = new AbortController();
  readonly stopping = new AbortController();
  private deadline: ReturnType<typeof setTimeout> | undefined;
  private readonly onDeadline: () => void;
  private readonly graceMs: number;
  constructor(onDeadline: () => void, graceMs = SHUTDOWN_GRACE_MS) { this.onDeadline = onDeadline; this.graceMs = graceMs; }
  stop(): void {
    if (this.stopping.signal.aborted) return;
    this.stopping.abort();
    this.deadline = setTimeout(() => { this.operation.abort(); this.onDeadline(); }, this.graceMs);
    this.deadline.unref();
  }
  dispose(): void { clearTimeout(this.deadline); }
}

export async function runLoop(consumer: PullConsumer, deps: Dependencies, shutdown: Shutdown): Promise<void> {
  while (!shutdown.stopping.signal.aborted) {
    let message: JsMsg | null;
    try { message = await consumer.next({ expires: 2_000 }); }
    catch (error) {
      if (shutdown.stopping.signal.aborted) break;
      deps.log('warn', 'nats_pull_failed', failureFields(error));
      await delay(2_000, undefined, { signal: shutdown.stopping.signal }).catch(() => {});
      continue;
    }
    if (shutdown.stopping.signal.aborted) break; // Any fetched delivery remains unacknowledged.
    if (message) await processDelivery(message, deps, shutdown.operation.signal);
  }
}
