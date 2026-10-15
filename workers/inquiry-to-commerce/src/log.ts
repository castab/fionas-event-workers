export type Log = (level: 'info' | 'warn' | 'error', event: string, fields?: Record<string, unknown>) => void;
export const log: Log = (level, event, fields = {}) => {
  console[level === 'info' ? 'log' : level](JSON.stringify({ time: new Date().toISOString(), level, event, ...fields }));
};
