import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { TestContext } from 'node:test';
export interface Call { path: string; headers: IncomingMessage['headers']; body: string }
export async function httpStub(t: TestContext, respond: (call: Call, res: ServerResponse) => void) {
  const calls: Call[] = [];
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    const call = { path: req.url!, headers: req.headers, body: Buffer.concat(chunks).toString() };
    calls.push(call);
    respond(call, res);
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('No stub address');
  return { calls, url: `http://127.0.0.1:${address.port}` };
}
export function json(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body));
}
export function token(index = 1, expiresAt = Date.now() + 900_000) {
  return { accessToken: `test-token-${index}`, tokenType: 'Bearer', expiresAt: new Date(expiresAt).toISOString(), expiresIn: 900 };
}
