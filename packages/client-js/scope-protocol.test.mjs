import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WebSocketServer } from 'ws';
import { Client } from './index.mts';

async function until(predicate) {
  const end = Date.now() + 5000;
  while (!await predicate()) {
    if (Date.now() > end) throw Error('protocol test timed out');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

test('runtime negotiates on live and pull; identity-only removal reaches the native store', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'axton-sdk-scope-'));
  const schema = JSON.parse(await readFile(new URL('../../fixtures/schemas/entry.json', import.meta.url), 'utf8'));
  let stored = 0;
  const client = await Client.open({ path: join(dir, 'db'), schema, onStore: { Entry: () => { stored++; } } });
  const envelopes = [];
  const errors = [];
  const server = createServer(async (request, response) => {
    let text = '';
    for await (const chunk of request) text += chunk;
    const body = JSON.parse(text);
    envelopes.push(body);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ cursors: Object.fromEntries(Object.entries(body.cursors).map(([c, v]) => [c, { from: v, to: 1, head: 1 }])), changes: [{ kind: 'upsert', stream: 'scope', cursor: 1, model: 'Entry', identity: { id: 'e' }, stamp: 1, state: { text: 'held', note: null } }] }));
  });
  const ws = new WebSocketServer({ server });
  let socket;
  ws.on('connection', current => {
    socket = current;
    current.on('message', text => {
      const body = JSON.parse(text.toString());
      envelopes.push(body);
      current.send(JSON.stringify({ type: 'subscribed', cursors: { scope: envelopes.filter(e => e.type === 'subscribe').length === 1 ? 0 : 1 } }));
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    await client.subscribe('scope');
    const connection = await client.connect({ url: `http://127.0.0.1:${server.address().port}`, token: 'secret' }, { onError: error => errors.push(error) });
    await until(() => envelopes.some(e => e.type === 'subscribe'));
    await connection.pause();
    await connection.resume();
    const send = (from, change) => socket.send(JSON.stringify({ cursors: { scope: { from, to: from + 1, head: from + 1 } }, changes: [change] }));
    await until(async () => (await client.read('Entry', { id: 'e' }))?.text === 'held');
    send(1, { kind: 'remove', stream: 'scope', cursor: 2, model: 'Entry', identity: { id: 'e' } });
    await until(async () => (await client.syncState()).cursors.scope === 2);
    assert.equal(await client.read('Entry', { id: 'e' }), null);
    assert.equal(stored, 1, 'identity-only removal releases cache without an authority onStore hook');
    assert.ok(envelopes.some(e => e.cursors?.scope === 0), 'runtime HTTP catch-up was observed');
    for (const envelope of envelopes) assert.ok(envelope.capabilities.includes('stream-membership-v1'));
    assert.deepEqual(errors, []);
    await connection.close();
  } finally {
    await client.close();
    for (const current of ws.clients) current.terminate();
    await new Promise(resolve => ws.close(resolve));
    await new Promise(resolve => server.close(resolve));
    await rm(dir, { recursive: true, force: true });
  }
});
