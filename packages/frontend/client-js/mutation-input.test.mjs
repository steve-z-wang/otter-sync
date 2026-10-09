import test from 'node:test';
import assert from 'node:assert/strict';
import { submitMutation } from './api/local.mts';

test('callback supplies encoded input after its owned local commands finish', async () => {
  let observed;
  const host = {
    admit: () => undefined,
    track: run => run('opaque-scope'),
    running: () => {},
    expired: () => Error('transaction_closed'),
    mutations: { async submit(command, scope, decode, local) {
      assert.equal(scope, 'opaque-scope');
      assert.deepEqual(command, { kind: 'submitMutation', name: 'Publish', version: 1, local: true });
      const input = await local(async command => { observed = command; });
      assert.deepEqual(input, { call: 'legal field' });
      return { status: 'pending', wait: async () => { throw Error('not terminal'); } };
    } },
  };
  await submitMutation(host, 'Publish', 1, async tx => {
    await tx.direct({ model: 'Draft', kind: 'delete', identity: { id: 'd' } });
    return { call: 'legal field' };
  }, value => value);
  assert.equal(observed.kind, 'direct');
});
