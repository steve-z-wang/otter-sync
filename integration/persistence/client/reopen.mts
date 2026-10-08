import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
const { GeneratedClient } = await import(
  pathToFileURL(`${process.argv[2]}/client.ts`).href
);
const path = process.argv[3];
const connection = {
  url: "http://127.0.0.1:1",
  token: "offline",
};
for (let run = 0; run < 2; run++) {
  await assert.rejects(
    GeneratedClient.open({ path, stream: "User:viewer", connection }),
    /unsupported Store format/,
  );
}
let first;
for (let run = 0; run < 2; run++) {
  const c = await GeneratedClient.open({
    path: `${path}.bound`,
    stream: "User:viewer",
    connection,
  });
  await c.client.connection?.pause();
  try {
    if (run === 0) {
      await c.models.todo.create({
        id: "live",
        title: "persisted",
        channel: "opaque Channel",
      });
      await c.transaction((tx) =>
        tx.models.todo.update({ id: "live" }, { title: "local edited" }),
      );
      await c.mutations.edit({
        todo: { id: "live", channel: "pending Channel" },
      });
    }
    assert.equal(
      (await c.models.todo.get({ id: "live" })).title,
      "local edited",
    );
    assert.equal(
      (await c.models.todo.get({ id: "live" })).channel,
      "pending Channel",
    );
    assert.equal((await c.syncState()).pending, 1);
    const saved = await c.client.readSql(
      "SELECT q.*, d.descriptor AS retained_descriptor, o.* FROM axton_mutation_queue q JOIN axton_descriptor d ON d.context=q.descriptor JOIN axton_mutation_queue_operation o ON o.mutation_id=q.id ORDER BY q.id,o.step",
      [],
    );
    assert.equal(saved.length, 1);
    if (run === 0) first = saved;
    else assert.deepEqual(saved, first);
  } finally {
    await c.close();
  }
}
console.log(
  "generated JS: legacy adoption refused twice; bound local CRUD and exact queued intent survive reopen",
);
