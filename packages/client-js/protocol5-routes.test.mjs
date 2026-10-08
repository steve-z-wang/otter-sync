import test from "node:test";
import assert from "node:assert/strict";
import { httpTransport } from "./transport.mts";
import { createServerConnection as reactNativeConnection } from "../client-react-native/live.mts";
for (const [name, make] of [
  ["JS", (options) => httpTransport(options)],
  ["React Native", (options) => reactNativeConnection(options).push],
]) {
  test(`${name} forwards protocol5 Handshake and Materialize routes unchanged`, async () => {
    const seen = [];
    const original = globalThis.fetch;
    globalThis.fetch = async (url, init) => {
      seen.push([url, init.body, init.headers.authorization]);
      return new Response("native answer");
    };
    try {
      const transport = make({ url: "http://server", token: "credential" });
      for (const route of ["handshake", "materialize"])
        assert.equal(await transport(route, "frozen"), "native answer");
      assert.deepEqual(seen, [
        ["http://server/sync/handshake", "frozen", "Bearer credential"],
        ["http://server/sync/materialize", "frozen", "Bearer credential"],
      ]);
    } finally {
      globalThis.fetch = original;
    }
  });
}
