// Test process boundary around the actual Node native actor, without an SDK engine.
import { createRequire } from "node:module";
const native = createRequire(import.meta.url)(
  "../../bindings/node/axton-node.node",
);
let runtime;
let scheduled = false;
function drain() {
  scheduled = false;
  for (const event of JSON.parse(native.runtimeDrain(runtime)))
    process.send(event);
}
process.on("message", (message) => {
  if (message.type === "openHost") {
    runtime = native.runtimeOpen(JSON.stringify(message.request), () => {
      if (!scheduled) {
        scheduled = true;
        setImmediate(drain);
      }
    });
  } else if (message.type === "detachHost") {
    native.runtimeDetach(runtime);
    process.disconnect();
  } else native.runtimeSubmit(runtime, JSON.stringify(message));
});
