import { fork } from "node:child_process";
import { WebSocket } from "ws";
import assert from "node:assert/strict";

// Executes host effects only. It never interprets authority/settlement or edits SQLite.
export class NativeHost {
  constructor(url) {
    this.url = url;
    this.events = [];
    this.waiters = [];
    this.effects = new Map();
    this.requests = [];
    this.hold = null;
    this.frameHold = null;
    this.heldFrames = [];
    this.failures = [];
    this.sequence = 0;
    this.stopping = false;
  }
  async open(path, schema, binding) {
    this.viewer = binding.viewer;
    this.child = fork(new URL("./actor-child.mjs", import.meta.url), [], {
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    this.child.on("message", (event) => {
      this.events.push(event);
      if (event.type === "effect")
        void this.effect(event).catch((error) => {
          this.failures.push(error);
          this.flush();
        });
      if (event.type === "cancelEffect") {
        this.effects.get(event.effectId)?.();
        this.effects.delete(event.effectId);
      }
      this.flush();
    });
    this.child.send({
      type: "openHost",
      request: { type: "open", requestId: "open", path, schema, binding },
    });
    const opened = await this.wait(
      (e) => e.type === "taskCompleted" && e.requestId === "open",
    );
    assert.equal(opened.ok, true, JSON.stringify(opened));
    this.context = opened.value.context;
    return this;
  }
  flush() {
    for (const waiter of [...this.waiters]) {
      const at = this.events.findIndex(waiter.match);
      if (at >= 0) {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        clearTimeout(waiter.timer);
        waiter.resolve(this.events.splice(at, 1)[0]);
      } else if (this.failures.length) {
        clearTimeout(waiter.timer);
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        waiter.reject(this.failures[0]);
      }
    }
  }
  wait(match) {
    return new Promise((resolve, reject) => {
      const waiter = {
        match,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          reject(
            new Error(
              `native event timeout: ${JSON.stringify(this.events.slice(-12))}`,
            ),
          );
        }, 20000),
      };
      this.waiters.push(waiter);
      this.flush();
    });
  }
  send(message) {
    this.child.send(message);
  }
  reply(effect, value) {
    if (this.child.connected && !this.stopping)
      this.send({
        type: "effectResult",
        effectId: effect.effectId,
        outcome: { ok: true, value },
      });
  }
  async task(command) {
    const requestId = `request${++this.sequence}`;
    this.send({ type: "task", requestId, command });
    const event = await this.wait(
      (e) => e.type === "taskCompleted" && e.requestId === requestId,
    );
    assert.equal(event.ok, true, JSON.stringify(event));
    return event.value;
  }
  async sql(sql, parameters = []) {
    return this.task({ kind: "sql", sql, parameters });
  }
  async effect(effect) {
    const op = effect.operation;
    if (op.kind === "timer") {
      const timer = setTimeout(() => this.reply(effect, null), op.millis);
      this.effects.set(effect.effectId, () => clearTimeout(timer));
      return;
    }
    if (op.kind === "socket") {
      const ws = new WebSocket(
        this.url.replace("http:", "ws:") + "/sync/live",
        {
          headers: { "x-viewer": this.viewer },
        },
      );
      this.effects.set(effect.effectId, () => ws.close());
      ws.on("open", () => ws.send(op.subscribe));
      ws.on("message", (data) => {
        const value = { event: "message", body: data.toString() };
        if (this.frameHold?.(JSON.parse(value.body)))
          this.heldFrames.push({ effect, value });
        else this.reply(effect, value);
      });
      ws.on("close", () => this.reply(effect, { event: "closed" }));
      ws.on("error", () => this.reply(effect, { event: "closed" }));
      return;
    }
    assert.equal(op.kind, "http", JSON.stringify(effect));
    const body = JSON.parse(op.body);
    this.requests.push({ route: op.route, body });
    const route = { action: "actions", fetch: "fetch", pull: "pull" }[op.route];
    assert.ok(route, `retired HTTP route ${op.route}`);
    const response = await fetch(this.url + "/sync/" + route, {
      method: "POST",
      headers: { "x-viewer": this.viewer },
      body: op.body,
    });
    const text = await response.text();
    const value = { status: response.status, body: text };
    if (
      this.hold &&
      (await this.hold({
        effect,
        route: op.route,
        body,
        response: JSON.parse(text),
      }))
    )
      return;
    if (response.ok) this.reply(effect, value);
    else if (this.child.connected && !this.stopping)
      this.send({
        type: "effectResult",
        effectId: effect.effectId,
        outcome: {
          ok: false,
          error: { message: text, status: response.status },
        },
      });
  }
  async until(sql, predicate) {
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
      const rows = await this.sql(sql);
      if (predicate(rows)) return rows;
      await new Promise((resolve) => setImmediate(resolve));
    }
    throw new Error(`committed condition timed out after 20s: ${sql}`);
  }
  async kill() {
    this.stopping = true;
    for (const stop of this.effects.values()) stop();
    this.effects.clear();
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    const exited = new Promise((resolve) => this.child.once("exit", resolve));
    this.child.kill("SIGKILL");
    await exited;
  }
  async close() {
    if (!this.child?.connected) return;
    this.stopping = true;
    this.send({ type: "close" });
    await this.wait((e) => e.type === "runtimeClosed");
    for (const stop of this.effects.values()) stop();
    this.effects.clear();
    this.child.send({ type: "detachHost" });
    await new Promise((resolve) => this.child.once("exit", resolve));
  }
}
