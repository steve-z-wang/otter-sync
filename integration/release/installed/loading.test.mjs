// How the installed selectors fail and forward: the napi-rs addon loader and
// the `axton` launcher. Each case edits this scratch project's node_modules
// and restores it.
import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const require = createRequire(import.meta.url);
const host = process.platform === "linux" ? `linux-${process.arch}-gnu` : `${process.platform}-${process.arch}`;
const nativePackage = dirname(require.resolve(`@axtonjs/native-${host}/package.json`));
const cliPackage = dirname(require.resolve(`@axtonjs/cli-${host}/package.json`));
const launcher = require.resolve("@axtonjs/cli/bin/axton.cjs");
const run = (args, options = {}) => spawnSync(process.execPath, args, { encoding: "utf8", ...options });
const pretend = "Object.defineProperty(process, 'platform', { value: 'aix' });";

function aside(path, t) {
  renameSync(path, `${path}.aside`);
  t.after(() => renameSync(`${path}.aside`, path));
}

test("the addon loader names an unsupported platform", () => {
  const result = run(["-e", `${pretend} require("@axtonjs/native")`]);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Unsupported OS: aix/);
});

test("the addon loader reports a missing platform package", (t) => {
  aside(nativePackage, t);
  const result = run(["-e", 'require("@axtonjs/native")']);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Cannot find native binding/);
  assert.match(result.stderr, new RegExp(`Cannot find module '@axtonjs/native-${host}'`));
});

test("the addon loader reports an addon that cannot be loaded", (t) => {
  const addon = join(nativePackage, `axton-node.${host}.node`);
  copyFileSync(addon, `${addon}.aside`);
  t.after(() => renameSync(`${addon}.aside`, addon));
  writeFileSync(addon, "not a shared library");
  const result = run(["-e", 'require("@axtonjs/native")']);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Cannot find native binding/);
  assert.doesNotMatch(result.stderr, new RegExp(`Cannot find module '@axtonjs/native-${host}'`));
});

test("the launcher names an unsupported platform", () => {
  const result = run(["-e", `${pretend} process.argv = [process.execPath, "axton", "--version"]; require(${JSON.stringify(launcher)})`]);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /axton: unsupported platform aix-/);
});

test("the launcher reports a missing platform package", (t) => {
  aside(cliPackage, t);
  const result = run([launcher, "--version"]);
  assert.equal(result.status, 1);
  assert.match(result.stderr, new RegExp(`axton: @axtonjs/cli-${host} is not installed`));
});

test("the launcher forwards arguments, stdio, exit status and signals", (t) => {
  const binary = join(cliPackage, "bin", "axton");
  copyFileSync(binary, `${binary}.aside`);
  t.after(() => renameSync(`${binary}.aside`, binary));
  // A stand-in compiler: echoes its arguments and stdin, then exits as told.
  writeFileSync(
    binary,
    `#!/bin/sh
case "$1" in
  echo) shift; printf '%s|' "$@"; cat; exit 7 ;;
  wait) sleep 30 >/dev/null & trap 'kill $!; exit 42' TERM; echo ready; wait ;;
  die) kill -TERM $$ ;;
esac
`,
  );
  chmodSync(binary, 0o755);
  const echoed = run([launcher, "echo", "a b", "c"], { input: "stdin" });
  assert.equal(echoed.status, 7);
  assert.equal(echoed.stdout, "a b|c|stdin");
  const died = run([launcher, "die"]);
  assert.equal(died.signal, "SIGTERM", "the launcher ends with the compiler's signal");
  return new Promise((resolve, reject) => {
    const { spawn } = require("node:child_process");
    const child = spawn(process.execPath, [launcher, "wait"], { stdio: ["ignore", "pipe", "inherit"] });
    child.stdout.once("data", () => child.kill("SIGTERM"));
    child.on("exit", (code) => {
      try {
        assert.equal(code, 42, "SIGTERM reached the compiler, whose exit status came back");
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  });
});

test("the addon and the compiler come from the same release", () => {
  const version = JSON.parse(readFileSync(require.resolve("@axtonjs/native/package.json"), "utf8")).version;
  assert.equal(JSON.parse(readFileSync(join(nativePackage, "package.json"), "utf8")).version, version);
  assert.equal(run([launcher, "--version"]).stdout, `axton ${version}\n`);
});
