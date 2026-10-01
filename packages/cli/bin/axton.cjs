#!/usr/bin/env node
"use strict";
// Runs the prebuilt compiler from this host's platform package, which npm
// installs as an optional dependency, forwarding arguments, stdio, the exit
// status and termination signals.
const { spawn } = require("node:child_process");

const PACKAGES = {
  "darwin-arm64": "@axtonjs/cli-darwin-arm64",
  "linux-x64-gnu": "@axtonjs/cli-linux-x64-gnu",
};

function fail(message) {
  console.error(`axton: ${message}`);
  process.exit(1);
}

const glibc = process.platform === "linux" && process.report.getReport().header.glibcVersionRuntime;
const host =
  process.platform === "linux"
    ? `linux-${process.arch}-${glibc ? "gnu" : "musl"}`
    : `${process.platform}-${process.arch}`;
const name = PACKAGES[host];
if (name === undefined) {
  fail(`unsupported platform ${host}; prebuilt compilers exist for ${Object.keys(PACKAGES).join(", ")}.`);
}
let binary;
try {
  binary = require.resolve(`${name}/bin/axton`);
} catch {
  fail(`${name} is not installed; reinstall @axtonjs/cli without omitting optional dependencies.`);
}

const child = spawn(binary, process.argv.slice(2), { stdio: "inherit" });
const forward = (signal) => child.kill(signal);
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) process.on(signal, forward);
child.on("error", (error) => fail(`could not run ${binary}: ${error.message}`));
child.on("exit", (code, signal) => {
  if (signal === null) process.exit(code);
  // End the way the compiler ended, so callers observe the same signal.
  process.removeAllListeners(signal);
  process.kill(process.pid, signal);
});
