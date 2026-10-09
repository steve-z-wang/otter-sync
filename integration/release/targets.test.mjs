// scripts/release/targets.json is the inventory; every place that repeats a
// target or a floor must agree with it.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const json = (path) => JSON.parse(read(path));
const targets = json("scripts/release/targets.json");
const names = targets.host.map((target) => target.name);

test("napi builds exactly the host targets", () => {
  assert.deepEqual(json("packages/native/package.json").napi.targets, targets.host.map((target) => target.rust));
});

for (const kind of ["native", "cli"]) {
  test(`@axtonjs/${kind} has one platform package per host target`, () => {
    for (const target of targets.host) {
      const platform = json(`packages/${kind}/npm/${target.name}/package.json`);
      assert.equal(platform.name, `@axtonjs/${kind}-${target.name}`);
      assert.deepEqual(platform.os, [target.npm.os]);
      assert.deepEqual(platform.cpu, [target.npm.cpu]);
      assert.deepEqual(platform.libc, target.npm.libc === undefined ? undefined : [target.npm.libc]);
      assert.deepEqual(platform.files, [kind === "native" ? target.addon : `bin/${target.cli}`]);
    }
  });
}

test("the axton launcher knows the same host targets", () => {
  const launcher = read("packages/cli/bin/axton.cjs");
  const table = [...launcher.matchAll(/"([\w-]+)": "@axtonjs\/cli-([\w-]+)"/g)];
  assert.deepEqual(table.map(([, key, name]) => [key, name]), names.map((name) => [name, name]));
});

test("the SDK floors match every manifest", () => {
  for (const directory of ["native", "backend/server", "frontend/client-js", "backend/postgres", "cli"]) {
    assert.equal(json(`packages/${directory}/package.json`).engines.node, targets.floors.node, directory);
  }
  assert.match(read("packages/frontend/dart/pubspec.yaml"), new RegExp(`sdk: '${targets.floors.dart} <4\\.0\\.0'`));
});
