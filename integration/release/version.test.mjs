import assert from "node:assert/strict";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  check,
  isSemver,
  jsonPathSegments,
  lockfiles,
} from "../../scripts/release/version.mjs";

const repository = fileURLToPath(new URL("../..", import.meta.url));

/** Copies every file the version check reads into a scratch root. */
function copyVersionFiles(t) {
  const root = mkdtempSync(join(tmpdir(), "axton-version-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const config = JSON.parse(readFileSync(join(repository, "release-please-config.json"), "utf8"));
  const locks = lockfiles(repository);
  const files = new Set([
    "package.json",
    "package-lock.json",
    "release-please-config.json",
    ".release-please-manifest.json",
    ...config.packages["."]["extra-files"].map((extra) => extra.path),
    ...locks.cargo,
    ...locks.dart,
  ]);
  for (const file of files) {
    cpSync(join(repository, file), join(root, file), { recursive: true });
  }
  return root;
}

function editJson(root, file, edit) {
  const path = join(root, file);
  const document = JSON.parse(readFileSync(path, "utf8"));
  edit(document);
  writeFileSync(path, `${JSON.stringify(document, null, 2)}\n`);
}

function editText(root, file, edit) {
  const path = join(root, file);
  writeFileSync(path, edit(readFileSync(path, "utf8")));
}

test("the repository carries one consistent version", () => {
  assert.deepEqual(check(repository), []);
});

test("the copied version files agree before any edit", (t) => {
  assert.deepEqual(check(copyVersionFiles(t)), []);
});

test("a manifest at another version is reported", (t) => {
  // Versions no real release reaches, so a release bump never collides.
  const root = copyVersionFiles(t);
  editJson(root, "packages/server/package.json", (manifest) => {
    manifest.version = "90.0.0";
  });
  editText(root, "Cargo.toml", (text) =>
    text.replace(/(\[workspace\.package\]\nversion = )"[^"]+"/, '$1"91.0.0"'),
  );
  editText(root, "packages/dart/pubspec.yaml", (text) =>
    text.replace(/version: \S+ # x-release-please-version/, "version: 92.0.0 # x-release-please-version"),
  );
  const problems = check(root);
  assert.ok(problems.some((p) => p.startsWith("packages/server/package.json $.version: 90.0.0")), problems.join("\n"));
  assert.ok(problems.some((p) => p.startsWith("Cargo.toml $.workspace.package.version: 91.0.0")), problems.join("\n"));
  assert.ok(problems.some((p) => p.startsWith("packages/dart/pubspec.yaml: 92.0.0")), problems.join("\n"));
});

test("an internal dependency must pin the release version exactly", (t) => {
  const root = copyVersionFiles(t);
  const version = JSON.parse(readFileSync(join(root, ".release-please-manifest.json"), "utf8"))["."];
  const config = JSON.parse(readFileSync(join(root, "release-please-config.json"), "utf8"));
  const pinned = config.packages["."]["extra-files"].find(
    (extra) => extra.type === "json" && /^\$\.(dependencies|optionalDependencies)\['/.test(extra.jsonpath),
  );
  if (pinned) {
    // An existing internal pin loosened to a range.
    const [, field, name] = pinned.jsonpath.match(/^\$\.(\w+)\['([^']+)'\]$/);
    editJson(root, pinned.path, (manifest) => {
      manifest[field][name] = `^${version}`;
    });
    const problems = check(root);
    assert.ok(problems.some((p) => p.includes(`${name} is ^${version}, expected exactly ${version}`)), problems.join("\n"));
  }
  // A new internal pin that release-please would not keep current.
  editJson(root, "packages/postgres/package.json", (manifest) => {
    manifest.dependencies = { ...manifest.dependencies, "@axtonjs/unlisted": version };
  });
  const problems = check(root);
  assert.ok(
    problems.includes(
      "packages/postgres/package.json: dependencies @axtonjs/unlisted is not updated by release-please-config.json",
    ),
    problems.join("\n"),
  );
});

test("a workspace package release-please does not version is reported", (t) => {
  const root = copyVersionFiles(t);
  const version = JSON.parse(readFileSync(join(root, ".release-please-manifest.json"), "utf8"))["."];
  mkdirSync(join(root, "packages/extra"), { recursive: true });
  writeFileSync(join(root, "packages/extra/package.json"), JSON.stringify({ name: "@axtonjs/extra", version }));
  editJson(root, "package.json", (manifest) => {
    manifest.workspaces.push("packages/extra");
  });
  editJson(root, "package-lock.json", (lock) => {
    lock.packages["packages/extra"] = { name: "@axtonjs/extra", version };
  });
  const problems = check(root);
  assert.ok(problems.includes("packages/extra/package.json: version is not updated by release-please-config.json"), problems.join("\n"));
  assert.ok(problems.includes("package-lock.json: packages/extra version is not updated by release-please-config.json"), problems.join("\n"));
});

test("an invalid SemVer release version is refused", (t) => {
  for (const invalid of ["0.1", "v0.1.0", "01.0.0", "0.1.0-", "latest"]) {
    assert.equal(isSemver(invalid), false, invalid);
  }
  for (const valid of ["0.1.0", "0.1.0-alpha.1", "1.0.0+build.5"]) {
    assert.equal(isSemver(valid), true, valid);
  }
  const root = copyVersionFiles(t);
  writeFileSync(join(root, ".release-please-manifest.json"), '{".": "0.1"}\n');
  assert.deepEqual(check(root), ['.release-please-manifest.json: "0.1" is not a valid SemVer version']);
});

test("a Cargo or Dart lockfile at another version is reported", (t) => {
  const root = copyVersionFiles(t);
  const locks = lockfiles(root);
  assert.ok(locks.cargo.includes("Cargo.lock") && locks.cargo.includes("bindings/node/Cargo.lock"), locks.cargo.join(" "));
  assert.ok(locks.dart.length > 0);
  editText(root, "bindings/node/Cargo.lock", (text) => text.replace(/(name = "axton-node"\nversion = )"[^"]+"/, '$1"0.0.9"'));
  editText(root, locks.dart[0], (text) =>
    text.replace(/( {4}source: path\n {4}version: )"[^"]+"/, '$1"0.0.9"'),
  );
  assert.deepEqual(
    check(root).filter((p) => p.includes("0.0.9")).map((p) => p.split(",")[0]),
    ["bindings/node/Cargo.lock: axton-node is 0.0.9", `${locks.dart[0]}: axton is 0.0.9`],
  );
});

test("JSONPath outside the supported subset is refused", () => {
  assert.deepEqual(jsonPathSegments("$.packages['packages/server'].version"), ["packages", "packages/server", "version"]);
  assert.deepEqual(jsonPathSegments("$.packages[''].version"), ["packages", "", "version"]);
  assert.throws(() => jsonPathSegments("$.package[?(@.name=='axton-core')].version"), /unsupported JSONPath/);
  assert.throws(() => jsonPathSegments("$..version"), /unsupported JSONPath/);
});

test("the version script's own CLI exits non-zero on disagreement", async (t) => {
  const { spawnSync } = await import("node:child_process");
  const script = join(dirname(fileURLToPath(import.meta.url)), "../../scripts/release/version.mjs");
  const root = copyVersionFiles(t);
  assert.equal(spawnSync(process.execPath, [script, "check", "--root", root]).status, 0);
  editJson(root, "package.json", (manifest) => {
    manifest.version = "9.9.9";
  });
  const failed = spawnSync(process.execPath, [script, "check", "--root", root], { encoding: "utf8" });
  assert.equal(failed.status, 1);
  assert.match(failed.stderr, /package\.json: version is 9\.9\.9, expected/);
});
