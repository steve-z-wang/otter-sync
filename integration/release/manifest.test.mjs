// The release inventory and the retry rule: a version already on a registry is
// skipped only when it holds the manifest's bytes; anything else stops.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import {
  expectedArtifacts,
  MANIFEST,
  npmState,
  pubState,
  verify,
  write,
} from "../../scripts/release/manifest.mjs";
import { releaseVersion } from "../../scripts/release/version.mjs";

const repository = new URL("../..", import.meta.url).pathname;
const targets = JSON.parse(readFileSync(join(repository, "scripts/release/targets.json"), "utf8"));
const version = releaseVersion(repository);

function scratch(t) {
  const directory = mkdtempSync(join(tmpdir(), "axton-manifest-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

/** A gzipped tar of [files] (path → text), as tar writes it. */
function archive(t, files) {
  const root = scratch(t);
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, "files", path)), { recursive: true });
    writeFileSync(join(root, "files", path), text);
  }
  const result = spawnSync("tar", ["-czf", join(root, "a.tar.gz"), "-C", join(root, "files"), "."]);
  assert.equal(result.status, 0);
  return readFileSync(join(root, "a.tar.gz"));
}

/** A fetch that answers each URL from [routes]: a status, or [status, body]. */
function registry(routes) {
  return async (url) => {
    const route = routes[url];
    if (route === undefined) throw new TypeError(`fetch failed: no route to ${url}`);
    const [status, body] = Array.isArray(route) ? route : [route];
    return new Response(status === 200 ? (Buffer.isBuffer(body) ? body : JSON.stringify(body)) : null, { status });
  };
}

const npmUrl = "https://registry.npmjs.org/@axtonjs%2fnative";
const npm = (routes) => npmState({ fetch: registry(routes), name: "@axtonjs/native", version, integrity: "sha512-built" });

test("npm: an unknown package or version is published", async () => {
  assert.equal(await npm({ [npmUrl]: 404 }), "publish");
  assert.equal(await npm({ [npmUrl]: [200, { versions: { "0.0.1": { dist: { integrity: "sha512-old" } } } }] }), "publish");
});

test("npm: the same bytes are skipped, different bytes stop", async () => {
  const at = (integrity) => ({ [npmUrl]: [200, { versions: { [version]: { dist: { integrity } } } }] });
  assert.equal(await npm(at("sha512-built")), "skip");
  await assert.rejects(npm(at("sha512-other")), /already on npm with integrity sha512-other/);
});

test("npm: an unreadable registry stops rather than counting as absent", async () => {
  await assert.rejects(npm({ [npmUrl]: 503 }), /HTTP 503/);
  await assert.rejects(npm({}), /failed: fetch failed/);
});

test("pub: an unknown package or version is published", async () => {
  const pub = (routes) => pubState({ fetch: registry(routes), name: "axton", version, archive: Buffer.alloc(0) });
  assert.equal(await pub({ "https://pub.dev/api/packages/axton": 404 }), "publish");
  assert.equal(await pub({ "https://pub.dev/api/packages/axton": [200, { versions: [{ version: "0.0.1" }] }] }), "publish");
});

test("pub: the same files are skipped, different files or a corrupt download stop", async (t) => {
  const staged = archive(t, { "pubspec.yaml": "name: axton\n", "lib/axton.dart": "library;\n" });
  const pub = (published, archiveSha256) =>
    pubState({
      fetch: registry({
        "https://pub.dev/api/packages/axton": [
          200,
          {
            versions: [
              {
                version,
                archive_url: "https://pub.dev/archive.tar.gz",
                archive_sha256: archiveSha256 ?? createHash("sha256").update(published).digest("hex"),
              },
            ],
          },
        ],
        "https://pub.dev/archive.tar.gz": [200, published],
      }),
      name: "axton",
      version,
      archive: staged,
    });
  // pub's own archive: other bytes, the same files.
  assert.equal(await pub(archive(t, { "lib/axton.dart": "library;\n", "pubspec.yaml": "name: axton\n" })), "skip");
  await assert.rejects(
    pub(archive(t, { "pubspec.yaml": "name: axton\n", "lib/axton.dart": "library; // changed\n" })),
    /different files: lib\/axton.dart/,
  );
  await assert.rejects(pub(archive(t, { "pubspec.yaml": "name: axton\n" })), /different files: lib\/axton.dart/);
  await assert.rejects(pub(staged, "0".repeat(64)), /does not match pub.dev's archive_sha256/);
});

test("the manifest lists exactly the release's files, npm archives in dependency order", (t) => {
  const expected = expectedArtifacts(targets, version);
  const npmOrder = expected.filter((artifact) => artifact.kind === "npm").map((artifact) => artifact.package);
  const platforms = targets.host.flatMap((target) => [`@axtonjs/native-${target.name}`, `@axtonjs/cli-${target.name}`]);
  assert.deepEqual(npmOrder, [
    ...platforms,
    "@axtonjs/native",
    "@axtonjs/cli",
    "@axtonjs/server",
    "@axtonjs/client",
    "@axtonjs/postgres",
  ]);
  assert.equal(
    expected.filter((artifact) => artifact.kind === "dart-library").length,
    targets.host.filter((target) => target.dartLibrary).length + targets.mobile.length,
  );

  const directory = scratch(t);
  const packed = [];
  for (const artifact of expected) {
    const bytes = Buffer.from(`bytes of ${artifact.file}`);
    writeFileSync(join(directory, artifact.file), bytes);
    if (artifact.kind === "npm") {
      const integrity = `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
      packed.push({ filename: artifact.file, name: artifact.package, version, integrity });
    }
  }
  const packedFile = join(scratch(t), "packed.json");
  writeFileSync(packedFile, JSON.stringify(packed));
  const manifest = write(directory, packedFile, "abc123", targets);
  assert.equal(manifest.version, version);
  assert.equal(manifest.commit, "abc123");
  assert.equal(manifest.artifacts.length, expected.length);
  assert.ok(manifest.artifacts.every((artifact) => /^[0-9a-f]{64}$/.test(artifact.sha256) && artifact.size > 0));
  assert.ok(manifest.artifacts.filter((a) => a.kind === "npm").every((a) => a.integrity.startsWith("sha512-")));
  assert.deepEqual(JSON.parse(readFileSync(join(directory, MANIFEST), "utf8")), manifest);
  verify(directory);

  writeFileSync(join(directory, expected.at(-1).file), "changed");
  assert.throws(() => verify(directory), /axton-dart-.*: does not match/);
  writeFileSync(join(directory, "stray.txt"), "");
  rmSync(join(directory, expected[0].file));
  assert.throws(() => write(directory, packedFile, "abc123", targets), (error) => {
    assert.match(error.message, new RegExp(`${expected[0].file}: missing`));
    assert.match(error.message, /stray.txt: not a release file/);
    return true;
  });
});
