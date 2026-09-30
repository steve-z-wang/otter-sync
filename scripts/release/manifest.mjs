#!/usr/bin/env node
// The inventory of one release and the rule for publishing it again.
//
//   node scripts/release/manifest.mjs write DIR --packed FILE --commit SHA
//       writes DIR/release-manifest.json for the files in DIR: every npm
//       archive listed in FILE (`npm pack --json`), every Dart library and the
//       staged Dart package, each with its size and SHA-256 and, for npm, its
//       integrity. DIR must hold exactly the release's files.
//   node scripts/release/manifest.mjs verify DIR
//       checks every file of DIR/release-manifest.json against it.
//   node scripts/release/manifest.mjs npm DIR
//       prints `publish FILE` or `skip FILE` for each npm archive, in
//       dependency order.
//   node scripts/release/manifest.mjs pub DIR
//       prints `publish` or `skip` for the Dart package.
//
// A version already on a registry is skipped only when it holds exactly the
// manifest's bytes: npm's `dist.integrity` equals the archive's, or pub.dev's
// archive holds the staged package's files. Any other answer, including an
// unreadable registry, stops the run.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { releaseVersion } from "./version.mjs";

const repository = fileURLToPath(new URL("../..", import.meta.url));
export const MANIFEST = "release-manifest.json";
const NPM_REGISTRY = "https://registry.npmjs.org";
const PUB_REGISTRY = "https://pub.dev";

/** The release's files, npm archives first in the order they are published. */
export function expectedArtifacts(targets, version) {
  const npm = [
    ...targets.host.flatMap((target) => [`native-${target.name}`, `cli-${target.name}`]),
    "native",
    "cli",
    "server",
    "client",
    "postgres",
  ].map((name) => {
    const target = targets.host.find((host) => name.endsWith(`-${host.name}`));
    return {
      kind: "npm",
      package: `@axtonjs/${name}`,
      ...(target && { target: target.name }),
      file: `axtonjs-${name}-${version}.tgz`,
    };
  });
  const libraries = [
    ...targets.host.filter((target) => target.dartLibrary).map((target) => [target.name, target.dartLibrary]),
    ...targets.mobile.map((target) => [target.name, target.dynamicLibrary]),
  ].map(([target, library]) => ({
    kind: "dart-library",
    target,
    file: `libaxton_dart-${version}-${target}${library.slice(library.lastIndexOf("."))}`,
  }));
  return [...npm, ...libraries, { kind: "dart-package", package: "axton", file: `axton-dart-${version}.tar.gz` }];
}

const digest = (algorithm, bytes, encoding) => createHash(algorithm).update(bytes).digest(encoding);
const sha256 = (bytes) => digest("sha256", bytes, "hex");
const integrity = (bytes) => `sha512-${digest("sha512", bytes, "base64")}`;

export function write(directory, packedFile, commit, targets) {
  const version = releaseVersion(repository);
  const packed = new Map(JSON.parse(readFileSync(packedFile, "utf8")).map((entry) => [entry.filename, entry]));
  const expected = expectedArtifacts(targets, version);
  const problems = [];
  const present = new Set(readdirSync(directory).filter((name) => name !== MANIFEST));
  const artifacts = [];
  for (const artifact of expected) {
    present.delete(artifact.file);
    const path = join(directory, artifact.file);
    if (!existsSync(path)) {
      problems.push(`${artifact.file}: missing`);
      continue;
    }
    const bytes = readFileSync(path);
    const entry = { ...artifact, size: bytes.length, sha256: sha256(bytes) };
    if (artifact.kind === "npm") {
      entry.integrity = integrity(bytes);
      const pack = packed.get(artifact.file);
      if (pack?.name !== artifact.package || pack.version !== version || pack.integrity !== entry.integrity) {
        problems.push(`${artifact.file}: not the archive npm pack reported for ${artifact.package}@${version}`);
      }
    }
    artifacts.push(entry);
  }
  for (const name of present) problems.push(`${name}: not a release file`);
  if (problems.length > 0) throw new Error(problems.join("\n"));
  const manifest = { schemaVersion: 1, version, commit, artifacts };
  writeFileSync(join(directory, MANIFEST), `${JSON.stringify(manifest, null, 2)}\n`);
  return manifest;
}

export function read(directory) {
  return JSON.parse(readFileSync(join(directory, MANIFEST), "utf8"));
}

export function verify(directory) {
  const problems = [];
  for (const artifact of read(directory).artifacts) {
    const path = join(directory, artifact.file);
    if (!existsSync(path)) {
      problems.push(`${artifact.file}: missing`);
      continue;
    }
    const bytes = readFileSync(path);
    if (bytes.length !== artifact.size || sha256(bytes) !== artifact.sha256) {
      problems.push(`${artifact.file}: does not match ${MANIFEST}`);
    }
  }
  if (problems.length > 0) throw new Error(problems.join("\n"));
}

async function get(fetch, url, init) {
  let response;
  try {
    response = await fetch(url, init);
  } catch (error) {
    throw new Error(`GET ${url} failed: ${error.message}`);
  }
  if (response.status === 404) return undefined;
  if (!response.ok) throw new Error(`GET ${url} answered HTTP ${response.status}`);
  return response;
}

/** `publish` when npm has no `name@version`, `skip` when it has these bytes. */
export async function npmState({ fetch, name, version, integrity }) {
  const url = `${NPM_REGISTRY}/${name.replace("/", "%2f")}`;
  const response = await get(fetch, url, { headers: { accept: "application/vnd.npm.install-v1+json" } });
  if (!response) return "publish";
  const published = (await response.json()).versions?.[version];
  if (!published) return "publish";
  if (published.dist?.integrity === integrity) return "skip";
  throw new Error(
    `${name}@${version} is already on npm with integrity ${published.dist?.integrity}, ` +
      `but this release built ${integrity}`,
  );
}

function extract(bytes) {
  const directory = mkdtempSync(join(tmpdir(), "axton-archive-"));
  const archive = join(directory, "archive.tar.gz");
  writeFileSync(archive, bytes);
  const root = join(directory, "files");
  mkdirSync(root);
  const tar = spawnSync("tar", ["-xzf", archive, "-C", root], { encoding: "utf8" });
  if (tar.status !== 0) throw new Error(`tar could not extract an archive: ${tar.stderr}`);
  return { root, remove: () => rmSync(directory, { recursive: true, force: true }) };
}

/** Each regular file under [root], by relative path, with its SHA-256. */
function tree(root, directory = root, files = new Map()) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) tree(root, path, files);
    else if (statSync(path).isFile()) files.set(relative(root, path), sha256(readFileSync(path)));
  }
  return files;
}

/** The paths at which two archives differ; empty when they hold the same files. */
export function differences(left, right) {
  const [a, b] = [extract(left), extract(right)];
  try {
    const [x, y] = [tree(a.root), tree(b.root)];
    return [...new Set([...x.keys(), ...y.keys()])].filter((path) => x.get(path) !== y.get(path)).sort();
  } finally {
    a.remove();
    b.remove();
  }
}

/**
 * `publish` when pub.dev has no `name` `version`, `skip` when its archive
 * holds exactly the files of [archive]. pub builds its own archive, whose
 * bytes differ run to run, so the comparison is by file content.
 */
export async function pubState({ fetch, name, version, archive }) {
  const url = `${PUB_REGISTRY}/api/packages/${name}`;
  const response = await get(fetch, url, { headers: { accept: "application/vnd.pub.v2+json" } });
  if (!response) return "publish";
  const published = (await response.json()).versions?.find((entry) => entry.version === version);
  if (!published) return "publish";
  const download = await get(fetch, published.archive_url);
  if (!download) throw new Error(`${published.archive_url} is missing`);
  const bytes = Buffer.from(await download.arrayBuffer());
  if (sha256(bytes) !== published.archive_sha256) {
    throw new Error(`${published.archive_url} does not match pub.dev's archive_sha256`);
  }
  const changed = differences(bytes, archive);
  if (changed.length === 0) return "skip";
  throw new Error(`${name} ${version} is already on pub.dev with different files: ${changed.join(", ")}`);
}

async function main([command, directory, ...options]) {
  const option = (name) => options[options.indexOf(name) + 1];
  if (command === "write" && directory && options.includes("--packed") && options.includes("--commit")) {
    const targets = JSON.parse(readFileSync(join(repository, "scripts/release/targets.json"), "utf8"));
    const manifest = write(directory, option("--packed"), option("--commit"), targets);
    console.log(`${MANIFEST}: ${manifest.artifacts.length} files of axton ${manifest.version}`);
  } else if (command === "verify" && directory) {
    verify(directory);
    console.log(`${directory}: every file matches ${MANIFEST}`);
  } else if (command === "npm" && directory) {
    const { version, artifacts } = read(directory);
    for (const artifact of artifacts.filter((entry) => entry.kind === "npm")) {
      const state = await npmState({ fetch, name: artifact.package, version, integrity: artifact.integrity });
      console.log(`${state} ${artifact.file}`);
    }
  } else if (command === "pub" && directory) {
    const { version, artifacts } = read(directory);
    const artifact = artifacts.find((entry) => entry.kind === "dart-package");
    const archive = readFileSync(join(directory, artifact.file));
    console.log(await pubState({ fetch, name: artifact.package, version, archive }));
  } else {
    console.error(
      "usage: node scripts/release/manifest.mjs write DIR --packed FILE --commit SHA | verify DIR | npm DIR | pub DIR",
    );
    process.exit(2);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(`manifest: ${error.message}`);
    process.exit(1);
  });
}
