#!/usr/bin/env node
// One release version V for every AXTON artifact.
//
// release-please owns V: `.release-please-manifest.json` records it and its
// built-in updaters rewrite the files listed in `release-please-config.json`
// (the root package.json and package-lock.json through the node strategy, the
// rest through `extra-files`). This script covers only what those updaters
// cannot: the local packages recorded in Cargo.lock files and the path entries
// for the Dart SDK in pubspec.lock files.
//
//   node scripts/release/version.mjs check   exits 1 on any disagreement
//   node scripts/release/version.mjs sync    rewrites the lockfile entries to V
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const INTERNAL_SCOPE = "@axtonjs/";
const DEPENDENCY_FIELDS = [
  "dependencies",
  "optionalDependencies",
  "peerDependencies",
  "devDependencies",
];
// Directories that hold build output or installed dependencies, never sources.
const SKIPPED_DIRECTORIES = new Set([
  ".git",
  ".dart_tool",
  ".tools",
  ".venv",
  "build",
  "dist",
  "node_modules",
  "site",
  "target",
]);

// Semantic Versioning 2.0.0, https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/;

export function isSemver(value) {
  return typeof value === "string" && SEMVER.test(value);
}

function readJson(root, path) {
  return JSON.parse(readFileSync(join(root, path), "utf8"));
}

/**
 * Splits the JSONPath subset used in the release-please configuration:
 * `$` followed by `.name` or `['key']` segments. Anything else is refused so
 * that the check never silently evaluates a path differently from release-please.
 */
export function jsonPathSegments(path) {
  if (!path.startsWith("$")) throw new Error(`unsupported JSONPath ${path}`);
  const segments = [];
  const pattern = /\.([A-Za-z_][\w-]*)|\['([^']*)'\]|\["([^"]*)"\]/y;
  pattern.lastIndex = 1;
  while (pattern.lastIndex < path.length) {
    const match = pattern.exec(path);
    if (!match) throw new Error(`unsupported JSONPath ${path}`);
    segments.push(match[1] ?? match[2] ?? match[3]);
  }
  return segments;
}

function jsonPathValue(document, path) {
  let value = document;
  for (const segment of jsonPathSegments(path)) {
    if (value === null || typeof value !== "object" || !(segment in value)) {
      return undefined;
    }
    value = value[segment];
  }
  return value;
}

/** Reads a string value from `[table]` sections of a TOML manifest. */
function tomlValue(text, path) {
  const segments = jsonPathSegments(path);
  const key = segments.pop();
  const table = segments.join(".");
  let current = "";
  for (const line of text.split("\n")) {
    const header = line.match(/^\s*\[([^\]]+)\]\s*$/);
    if (header) {
      current = header[1].trim();
      continue;
    }
    const entry = line.match(/^\s*([A-Za-z0-9_-]+)\s*=\s*"([^"]*)"/);
    if (entry && current === table && entry[1] === key) return entry[2];
  }
  return undefined;
}

/** The release version recorded by release-please. */
export function releaseVersion(root) {
  return readJson(root, ".release-please-manifest.json")["."];
}

function releaseConfig(root) {
  const config = readJson(root, "release-please-config.json");
  const unit = config.packages?.["."];
  if (!unit || Object.keys(config.packages).length !== 1) {
    throw new Error("release-please-config.json must define exactly one root release unit");
  }
  return { ...config, ...unit };
}

function walk(root, name, found = [], directory = root) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!SKIPPED_DIRECTORIES.has(entry.name)) {
        walk(root, name, found, join(directory, entry.name));
      }
    } else if (entry.name === name) {
      found.push(relative(root, join(directory, entry.name)));
    }
  }
  return found.sort();
}

/** Every Cargo.lock and pubspec.lock whose AXTON entries this script owns. */
export function lockfiles(root) {
  return { cargo: walk(root, "Cargo.lock"), dart: walk(root, "pubspec.lock") };
}

// Cargo.lock: a local package has no `source`; all of them are AXTON crates.
function cargoLock(text, version) {
  const found = [];
  const updated = text.replace(
    /\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"\n(?!source = )/g,
    (block, name, current) => {
      found.push({ name, version: current });
      return `[[package]]\nname = "${name}"\nversion = "${version}"\n`;
    },
  );
  return { found, updated };
}

// pubspec.lock: the `axton` package resolved from a path is the Dart SDK.
function pubspecLock(text, version) {
  const found = [];
  const updated = text.replace(
    /^( {2}axton:\n(?: {4}.*\n| {6}.*\n)*? {4}source: path\n {4}version: )"([^"]+)"$/m,
    (_, prefix, current) => {
      found.push({ name: "axton", version: current });
      return `${prefix}"${version}"`;
    },
  );
  return { found, updated };
}

function internalPins(document, label, version, problems, covered) {
  for (const field of DEPENDENCY_FIELDS) {
    for (const [name, spec] of Object.entries(document[field] ?? {})) {
      if (!name.startsWith(INTERNAL_SCOPE)) continue;
      if (spec !== version) {
        problems.push(`${label}: ${field} ${name} is ${spec}, expected exactly ${version}`);
      }
      if (covered && !covered.has(`${field}['${name}']`)) {
        problems.push(`${label}: ${field} ${name} is not updated by release-please-config.json`);
      }
    }
  }
}

/** Returns every disagreement with V; an empty list means consistent. */
export function check(root) {
  const problems = [];
  const version = releaseVersion(root);
  if (!isSemver(version)) {
    return [`.release-please-manifest.json: "${version}" is not a valid SemVer version`];
  }
  const config = releaseConfig(root);
  if (config["release-type"] !== "node") {
    problems.push("release-please-config.json: the root release unit must use release-type node");
  }

  // The node strategy versions the root package.json and package-lock.json.
  const rootPackage = readJson(root, "package.json");
  if (rootPackage.version !== version) {
    problems.push(`package.json: version is ${rootPackage.version}, expected ${version}`);
  }
  const rootLock = readJson(root, "package-lock.json");
  if (rootLock.version !== version || rootLock.packages?.[""]?.version !== version) {
    problems.push(`package-lock.json: root version is ${rootLock.version}, expected ${version}`);
  }

  // Everything else release-please rewrites is listed in extra-files.
  const coveredPins = new Map();
  for (const extra of config["extra-files"] ?? []) {
    const file = typeof extra === "string" ? extra : extra.path;
    if (!existsSync(join(root, file))) {
      problems.push(`${file}: listed in extra-files but missing`);
      continue;
    }
    const text = readFileSync(join(root, file), "utf8");
    let value;
    if (extra.type === "json") {
      value = jsonPathValue(JSON.parse(text), extra.jsonpath);
      const segments = jsonPathSegments(extra.jsonpath);
      if (segments.length === 2 && DEPENDENCY_FIELDS.includes(segments[0])) {
        if (!coveredPins.has(file)) coveredPins.set(file, new Set());
        coveredPins.get(file).add(`${segments[0]}['${segments[1]}']`);
      }
    } else if (extra.type === "toml") {
      value = tomlValue(text, extra.jsonpath);
    } else if (extra.type === "generic") {
      const line = text.split("\n").find((l) => l.includes("x-release-please-version"));
      value = line?.match(/\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?/)?.[0];
    } else {
      problems.push(`${file}: unsupported extra-files type ${extra.type}`);
      continue;
    }
    const where = extra.jsonpath ? `${file} ${extra.jsonpath}` : file;
    if (value === undefined) problems.push(`${where}: not found`);
    else if (value !== version) problems.push(`${where}: ${value}, expected ${version}`);
  }

  // Internal pins are exact, and each one is kept current by release-please.
  for (const extra of config["extra-files"] ?? []) {
    if (extra.type !== "json" || extra.jsonpath !== "$.version") continue;
    if (!extra.path.endsWith("package.json")) continue;
    const document = readJson(root, extra.path);
    internalPins(document, extra.path, version, problems, coveredPins.get(extra.path) ?? new Set());
  }
  for (const [path, entry] of Object.entries(rootLock.packages ?? {})) {
    if (path === "" || path.includes("node_modules/")) continue;
    if (!entry.name?.startsWith(INTERNAL_SCOPE)) continue;
    if (entry.version !== version) {
      problems.push(`package-lock.json: ${path} is ${entry.version}, expected ${version}`);
    }
    internalPins(entry, `package-lock.json ${path}`, version, problems);
  }

  // Lockfiles owned by this script.
  const locks = lockfiles(root);
  for (const file of locks.cargo) {
    for (const entry of cargoLock(readFileSync(join(root, file), "utf8"), version).found) {
      if (entry.version !== version) {
        problems.push(`${file}: ${entry.name} is ${entry.version}, expected ${version}`);
      }
    }
  }
  for (const file of locks.dart) {
    for (const entry of pubspecLock(readFileSync(join(root, file), "utf8"), version).found) {
      if (entry.version !== version) {
        problems.push(`${file}: axton is ${entry.version}, expected ${version}`);
      }
    }
  }
  return problems;
}

/** Rewrites the lockfile entries owned by this script to V; returns changed files. */
export function sync(root) {
  const version = releaseVersion(root);
  if (!isSemver(version)) throw new Error(`"${version}" is not a valid SemVer version`);
  const changed = [];
  const locks = lockfiles(root);
  for (const [files, update] of [
    [locks.cargo, cargoLock],
    [locks.dart, pubspecLock],
  ]) {
    for (const file of files) {
      const text = readFileSync(join(root, file), "utf8");
      const { updated } = update(text, version);
      if (updated !== text) {
        writeFileSync(join(root, file), updated);
        changed.push(file);
      }
    }
  }
  return changed;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [command, ...rest] = process.argv.slice(2);
  const rootIndex = rest.indexOf("--root");
  const root =
    rootIndex >= 0 ? rest[rootIndex + 1] : fileURLToPath(new URL("../..", import.meta.url));
  if (command === "check") {
    const problems = check(root);
    for (const problem of problems) console.error(problem);
    if (problems.length > 0) process.exit(1);
    console.log(`version ${releaseVersion(root)} is consistent`);
  } else if (command === "sync") {
    for (const file of sync(root)) console.log(`updated ${file}`);
  } else {
    console.error("usage: node scripts/release/version.mjs check|sync [--root DIR]");
    process.exit(2);
  }
}
