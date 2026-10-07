import {execFileSync} from 'node:child_process';
import {copyFileSync,renameSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
const directory = fileURLToPath(new URL('.', import.meta.url));
const release=process.argv.includes('--release');
// `--probe` builds the test-only transaction probe into a separate artifact,
// `axton-node-probe.node`; the normal addon never contains it.
const probe=process.argv.includes('--probe');
execFileSync('cargo', ['build', ...(release?['--release']:[]), ...(probe?['--features','probe']:[]), '--locked', '--manifest-path', `${directory}Cargo.toml`], {stdio:'inherit'});
const filename = process.platform === 'darwin' ? 'libaxton_node.dylib' : process.platform === 'win32' ? 'axton_node.dll' : 'libaxton_node.so';
// A fresh inode prevents macOS from reusing a stale native-code signature cache.
const output=`${directory}${probe?'axton-node-probe.node':'axton-node.node'}`;
copyFileSync(`${directory}target/${release?'release':'debug'}/${filename}`, `${output}.tmp`);
renameSync(`${output}.tmp`,output);
