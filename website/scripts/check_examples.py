"""Typecheck client snippets directly from the maintained Markdown sources.

Snippets fenced with title="action-contract" are checked against the
Mutation and Query fixture in integration/action-contract; the others
against the round-trip Entry fixture. The Bootstrap guide is its own fixture: its
schema snippet is compiled, and its TypeScript and Dart snippets are checked
against what that schema generates.

Run after scripts/build.sh, npm ci at the root, and Dart package resolution.
The real HTTP/SQLite behavior is covered by integration/e2e/run.sh.
"""
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap

ROOT = Path(__file__).resolve().parents[2]
SOURCES = {
    'ts': ['website/docs/frontend/client-api.md', 'website/docs/frontend/runtime.md',
           'website/docs/frontend/setup.md', 'website/docs/frontend/sync.md'],
    'dart': ['website/docs/frontend/client-api.md', 'website/docs/frontend/runtime.md',
             'website/docs/frontend/setup.md', 'website/docs/frontend/sync.md'],
}
BACKEND_SOURCES = ['website/docs/backend/api.md', 'website/docs/backend/database.md',
                   'website/docs/backend/setup.md']
LOAD_GUIDE = 'website/docs/frontend/loads.md'
TSC_FLAGS = ['--noEmit', '--strict', '--exactOptionalPropertyTypes', '--skipLibCheck', '--target', 'ES2022',
             '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--allowImportingTsExtensions']


def snippets(language, sources=None, *, context='ordinary'):
    result = []
    fences = r'(?:ts|typescript)' if language == 'ts' else re.escape(language)
    for source in sources if sources is not None else SOURCES[language]:
        text = (ROOT / source).read_text()
        pattern = r'^(?P<indent> *)```' + fences + r'(?P<meta>[^\n]*)\n(?P<code>.*?)^(?P=indent)```'
        for match in re.finditer(pattern, text, re.M | re.S):
            meta = match['meta'].strip()
            actual_context = 'operation' if meta == 'title="action-contract"' else 'v05' if meta == 'title="v05-sdk"' else 'ordinary'
            if actual_context != context:
                continue
            code = re.sub(r'^import .*?;\n', '', textwrap.dedent(match['code']), flags=re.M | re.S)
            line = text[:match.start()].count('\n') + 1
            result.append((f'{source}:{line}', code))
    return result


def check_load_guide():
    """Compile the Bootstrap guide's schema, then check its client and backend
    TypeScript and its Dart against the generated code."""
    (source, schema), = snippets('model', [LOAD_GUIDE])
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'packages/dart') as temp:
        directory = Path(temp)
        (directory / 'schema.model').write_text(schema)
        subprocess.run([str(ROOT / 'target/debug/axton'), 'compile', str(directory), str(directory / 'generated'),
                        '--backend-runtime', '../../../server/index.mts',
                        '--client-runtime', '../../../client-js/index.mts'], cwd=ROOT, check=True)
        print(f'Compiled schema from {source}')
        ts = directory / 'examples.mts'
        ts.write_text('''import type { GeneratedClient } from './generated/client.ts';
type Tx = unknown;
declare const client: GeneratedClient;
declare function readTodoIds(tx: Tx, userId: string, projectId: string, after: string | null, limit: number): Promise<string[]>;
''' + '\n'.join(f'// {source}\nasync function example{i}() {{\n{code.replace("export const", "const")}\n}}'
                  for i, (source, code) in enumerate(snippets('ts', [LOAD_GUIDE]))))
        subprocess.run([str(ROOT / 'node_modules/.bin/tsc'), *TSC_FLAGS, str(ts)], cwd=ROOT, check=True)
        dart = directory / 'examples.dart'
        dart.write_text('''// ignore_for_file: unused_local_variable
import 'generated/generated.dart';
late GeneratedClient client;
''' + '\n'.join(f'// {source}\nFuture<void> example{i}() async {{\n{code}\n}}'
                  for i, (source, code) in enumerate(snippets('dart', [LOAD_GUIDE]))))
        subprocess.run(['dart', 'analyze', str(dart)], cwd=ROOT / 'packages/dart', check=True)
    print(f"Typechecked {len(snippets('ts', [LOAD_GUIDE]))} TypeScript and {len(snippets('dart', [LOAD_GUIDE]))} Dart Bootstrap guide snippets.")


def check():
    # Keep temporary sources within package ancestry so module resolution uses
    # the same installed dependencies and Dart package config as the fixture.
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'packages/dart') as temp:
        directory = Path(temp)
        ts = directory / 'examples.mts'
        ts.write_text('''import { GeneratedClient, PrerequisiteRetry, schema, AdmissionRefused, type StoreConnection } from '../../../integration/e2e/fixtures/round-trip/generated/client.ts';
import type { Transaction } from '../../client-js/index.mts';
declare const client: GeneratedClient;
declare const backendUrl: string;
declare const viewer: string;
declare const connection: StoreConnection;
declare let accessToken: string;
declare function renewAccessToken(): Promise<string>;
declare function uploadFile(key: unknown): Promise<void>;
declare function render(entries: unknown): void;
declare const rejectionOrdinal: number;
declare const stop: () => void;
''' + '\n'.join(f'// {source}\nasync function example{i}() {{\n{code}\n}}'
                  for i, (source, code) in enumerate(snippets('ts'))))
        subprocess.run([str(ROOT / 'node_modules/.bin/tsc'), '--noEmit', '--strict',
                        '--exactOptionalPropertyTypes', '--skipLibCheck', '--target', 'ES2022',
                        '--module', 'NodeNext', '--moduleResolution', 'NodeNext',
                        '--allowImportingTsExtensions', str(ts)], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'integration/action-contract') as temp:
        operation = Path(temp) / 'examples.mts'
        operation.write_text('''import type { TodoCreate } from '../generated.ts';
import type { GeneratedClient } from '../client.ts';
declare const client: GeneratedClient;
''' + '\n'.join(f'// {source}\nasync function example{i}() {{\n{code}\n}}'
                  for i, (source, code) in enumerate(snippets('ts', context='operation'))))
        subprocess.run([str(ROOT / 'node_modules/.bin/tsc'), '--noEmit', '--strict',
                        '--exactOptionalPropertyTypes', '--skipLibCheck', '--target', 'ES2022',
                        '--module', 'NodeNext', '--moduleResolution', 'NodeNext',
                        '--allowImportingTsExtensions', str(operation)], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'packages/dart') as temp:
        directory = Path(temp)
        dart = directory / 'examples.dart'
        dart.write_text('''// ignore_for_file: unused_local_variable, unused_import
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart' show Client;
import '../../../integration/e2e/fixtures/round-trip/generated/generated.dart';
late GeneratedClient client;
late StreamSubscription<List<Entry>> subscription;
late HttpClient http;
void render(List<Entry> entries) {}
const rejectionOrdinal = 1;
late String accessToken;
late String backendUrl;
late String viewer;
late Directory applicationSupportDirectory;
late StoreConnection connection;
Future<String> renewAccessToken() async => '';
Future<void> uploadFile(dynamic key) async {}
''' + '\n'.join(f'// {source}\nFuture<void> example{i}() async {{\n{code}\n}}'
                  for i, (source, code) in enumerate(snippets('dart'))))
        subprocess.run(['dart', 'analyze', str(dart)], cwd=ROOT / 'packages/dart', check=True)
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'integration/action-contract') as temp:
        dart = Path(temp) / 'examples.dart'
        dart.write_text('''// ignore_for_file: unused_local_variable
import '../generated.dart';
late GeneratedClient client;
''' + '\n'.join(f'// {source}\nFuture<void> example{i}() async {{\n{code}\n}}'
                  for i, (source, code) in enumerate(snippets('dart', context='operation'))))
        subprocess.run(['dart', 'analyze', str(dart)], cwd=ROOT / 'integration/action-contract', check=True)
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'integration/e2e/fixtures/round-trip') as temp:
        backend = Path(temp) / 'backend.mts'
        backend.write_text('''import { PrismaClient, type Prisma } from '@prisma/client';
import { createBackend, Entry, type Options } from '../generated/backend.ts';
import { prisma } from '../../../../../packages/postgres/index.mts';
declare const db: PrismaClient;
declare const backend: ReturnType<typeof createBackend<Prisma.TransactionClient>>;
''' + '\n'.join(f'// {source}\nasync function example{i}() {{\n{code.replace("export const", "const")}\n}}'
                  for i, (source, code) in enumerate(snippets('ts', BACKEND_SOURCES))))
        subprocess.run([str(ROOT / 'node_modules/.bin/tsc'), '--noEmit', '--strict',
                        '--exactOptionalPropertyTypes', '--skipLibCheck', '--target', 'ES2022',
                        '--module', 'NodeNext', '--moduleResolution', 'NodeNext',
                        '--allowImportingTsExtensions', str(backend)], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'integration/action-contract') as temp:
        backend = Path(temp) / 'backend.mts'
        backend.write_text('''import { Todo, CallRejected, createBackend, devAuth, type MutationContext, type Mutations, type Queries, type Loaders, type TodoIdentity } from '../backend.ts';
import type { Database } from '../../../packages/server/index.mts';
type Tx = unknown;
declare const database: Database<Tx>;
declare const mutations: Mutations<Tx>;
declare const queries: Queries<Tx>;
declare const loaders: Loaders<Tx>;
declare function saveTodo(tx: Tx, todo: unknown): Promise<void>;
declare function searchTodos(tx: Tx, userId: string, text: string, cursor: string | null): Promise<{ ids: string[]; next: string | null }>;
declare function loadVisibleTodo(tx: Tx, userId: string, id: TodoIdentity): Promise<Todo | null>;
''' + '\n'.join(f'// {source}\nasync function example{i}() {{\n{code}\n}}'
                  for i, (source, code) in enumerate(snippets('ts', BACKEND_SOURCES, context='operation'))))
        subprocess.run([str(ROOT / 'node_modules/.bin/tsc'), '--noEmit', '--strict',
                        '--exactOptionalPropertyTypes', '--skipLibCheck', '--target', 'ES2022',
                        '--module', 'NodeNext', '--moduleResolution', 'NodeNext',
                        '--allowImportingTsExtensions', str(backend)], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix='axton-docs-schema-') as temp:
        for i, (source, code) in enumerate(snippets('text', ['website/docs/schema/define.md'])):
            directory = Path(temp) / str(i)
            directory.mkdir()
            (directory / 'example.model').write_text(code)
            subprocess.run([str(ROOT / 'target/debug/axton'), 'compile', str(directory),
                            str(directory / 'generated')], cwd=ROOT, check=True)
            print(f'Compiled schema from {source}')
    with tempfile.TemporaryDirectory(prefix='.docs-check-', dir=ROOT / 'integration/v05-sdk') as temp:
        directory = Path(temp)
        ts = directory / 'examples.mts'
        ts.write_text("import type { GeneratedClient } from '../client.ts';\ndeclare const client: GeneratedClient;\n" + '\n'.join(f'// {source}\nasync function example{i}() {{\n{code}\n}}' for i, (source, code) in enumerate(snippets('ts', context='v05'))))
        subprocess.run([str(ROOT / 'node_modules/.bin/tsc'), *TSC_FLAGS, str(ts)], cwd=ROOT, check=True)
        dart = directory / 'examples.dart'
        dart.write_text("// ignore_for_file: unused_local_variable\nimport '../generated.dart';\nlate GeneratedClient client;\n" + '\n'.join(f'// {source}\nFuture<void> example{i}() async {{\n{code}\n}}' for i, (source, code) in enumerate(snippets('dart', context='v05'))))
        subprocess.run(['dart', 'analyze', str(dart)], cwd=ROOT / 'packages/dart', check=True)
    check_load_guide()
    print(f"Typechecked {len(snippets('ts')) + len(snippets('ts', BACKEND_SOURCES))} TypeScript, {len(snippets('ts', context='operation')) + len(snippets('ts', BACKEND_SOURCES, context='operation'))} Mutation/Query TypeScript, {len(snippets('dart'))} Dart and {len(snippets('dart', context='operation'))} Mutation/Query Dart documentation snippets.")


if __name__ == '__main__':
    check()
