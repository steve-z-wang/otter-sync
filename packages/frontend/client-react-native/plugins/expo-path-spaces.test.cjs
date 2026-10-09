const assert = require('node:assert/strict');
const { test } = require('node:test');
const { patchPodfile, patchBundlePhase, patchXcodeProject } = require('./expo-path-spaces.cjs');

test('adds an idempotent post-install correction for the Expo Constants script', () => {
  const podfile = `post_install do |installer|\n  react_native_post_install(installer)\nend\n`;
  const patched = patchPodfile(podfile);
  assert.match(patched, /Generate app\.config for prebuilt Constants\.manifest/);
  assert.match(patched, /phase\.shell_script = 'bash -l -c/);
  assert.equal(patchPodfile(patched), patched);
});

test('quotes the bundle phase script path idempotently', () => {
  const resolver = "require('path').dirname(require.resolve('react-native/package.json')) + '/scripts/react-native-xcode.sh'";
  const script = `"export BUNDLE_COMMAND=\\"export:embed\\"\\n\\n\`\\"$NODE_BINARY\\" --print \\"${resolver}\\"\`\\n"`;
  const patched = patchBundlePhase(script);
  assert.equal(patched, `"export BUNDLE_COMMAND=\\"export:embed\\"\\n\\n\\"$(\\"$NODE_BINARY\\" --print \\"${resolver}\\")\\"\\n"`);
  assert.equal(patchBundlePhase(patched), patched);
  const project = { hash: { project: { objects: { PBXShellScriptBuildPhase: { A: { shellScript: script }, B_comment: 'Bundle' } } } } };
  patchXcodeProject(project);
  assert.equal(project.hash.project.objects.PBXShellScriptBuildPhase.A.shellScript, patched);
});
