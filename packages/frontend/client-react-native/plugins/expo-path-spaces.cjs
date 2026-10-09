const fs = require('node:fs');
const path = require('node:path');
// Resolved from the consuming app (Expo CLI runs with the app as working directory), since this
// file lives in the SDK package and has no node_modules of its own.
function configPlugins() {
  return require(require.resolve('expo/config-plugins', { paths: [process.cwd(), __dirname] }));
}

// Expo 57's generated scripts execute unquoted paths in two places, which
// breaks when the repository path contains spaces. Both corrections are
// idempotent and are re-applied on every prebuild.

const marker = '  # AXTON: quote Expo Constants script paths that may contain spaces.\n';
const correction = `${marker}  installer.pods_project.targets.each do |target|\n` +
  `    next unless target.name == 'EXConstants'\n` +
  `    target.shell_script_build_phases.each do |phase|\n` +
  `      next unless phase.name == '[CP-User] Generate app.config for prebuilt Constants.manifest'\n` +
  `      phase.shell_script = 'bash -l -c "\\\"$PODS_TARGET_SRCROOT/../scripts/get-app-config-ios.sh\\\""'\n` +
  `    end\n` +
  `  end\n`;

function patchPodfile(contents) {
  if (contents.includes(marker)) {
    return contents;
  }
  const hook = 'post_install do |installer|\n';
  if (!contents.includes(hook)) {
    throw new Error('Expo Podfile has no post_install hook');
  }
  return contents.replace(hook, hook + correction);
}

// The "Bundle React Native code and images" phase runs the script path that
// `node --print` produces through an unquoted backtick substitution.
const bundleResolver =
  "require('path').dirname(require.resolve('react-native/package.json')) + '/scripts/react-native-xcode.sh'";
const bundleUnquoted = `\`\\"$NODE_BINARY\\" --print \\"${bundleResolver}\\"\``;
const bundleQuoted = `\\"$(\\"$NODE_BINARY\\" --print \\"${bundleResolver}\\")\\"`;

function patchBundlePhase(shellScript) {
  return shellScript.split(bundleUnquoted).join(bundleQuoted);
}

function patchXcodeProject(project) {
  const phases = project.hash.project.objects.PBXShellScriptBuildPhase ?? {};
  for (const phase of Object.values(phases)) {
    if (phase && typeof phase.shellScript === 'string') {
      phase.shellScript = patchBundlePhase(phase.shellScript);
    }
  }
  return project;
}

module.exports = function withExpoPathSpaces(config) {
  const { withDangerousMod, withXcodeProject } = configPlugins();
  config = withDangerousMod(config, ['ios', async modConfig => {
    const podfile = path.join(modConfig.modRequest.platformProjectRoot, 'Podfile');
    fs.writeFileSync(podfile, patchPodfile(fs.readFileSync(podfile, 'utf8')));
    return modConfig;
  }]);
  return withXcodeProject(config, modConfig => {
    patchXcodeProject(modConfig.modResults);
    return modConfig;
  });
};

module.exports.patchPodfile = patchPodfile;
module.exports.patchBundlePhase = patchBundlePhase;
module.exports.patchXcodeProject = patchXcodeProject;
