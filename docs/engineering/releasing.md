# Releasing

Every AXTON artifact of a release carries one version V and is built from one tagged commit: the npm packages `@axtonjs/{native,cli,server,client,postgres}` and their platform packages, the pub.dev package `axton`, and the Dart native libraries its build hook downloads from the GitHub Release. [Release Please](../../.github/workflows/release-please.yml) chooses V and tags it; [Release publish](../../.github/workflows/release-publish.yml) builds, verifies and publishes. The [automated release design](../superpowers/specs/2026-09-29-automated-release-design.md) records why.

## Normal flow

1. Merge changes to `main` with conventional squash titles (`fix:`, `feat:`, `feat!:`). Release Please keeps one release PR open with the next version, `CHANGELOG.md` and every manifest; its `lockfiles` job commits the matching `Cargo.lock` and `pubspec.lock` refresh to that PR.
2. Review the version and changelog, then merge the release PR once its checks pass.
3. Release Please tags `vV` and creates a GitHub prerelease. The tag runs Release publish:

   ```text
   version ─┬─ verify (verify.yml) ───────────────────────────────────────────────┐
            ├─ build-host (darwin-arm64, linux-x64-gnu) ─┐                        │
            ├─ build-ios ────────────────────────────────┼─ pack ─ verify-staged ─┴─ assets ─┬─ publish-npm ──┬─ verify-published ─ complete
            └─ build-android ────────────────────────────┘                                   └─ publish-dart ─┘
   ```

   `assets` attaches the libraries, npm archives, staged Dart package and `release-manifest.json` to the public prerelease. The publish jobs publish those attached bytes: npm under the `alpha` dist-tag, never `latest`; pub.dev through automated publishing. `verify-published` installs V from both registries on clean runners, and `complete` then clears the prerelease flag.
4. The release is complete when its GitHub Release is no longer a prerelease. Finish a release before merging the next release PR.

## Recovery

Open the tag's run and choose **Re-run failed jobs**. Never re-run all jobs: a rebuild produces different bytes, and `assets` stops on any file that differs from the one already attached. Publishing always works from the release's attached files, so a retry never depends on expired Actions artifacts. A registry version is skipped only when its bytes match `release-manifest.json` (npm `dist.integrity`; the files of pub.dev's archive); any other answer, including an unreachable registry, stops the run. A stop means a new version, not a replacement. pub.dev accepts only the tag-push run, so recover by re-running it, never by a manual dispatch.

## GitHub setup

- **Release App.** Create a GitHub App owned by `zanminwang`, with webhooks inactive and these repository permissions: Contents read and write, Pull requests read and write (Metadata read is implied). Install it on `zanminwang/axton` only and generate a private key.
- **Variable and secret.** Under the repository's Actions settings, set the variable `AXTON_RELEASE_APP_CLIENT_ID` to the App's Client ID and the secret `AXTON_RELEASE_APP_PRIVATE_KEY` to the private key. Release Please stays idle until the variable exists. If rulesets protect `main` or `v*` tags, let the App push its release branch and create `v*` tags.
- **Environment.** Create the environment `release`, limited to tags matching `v*`, without required reviewers. Both publish jobs run in it and the registries require it.
- **Releases.** Leave immutable releases off: assets are attached after Release Please publishes the prerelease.

## First release

npm and pub.dev accept trusted publishing only for packages that exist, so the maintainer publishes V's first artifacts by hand, from the bytes this pipeline built. Merge the workflows first; the tag's commit must contain them.

1. Tag the release commit on `main` and push the tag:

   ```sh
   git tag v0.1.0 <commit>
   git push origin v0.1.0
   ```

   The run attaches every artifact to a new public prerelease, then `publish-npm` and `publish-dart` fail with "configure trusted publishing".
2. Outside any git checkout, download and check the release:

   ```sh
   mkdir axton-v0.1.0 && cd axton-v0.1.0
   gh release download v0.1.0 --repo zanminwang/axton
   node <axton checkout>/scripts/release/manifest.mjs verify .
   ```

3. Publish to npm in dependency order after `npm login`:

   ```sh
   npm publish axtonjs-native-darwin-arm64-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-cli-darwin-arm64-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-native-linux-x64-gnu-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-cli-linux-x64-gnu-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-native-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-cli-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-server-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-client-0.1.0.tgz --tag alpha --access public
   npm publish axtonjs-postgres-0.1.0.tgz --tag alpha --access public
   ```

   A package's first version may also become `latest`; record `npm dist-tag ls @axtonjs/server` and leave it.
4. Publish the staged Dart package with the pub.dev Google account:

   ```sh
   mkdir axton && tar -xzf axton-dart-0.1.0.tar.gz -C axton
   dart pub publish --directory axton
   ```

   Then mark `axton` unlisted on its pub.dev admin page.
5. Configure trusted publishing. On npmjs.com, for each of the nine packages, add a GitHub Actions trusted publisher: organization or user `zanminwang`, repository `axton`, workflow filename `release-publish.yml`, environment `release`, allowing `npm publish`; then set publishing access to require two-factor authentication and disallow tokens. On pub.dev, under `axton`'s admin page, enable automated publishing from GitHub Actions: repository `zanminwang/axton`, tag pattern `v{{version}}`, required environment `release`.
6. Re-run the failed jobs of the `v0.1.0` run. The publish jobs find the matching versions and skip them, `verify-published` installs them from the registries, and `complete` finishes the release.
7. Set up the release App last, so Release Please starts from the `v0.1.0` release. The next release PR merge proves the unattended flow.
