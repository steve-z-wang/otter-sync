# Writing website guides

The README, guides and documentation website are maintained in English. Keep API identifiers and examples consistent with the implementation. Additional languages can be considered when there is a clear need and a maintenance plan.

## Updating documentation

- Update affected guides and examples in the same PR as a behavior change.
- Preserve working commands, relative links, code examples and explicit platform limitations.
- Use consistent concept names: Model, Record, Identity, Mutation, Handler, Loader, Scope, Publish, Receipt, Stamp, Client, Persistence, Push/Pull and Cursor.
- Describe current behavior and verify it against the implementation. Keep internal design discussions and development history out of user documentation.
- Check local links, heading fragments and Markdown formatting when moving or renaming pages.

## Usage documentation

Organize the website by the part of the application a reader is building: shared Schema, Frontend and Backend, with Getting started first and Contributing last. Keep each topic's usage guides and API reference together in its section. Within those sections, a tutorial should take a reader from a fresh checkout to an observable result, while a reference should let them look up a method directly.

For every application-facing interface, document its purpose, actual signature/options, return value, local versus network behavior, errors and a concrete usage example. Keep language variants together in synchronized TypeScript / Flutter tabs above code examples; explain shared behavior once. Flutter examples use Dart. Keep the [interface index](../../website/docs/api-index.md) current when exports or generated APIs change. Label application-provided functions in snippets, and distinguish shipped APIs from planned integrations.

After building the native libraries and resolving the example/Dart dependencies, run `python3 website/scripts/check_examples.py`. It extracts client and backend snippets from Markdown for TypeScript/Dart typechecking and compiles the schema guide's examples. `scripts/test.sh` includes this check. The real HTTP workflow and the tutorial's offline/online CLI commands are exercised by `integration/e2e/run.sh`.

## Documentation website

Website pages live in `website/docs/`. MkDocs Material builds only that directory; engineering documents and writing conventions remain separate. Edit the page source and the local preview reloads automatically. Add new website pages to `website/mkdocs.yml`. Repository and package READMEs link to these pages instead of duplicating the guides. Use relative Markdown links between website pages and GitHub links for engineering documents and implementation source.

See [website setup](https://github.com/zanminwang/axton/blob/main/website/README.md) for installation, preview, strict builds and automatic Pages deployment after merging to `main` and the manual redeploy option. Avoid independently maintained copies in GitHub Wiki. Website deployment and repository visibility are separate settings.
