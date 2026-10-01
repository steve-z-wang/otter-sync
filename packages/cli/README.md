# @axtonjs/cli

> **Alpha.** AXTON is alpha software: its API is unstable and it is not ready for production use.

The `axton` schema compiler as a prebuilt executable. It generates the TypeScript and Dart APIs for your Models, Mutations and Queries:

```sh
npx axton compile models generated
npx axton --version
```

Generated TypeScript imports `@axtonjs/server` and `@axtonjs/client`; `--backend-runtime` and `--client-runtime` override those import specifiers.

Each supported target has its own package, `@axtonjs/cli-<target>`, which npm installs as an optional dependency on matching hosts: `darwin-arm64` and `linux-x64-gnu`. On any other host `axton` exits with an unsupported-platform error; the compiler is never built from source. See the [schema reference](https://github.com/zanminwang/axton/blob/main/website/docs/schema/reference.md).
