# Changelog

## [0.5.2](https://github.com/zanminwang/axton/compare/v0.5.1...v0.5.2) (2026-10-08)


### Bug Fixes

* **client:** let lifecycle dependencies subsume sequence edges ([#257](https://github.com/zanminwang/axton/issues/257)) ([930968a](https://github.com/zanminwang/axton/commit/930968a4da12015fe6f4e69924eb65bf4d76280e))
* isolate unsupported protocol 5 Mutation versions ([#262](https://github.com/zanminwang/axton/issues/262)) ([0ee02eb](https://github.com/zanminwang/axton/commit/0ee02eb95563bb597b9168b33ffed5fdf2a4c842))
* retire completed materialization owners before the next batch ([#261](https://github.com/zanminwang/axton/issues/261)) ([227505c](https://github.com/zanminwang/axton/commit/227505ce4349e5620dd443580c5648203a5f7cd4))

## [0.5.1](https://github.com/zanminwang/axton/compare/v0.5.0...v0.5.1) (2026-10-08)


### Bug Fixes

* **client:** publish retained observers after Store worker commits ([#250](https://github.com/zanminwang/axton/issues/250)) ([9e44bc9](https://github.com/zanminwang/axton/commit/9e44bc9f72d69fbf7cb4af9b017a47bdcd0669ef))
* **client:** retain Batch ownership through local settlement ([#252](https://github.com/zanminwang/axton/issues/252)) ([6544fca](https://github.com/zanminwang/axton/commit/6544fca1d82c8cfbedc60f0eca0e418216c4ce6b))

## [0.5.0](https://github.com/zanminwang/axton/compare/v0.4.2...v0.5.0) (2026-10-08)


### ⚠ BREAKING CHANGES

* simplify AXTON to the Rust-owned protocol-5 engine

### Features

* simplify AXTON to the Rust-owned protocol-5 engine ([5443978](https://github.com/zanminwang/axton/commit/54439785eeb8013b219f8d7071a5670a8bbeb375))

## [0.4.2](https://github.com/zanminwang/axton/compare/v0.4.1...v0.4.2) (2026-10-07)


### Bug Fixes

* **server:** drain admitted work before closing listener ([#245](https://github.com/zanminwang/axton/issues/245)) ([8b1bd56](https://github.com/zanminwang/axton/commit/8b1bd56531242e8e5a84095be5c9663174a0038b))

## [0.4.1](https://github.com/zanminwang/axton/compare/v0.4.0...v0.4.1) (2026-10-07)


### Bug Fixes

* correct Delta handoff, Live close and Dart rejection ([#243](https://github.com/zanminwang/axton/issues/243)) ([d677126](https://github.com/zanminwang/axton/commit/d677126ec6de1885dbdeebbb932dcf886805566b))

## [0.4.0](https://github.com/zanminwang/axton/compare/v0.3.0...v0.4.0) (2026-10-06)


### Features

* introduce protocol 4 bound Stores (SYN-5) ([#235](https://github.com/zanminwang/axton/issues/235)) ([3935c8f](https://github.com/zanminwang/axton/commit/3935c8fe66df282362cd9958cefddcb58b029510))


### Bug Fixes

* preserve read outcomes and reject partial Query tracking ([#237](https://github.com/zanminwang/axton/issues/237)) ([2fcca1a](https://github.com/zanminwang/axton/commit/2fcca1a1139cbdd516df08ffbf31ee6e25533e2e))
* release Query once flights when preparation fails ([#238](https://github.com/zanminwang/axton/issues/238)) ([7a03be3](https://github.com/zanminwang/axton/commit/7a03be31aa083f37bd6fa0f365c4bad423aec525))


### Miscellaneous Chores

* release 0.4.0 ([6d7d9ae](https://github.com/zanminwang/axton/commit/6d7d9aec6d6d2e2761e5a6a67581511e67587343))

## [0.3.0](https://github.com/zanminwang/axton/compare/v0.2.0...v0.3.0) (2026-10-02)


### ⚠ BREAKING CHANGES

* deliver model authority without local stream ownership ([#228](https://github.com/zanminwang/axton/issues/228))
* replace Channel with Scope membership APIs ([#225](https://github.com/zanminwang/axton/issues/225))

### Upgrade notes

Upgrade the server, PostgreSQL adapter, generated tooling and JS/Dart runtimes together: requests and live negotiation require `stream-authority-v1`. Devices keep one Model/identity/stamp across Streams without a local holding ledger. Unsubscribe and historical Remove retain Models; newer stamped viewer Loader null supplies canonical absence. Applications own business cache reclamation through existing `onStore` hooks and transactions.

Stop old writers and live sessions, apply the appropriate prior layout upgrades and current DDL, run `packages/postgres/migrations/2026-10-01-local-authority.sql`, deploy coordinated authority-capable admission/runtimes, then resume traffic. Never reset cursors or reinterpret Remove as null. See the [deployment cutover](https://github.com/zanminwang/axton/blob/v0.3.0/website/docs/backend/deployment.md#stream-membership-cutover).

### Features

* deliver model authority without local stream ownership ([#228](https://github.com/zanminwang/axton/issues/228)) ([e0ff93e](https://github.com/zanminwang/axton/commit/e0ff93ed9a9ffa3b6f06fd2064541f2e687c4e6c))
* replace Channel with Scope membership APIs ([#225](https://github.com/zanminwang/axton/issues/225)) ([66232e5](https://github.com/zanminwang/axton/commit/66232e525648188e8a8986870ea7a272741029a9))
* replace delivery scopes and tags with Stream tracking ([#227](https://github.com/zanminwang/axton/issues/227)) ([134294d](https://github.com/zanminwang/axton/commit/134294d0e7b0d6161e9c9a8dfed85eee02ca1d1b))

## [0.2.0](https://github.com/zanminwang/axton/compare/v0.1.2...v0.2.0) (2026-09-30)


### ⚠ BREAKING CHANGES

* add tagged Channel membership and synchronized removal

### Features

* add tagged Channel membership and synchronized removal ([bcd5741](https://github.com/zanminwang/axton/commit/bcd5741000b564e092be3c6541a23f3d28d7f95f))


### Bug Fixes

* stabilize the Load COMMIT failure fixture ([#224](https://github.com/zanminwang/axton/issues/224)) ([91942b3](https://github.com/zanminwang/axton/commit/91942b362555539a886accb43328d8ffe6b433d9))

## [0.1.2](https://github.com/zanminwang/axton/compare/v0.1.1...v0.1.2) (2026-09-30)


### Features

* Load handlers enroll loaded records into Channels ([#214](https://github.com/zanminwang/axton/issues/214)) ([#219](https://github.com/zanminwang/axton/issues/219)) ([cf3ef7b](https://github.com/zanminwang/axton/commit/cf3ef7b9693b7dc2cf8a51c7e100df4064d992ae))

## [0.1.1](https://github.com/zanminwang/axton/compare/v0.1.0...v0.1.1) (2026-09-29)


### Bug Fixes

* publish the Dart package's native manifest from lib/ and npm tarballs by path ([#213](https://github.com/zanminwang/axton/issues/213)) ([#216](https://github.com/zanminwang/axton/issues/216)) ([f394be1](https://github.com/zanminwang/axton/commit/f394be11c94470fb19885e1fb01fb3eb05c053cd))

## 0.1.0

First alpha release: the `axton` compiler, the `@axtonjs` Node SDKs and the `axton` Dart client, with prebuilt native code. The API is unstable and not for production use.
