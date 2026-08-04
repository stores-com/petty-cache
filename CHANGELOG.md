# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.0.0] - 2026-08-03
### Changed
- Every function now supports both callbacks and promises (async/await) — omit the callback to receive a promise. This includes cache-miss functions and `retrieveOrCreate`'s `size` option, which may now be async functions.
- **Breaking:** `fetch`, `fetchAndRefresh`, and the `semaphore` methods `consumeLock`, `expand`, `releaseLock`, `reset`, and `retrieveOrCreate` previously treated a missing callback as fire-and-forget and silently discarded errors. These calls now return a promise, so an error that was previously swallowed becomes an unhandled promise rejection — which terminates the process on Node.js 15 and later — unless the caller awaits the promise or attaches a `.catch()`. Audit callback-less calls to these methods before upgrading.
- Callback support is deprecated and will be removed in v5. Callback-style usage now emits a once-per-process `DeprecationWarning` per function.
- Removed the `async` dependency.
### Fixed
- A cache-miss function that throws synchronously no longer leaves the in-process locks for its key held forever.
- Background refresh failures in `fetchAndRefresh` no longer produce unhandled promise rejections.

## [3.7.0] - 2026-03-12
### Changed
- Added the ability for `pettyCache.bulkGet`, `pettyCache.bulkSet`, and `pettyCache.bulkFetch` functions to support callbacks and promises.

## [3.6.0] - 2026-02-12
### Changed
- Added the ability for `pettyCache.get`, `pettyCache.set`, and `pettyCache.patch` functions to support callbacks and promises.

## [3.5.0] - 2025-05-01
### Changed
- Added the ability for `pettyCache.del` functions to support callbacks and promises.

## [3.4.0] - 2025-02-21
### Changed
- Added the ability for `pettyCache.mutex` functions to support callbacks and promises.

## [3.3.0] - 2024-07-03
### Changed
- Added the ability for `pettyCache.fetch` to support async functions.

## [3.2.0] - 2021-04-05
### Changed
- Upgraded `redis` version to `~3.1.0`.

## [3.1.0] - 2021-02-16
### Added
- Added the ability for `pettyCache.bulkFetch` to specify TTL options.