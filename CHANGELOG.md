# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.8.0] - 2026-08-03
### Changed
- Added the ability for the `pettyCache.fetch` function to support callbacks and promises.
- Added the ability for the `pettyCache.fetchAndRefresh` function to support callbacks and promises, including async cache-miss functions on background refresh.
- Added the ability for the `pettyCache.semaphore` functions to support callbacks and promises, including async `options.size` functions for `retrieveOrCreate`.
- Modernized the internals of `fetch`, `fetchAndRefresh`, `get`, `bulkGet`, `bulkFetch`, and the `semaphore` functions to async/await.
### Fixed
- A cache-miss function passed to `fetch` that throws synchronously no longer leaves the in-process locks for that key held forever.
- Background refresh failures in `fetchAndRefresh` no longer produce unhandled promise rejections when updating cache fails.

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