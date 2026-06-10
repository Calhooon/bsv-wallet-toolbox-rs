# Changelog

All notable changes to `bsv-wallet-toolbox-rs` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to semantic versioning (`0.x.y` — the patch slot `y` carries
backward-compatible changes).

## [0.3.39] - 2026-06-10

### Added
- `StorageClient::with_timeout(Duration)` (builder) and
  `StorageClient::set_timeout(Duration)` — configurable per-request RPC timeout
  (default 30s). Raise it for cold-start-prone servers (e.g. Cloudflare Workers
  can exceed 20s on the first hit). Applies to both the Peer send and the
  response wait of every RPC.
- New feature-independent `storage::broadcast` module exposing
  `BroadcastOutcome`, `classify_broadcast_results`, and
  `validate_beef_for_broadcast`. Previously these lived under
  `storage::sqlx` (gated behind `sqlite`/`mysql`); they are now available
  regardless of the storage backend feature. The legacy `storage::sqlx::*`
  re-exports are preserved for backward compatibility.

### Changed
- `--features remote` now builds standalone (without the default `sqlite`
  feature). Broadcast classification was extracted out of the sqlx backend so
  the remote `StorageClient` no longer transitively requires a SQL backend.
  Verified by `cargo check --no-default-features --features remote`.
- Bumped `bsv-rs` dependency `0.3.4` → `0.3.13`.

### Compatibility
- Backward compatible. No public path was removed: `storage::sqlx::BroadcastOutcome`
  (and siblings) still resolve under the `sqlite`/`mysql` features, and the
  crate-root re-export is now strictly wider (feature-independent).
