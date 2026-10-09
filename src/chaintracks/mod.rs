//! Chaintracks - an embedded block header store
//!
//! A port of the storage half of the TypeScript Chaintracks (`@bsv/wallet-toolbox`,
//! `src/services/chaintracker/chaintracks/`).
//!
//! ## Not a source of truth
//!
//! The header service is the source of headers for a wallet: point
//! [`ServicesOptions::chaintracks_url`](crate::services::ServicesOptions) at
//! one. It checks every header's proof of work, the difficulty rule, the
//! checkpoints and its ancestry before it stores it.
//!
//! This embedded store checks none of those. Until 0.5.0 it shipped four
//! ingestors that filled it from a third-party explorer's REST and
//! websocket routes and from a CDN; they are removed (Rule 28, T16 to T19):
//! they re-derived what the header service holds, unchecked. Nothing in
//! this crate feeds the store now. [`BulkIngestor`] and [`LiveIngestor`]
//! remain as the seam for a host that feeds it from a header service it
//! runs (its `getHeaders` route and its event feed); such a host ports the
//! header service's rules ahead of [`ChaintracksManagement::add_header`]
//! before it treats a stored header as verified.
//!
//! ## Architecture
//!
//! Chaintracks uses a two-tier storage system:
//! - **Bulk Storage**: Historical headers (immutable, height-indexed)
//! - **Live Storage**: Recent headers (mutable, tracks forks/reorgs)
//!
//! ## Components
//!
//! - [`Chaintracks`] - Main orchestrator
//! - [`ChaintracksStorage`] - Storage trait with multiple backends
//! - [`BulkIngestor`] - The seam for a historical header source
//! - [`LiveIngestor`] - The seam for a new-header source

#[allow(clippy::module_inception)]
mod chaintracks;
mod storage;
mod traits;
mod types;

pub use chaintracks::*;
pub use storage::*;
pub use traits::*;
pub use types::*;

// Re-export ChainTracker from bsv-sdk for convenience
pub use bsv_rs::transaction::ChainTracker;
