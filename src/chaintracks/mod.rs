//! Chaintracks - Block Header Tracking System
//!
//! Port of the TypeScript Chaintracks implementation from:
//! `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/`
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
//! - [`BulkIngestor`] - Historical header fetching (CDN, WhatsOnChain)
//! - [`LiveIngestor`] - Real-time header streaming (WebSocket, Polling)
//!
//! ## Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::chaintracks::{Chaintracks, ChaintracksOptions};
//!
//! let options = ChaintracksOptions::default_mainnet();
//! let chaintracks = Chaintracks::new(options).await?;
//! chaintracks.make_available().await?;
//!
//! let tip = chaintracks.find_chain_tip_header().await?;
//! println!("Chain tip: {} at height {}", tip.hash, tip.height);
//! ```

mod types;
mod traits;
mod storage;
mod ingestors;
#[allow(clippy::module_inception)]
mod chaintracks;

pub use types::*;
pub use traits::*;
pub use storage::*;
pub use chaintracks::*;

// Re-export ingestor module for convenience
pub mod ingestor {
    //! Ingestor implementations
    pub use super::ingestors::*;
}

// Also re-export commonly used ingestor types at the top level
pub use ingestors::{
    BulkCdnIngestor,
    BulkCdnOptions,
    BulkWocIngestor,
    BulkWocOptions,
    LivePollingIngestor,
    LivePollingOptions,
    LiveWebSocketIngestor,
    LiveWebSocketOptions,
};

// Re-export ChainTracker from bsv-sdk for convenience
pub use bsv_sdk::transaction::ChainTracker;
