//! Chaintracks storage backends
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Storage/`

mod memory;
#[cfg(any(feature = "sqlite", feature = "mysql"))]
mod sqlite;

pub use memory::*;
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub use sqlite::SqliteStorage;
