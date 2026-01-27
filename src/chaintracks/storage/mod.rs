//! Chaintracks storage backends
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Storage/`

mod memory;
// mod sqlite;  // TODO: Implement SQLite backend

pub use memory::*;
