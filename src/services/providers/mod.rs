//! Service provider implementations.
//!
//! This module contains implementations for blockchain service providers:
//! - WhatsOnChain - Primary UTXO and transaction data provider
//! - ARC - Transaction broadcasting service (TAAL, GorillaPool)
//! - Bitails - Alternative merkle proof provider
//! - BHS - Block Header Service for header lookups and merkle root validation

pub mod arc;
pub mod bhs;
pub mod bitails;
pub mod whatsonchain;

pub use arc::{Arc, ArcConfig};
pub use bhs::{BhsConfig, BlockHeaderService};
pub use bitails::{Bitails, BitailsConfig};
pub use whatsonchain::{WhatsOnChain, WhatsOnChainConfig};
