//! Service provider implementations.
//!
//! This module contains implementations for blockchain service providers:
//! - WhatsOnChain - Primary UTXO and transaction data provider
//! - ARC - Transaction broadcasting service (TAAL, GorillaPool)
//! - Bitails - Alternative merkle proof provider

pub mod whatsonchain;
pub mod arc;
pub mod bitails;

pub use whatsonchain::{WhatsOnChain, WhatsOnChainConfig};
pub use arc::{Arc, ArcConfig};
pub use bitails::{Bitails, BitailsConfig};
