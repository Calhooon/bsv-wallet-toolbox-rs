//! The wallet door's own check of a received AtomicBEEF's merkle roots.
//!
//! The reference checks a received BEEF twice: its wallet's
//! `internalizeAction` verifies it with the wallet's own services' chain
//! tracker before calling storage, and its storage method verifies it again
//! with the storage's tracker. Here the storage door's check (the store's
//! own tracker, else its services') is the one check when the wallet's
//! storage is a local store in the wallet's own process
//! (`WalletStorageProvider::is_storage_provider`); when it is not, this
//! check runs at the wallet door, before storage is called, so that
//! something the wallet's own process holds has checked the roots.
//!
//! This module compiles in every feature set (a wallet on a storage client
//! builds without `sqlite`), so it reads the BEEF with bsv-rs directly and
//! answers with the storage door's refusal texts.

use bsv_rs::transaction::{Beef, ChainTracker};

use crate::error::{Error, Result};

/// The refusal of an AtomicBEEF when nothing at hand has a chain tracker to
/// verify its merkle roots (the storage door's sentence; the wallet door
/// answers it when its services have no tracker).
pub(crate) const NO_CHAIN_TRACKER_REFUSAL: &str =
    "No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker";

/// Verifies `beef_bytes`' merkle roots with `tracker`, as the reference's
/// wallet does (`ab.verify(chainTracker, false)`): the BEEF must be an
/// AtomicBEEF, every transaction it carries must be whole (an entry carried
/// by txid alone is refused: the reference's wallet allows none and asks its
/// storage for nothing), and every root it proves must be one the tracker
/// confirms. Refusals carry the storage door's texts.
pub(crate) async fn verify_received_beef_roots(
    beef_bytes: &[u8],
    tracker: &dyn ChainTracker,
) -> Result<()> {
    let mut beef = Beef::from_binary(beef_bytes)
        .map_err(|e| Error::ValidationError(format!("Failed to parse AtomicBEEF: {}", e)))?;
    if beef.atomic_txid.is_none() {
        return Err(Error::ValidationError(
            "BEEF is not AtomicBEEF (missing atomic_txid)".to_string(),
        ));
    }
    if !beef.verify_valid(true).valid {
        return Err(Error::ValidationError(
            "BEEF structure is invalid".to_string(),
        ));
    }
    if let Some(entry) = beef.txs.iter().find(|t| t.is_txid_only()) {
        return Err(Error::ValidationError(format!(
            "The tx parameter must be valid AtomicBEEF: transaction {} is carried by txid alone",
            entry.txid()
        )));
    }
    let validation = beef.verify_valid(false);
    if !validation.valid {
        return Err(Error::ValidationError(
            "BEEF structure is invalid".to_string(),
        ));
    }
    for (height, root) in &validation.roots {
        let confirmed = tracker
            .is_valid_root_for_height(root, *height)
            .await
            .map_err(|e| {
                Error::ValidationError(format!(
                    "ChainTracker verification failed at height {}: {}",
                    height, e
                ))
            })?;
        if !confirmed {
            return Err(Error::ValidationError(format!(
                "Invalid merkle root {} at height {}",
                root, height
            )));
        }
    }
    Ok(())
}
