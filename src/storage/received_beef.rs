//! The one check of a received AtomicBEEF, called by both doors of
//! `internalizeAction`: the wallet's (when its storage does not say its own
//! door checks in the wallet's process,
//! `WalletStorageProvider::checks_received_beef_in_process`) and a local
//! store's. The same steps in the same order with the same texts:
//!
//! 1. the bytes, through bsv-rs's streaming reader ([`refuse_invalid_beef_bytes`]:
//!    `Invalid BEEF at byte <n>: <kind>`);
//! 2. the parse (`Failed to parse AtomicBEEF: ...`) and the subject
//!    (`BEEF is not AtomicBEEF (missing atomic_txid)`);
//! 3. the structure, with its roots (`BEEF structure is invalid`);
//! 4. the caller resolves its chain tracker ([`NO_CHAIN_TRACKER_REFUSAL`]
//!    when it has none);
//! 5. every transaction whole ([`ReceivedBeef::whole_roots`]: an entry the
//!    BEEF names by its id without its bytes is refused);
//! 6. every root against the tracker ([`check_roots`]).
//!
//! The reference's wallet and storage each verify a received BEEF
//! (`ab.verify(chainTracker, false)`); here it is checked once, at the
//! door the wallet's process holds. This module compiles in every feature
//! set (a wallet on a storage client builds without `sqlite`).

use std::collections::HashMap;

use bsv_rs::transaction::{Beef, BeefStream, ChainTracker};

use crate::error::{Error, Result};

/// The refusal of an AtomicBEEF when nothing at hand has a chain tracker to
/// verify its merkle roots (one sentence at both doors).
pub(crate) const NO_CHAIN_TRACKER_REFUSAL: &str =
    "No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker";

/// Reads a stranger's BEEF through bsv-rs's streaming reader, one element at
/// a time, and refuses it only for invalid bytes: [`Error::InvalidBeef`] with
/// the offset of the byte and the kind (a bad varint, a cut field, a tree
/// height over 64, a transaction with no input, trailing bytes, and the rest
/// of the reader's twenty-four). Nothing is refused for its size or its counts.
///
/// The frame and each element are checked; the graph (an input naming an
/// earlier transaction) and the roots against the headers are not: those
/// stay with the caller's own verification.
pub fn refuse_invalid_beef_bytes(bytes: &[u8]) -> Result<()> {
    let mut stream = BeefStream::new(bytes);
    loop {
        match stream.next_element() {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(bsv_rs::transaction::beef_stream::StreamError::Refused(r)) => {
                return Err(Error::InvalidBeef {
                    offset: r.offset,
                    kind: r.kind(),
                })
            }
            Err(bsv_rs::transaction::beef_stream::StreamError::Io(e)) => {
                return Err(Error::IoError(e))
            }
        }
    }
}

/// A received AtomicBEEF whose bytes, subject and structure are checked
/// (steps 1 to 3), with the roots its structure proves.
pub(crate) struct ReceivedBeef {
    /// The BEEF as read (its structure checked).
    pub(crate) beef: Beef,
    /// The AtomicBEEF's subject txid (the storage door reads it).
    #[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
    pub(crate) txid: String,
    roots: HashMap<u32, String>,
}

/// Steps 1 to 3: the bytes, the parse and the subject, the structure.
pub(crate) fn read_received_beef(bytes: &[u8]) -> Result<ReceivedBeef> {
    refuse_invalid_beef_bytes(bytes)?;
    let mut beef = Beef::from_binary(bytes)
        .map_err(|e| Error::ValidationError(format!("Failed to parse AtomicBEEF: {}", e)))?;
    let txid = beef.atomic_txid.clone().ok_or_else(|| {
        Error::ValidationError("BEEF is not AtomicBEEF (missing atomic_txid)".to_string())
    })?;
    // bsv-rs's `verify_valid` (since 0.8.11) reads the structure alone
    // (its flag decides nothing): a txid-only entry no BUMP
    // proves is refused here, one a BUMP proves passes to step 5.
    let validation = beef.verify_valid(true);
    if !validation.valid {
        return Err(Error::ValidationError(
            "BEEF structure is invalid".to_string(),
        ));
    }
    Ok(ReceivedBeef {
        beef,
        txid,
        roots: validation.roots,
    })
}

impl ReceivedBeef {
    /// Step 5: refuses an entry the BEEF names by its id without its bytes
    /// (whether or not a store holds that transaction, the BEEF must carry
    /// it whole), else the roots to check (step 6). Called after the
    /// caller has resolved its tracker.
    pub(crate) fn whole_roots(&self) -> Result<HashMap<u32, String>> {
        if let Some(entry) = self.beef.txs.iter().find(|t| t.is_txid_only()) {
            return Err(Error::ValidationError(format!(
                "The tx parameter must be valid AtomicBEEF: transaction {} is carried by txid alone",
                entry.txid()
            )));
        }
        Ok(self.roots.clone())
    }
}

/// Step 6: every root confirmed by `tracker`; a tracker fault refuses.
pub(crate) async fn check_roots(
    roots: &HashMap<u32, String>,
    tracker: &dyn ChainTracker,
) -> Result<()> {
    for (height, root) in roots {
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
