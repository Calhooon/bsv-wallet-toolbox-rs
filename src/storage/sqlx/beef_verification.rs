//! BEEF Verification Utilities
//!
//! This module provides functions for verifying BEEF (Background Evaluation
//! Extended Format) merkle proofs against a ChainTracker.

use bsv_sdk::transaction::{Beef, ChainTracker};
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::storage::traits::BeefVerificationMode;

/// Verifies BEEF merkle proofs against a ChainTracker.
///
/// This function validates that all merkle proofs in a BEEF are valid
/// according to the provided ChainTracker. It's used during internalize_action
/// and create_action to ensure incoming transactions have valid proofs.
///
/// # Arguments
///
/// * `beef` - The parsed BEEF structure to verify (mutable for internal validation)
/// * `chain_tracker` - ChainTracker implementation for verification
/// * `mode` - Verification mode controlling strictness
/// * `known_txids` - Set of txids already known to the wallet (for TrustKnown mode)
///
/// # Returns
///
/// * `Ok(true)` - BEEF is valid
/// * `Ok(false)` - BEEF has no proofs to verify (e.g., unproven transactions)
/// * `Err(_)` - BEEF verification failed
///
/// # Example
///
/// ```rust,ignore
/// let mut beef = Beef::from_binary(&bytes)?;
/// let is_valid = verify_beef_merkle_proofs(
///     &mut beef,
///     chain_tracker.as_ref(),
///     BeefVerificationMode::Strict,
///     &HashSet::new(),
/// ).await?;
/// ```
pub async fn verify_beef_merkle_proofs(
    beef: &mut Beef,
    chain_tracker: &dyn ChainTracker,
    mode: BeefVerificationMode,
    _known_txids: &std::collections::HashSet<String>,
) -> Result<bool> {
    // If disabled, skip verification
    if mode == BeefVerificationMode::Disabled {
        return Ok(true);
    }

    // If BEEF has no merkle proofs (bumps), nothing to verify
    if beef.bumps.is_empty() {
        return Ok(false);
    }

    // Validate BEEF structure first and extract roots
    // This must be done synchronously before any await points due to RefCell in Beef
    let roots: HashMap<u32, String> = {
        let validation = beef.verify_valid(true);
        if !validation.valid {
            return Err(Error::ValidationError(
                "BEEF structure is invalid".to_string(),
            ));
        }
        validation.roots
    };

    // Verify each merkle root against the chain
    for (height, root) in &roots {
        // In TrustKnown mode, we could skip verification for known txids
        // However, the roots don't directly map to txids, so we verify all roots
        // The TrustKnown mode is more useful at the transaction level

        let is_valid = chain_tracker
            .is_valid_root_for_height(root, *height)
            .await
            .map_err(|e| {
                Error::ValidationError(format!(
                    "ChainTracker verification failed at height {}: {}",
                    height, e
                ))
            })?;

        if !is_valid {
            return Err(Error::ValidationError(format!(
                "Invalid merkle root {} at height {}",
                root, height
            )));
        }
    }

    Ok(true)
}

/// Verifies a single transaction's merkle proof against the chain.
///
/// This is a convenience function for verifying a specific txid within a BEEF.
///
/// # Arguments
///
/// * `beef` - The BEEF containing the transaction
/// * `txid` - The transaction ID to verify
/// * `chain_tracker` - ChainTracker implementation
///
/// # Returns
///
/// * `Ok(true)` - Transaction has a valid merkle proof
/// * `Ok(false)` - Transaction has no merkle proof in BEEF
/// * `Err(_)` - Verification failed
pub async fn verify_txid_merkle_proof(
    beef: &Beef,
    txid: &str,
    chain_tracker: &dyn ChainTracker,
) -> Result<bool> {
    // Find the BUMP (merkle proof) for this txid and extract data
    // This must be done synchronously before any await points due to RefCell in Beef
    let (block_height, computed_root) = {
        let bump = match beef.find_bump(txid) {
            Some(b) => b,
            None => return Ok(false), // No proof for this txid
        };

        // Get the block height
        let height = bump.block_height;

        // Compute the merkle root from the BUMP
        let root = bump.compute_root(Some(txid)).map_err(|e| {
            Error::ValidationError(format!(
                "Failed to compute merkle root for txid {}: {}",
                txid, e
            ))
        })?;

        (height, root)
    };

    // Verify against the chain (async operation)
    let is_valid = chain_tracker
        .is_valid_root_for_height(&computed_root, block_height)
        .await
        .map_err(|e| {
            Error::ValidationError(format!(
                "ChainTracker verification failed for txid {}: {}",
                txid, e
            ))
        })?;

    if !is_valid {
        return Err(Error::ValidationError(format!(
            "Invalid merkle proof for txid {} at height {}",
            txid, block_height
        )));
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_sdk::transaction::ChainTrackerError;
    use std::collections::HashSet;

    // A mock ChainTracker that always returns true
    struct AlwaysValidChainTracker;

    #[async_trait::async_trait]
    impl ChainTracker for AlwaysValidChainTracker {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, ChainTrackerError> {
            Ok(true)
        }

        async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
            Ok(800000)
        }
    }

    // A mock ChainTracker that always returns false
    struct AlwaysInvalidChainTracker;

    #[async_trait::async_trait]
    impl ChainTracker for AlwaysInvalidChainTracker {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, ChainTrackerError> {
            Ok(false)
        }

        async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
            Ok(800000)
        }
    }

    #[tokio::test]
    async fn test_verify_disabled_mode_skips_verification() {
        // Create an empty BEEF
        let mut beef = Beef::default();
        let tracker = AlwaysInvalidChainTracker;
        let known = HashSet::new();

        // Even with an invalid tracker, Disabled mode should pass
        let result = verify_beef_merkle_proofs(
            &mut beef,
            &tracker,
            BeefVerificationMode::Disabled,
            &known,
        )
        .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_verify_empty_beef_returns_false() {
        let mut beef = Beef::default();
        let tracker = AlwaysValidChainTracker;
        let known = HashSet::new();

        let result = verify_beef_merkle_proofs(
            &mut beef,
            &tracker,
            BeefVerificationMode::Strict,
            &known,
        )
        .await;

        // Empty BEEF has no proofs, returns Ok(false)
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_beef_verification_mode_default() {
        // Default should be Strict
        let mode = BeefVerificationMode::default();
        assert_eq!(mode, BeefVerificationMode::Strict);
    }

    #[test]
    fn test_beef_verification_mode_serialization() {
        let mode = BeefVerificationMode::TrustKnown;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"trustKnown\"");

        let parsed: BeefVerificationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, BeefVerificationMode::TrustKnown);
    }
}
