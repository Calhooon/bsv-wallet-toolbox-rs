//! BEEF Verification Utilities
//!
//! This module provides functions for verifying BEEF (Background Evaluation
//! Extended Format) merkle proofs against a ChainTracker.

use bsv_rs::transaction::{Beef, ChainTracker};
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
    use bsv_rs::primitives::{sha256d, to_hex};
    use bsv_rs::transaction::{ChainTrackerError, MerklePath, MockChainTracker, Transaction};
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

    // A mock ChainTracker that returns an error
    struct ErrorChainTracker;

    #[async_trait::async_trait]
    impl ChainTracker for ErrorChainTracker {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, ChainTrackerError> {
            Err(ChainTrackerError::Other(
                "chain tracker unavailable".to_string(),
            ))
        }

        async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
            Err(ChainTrackerError::Other(
                "chain tracker unavailable".to_string(),
            ))
        }
    }

    /// A well-known P2PKH transaction hex (Bitcoin's famous "pizza" transaction style).
    const TEST_TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

    /// Minimal valid coinbase-like transaction (no real inputs).
    const MINIMAL_TX_BYTES: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, // version
        0x01, // input count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // prev txid (32 zero bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff,
        0xff, // vout (0xFFFFFFFF for coinbase)
        0x00, // script length
        0xff, 0xff, 0xff, 0xff, // sequence
        0x01, // output count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // satoshis
        0x00, // script length
        0x00, 0x00, 0x00, 0x00, // locktime
    ];

    /// Compute the txid (double-SHA256, reversed) of raw transaction bytes.
    fn compute_txid(raw_tx: &[u8]) -> String {
        let hash = sha256d(raw_tx);
        let mut reversed = hash;
        reversed.reverse();
        to_hex(&reversed)
    }

    /// Helper: build a BEEF with a single proven transaction using a coinbase-style merkle path.
    /// Returns (beef, txid, merkle_root) where merkle_root == txid for a single-tx block.
    fn build_single_proven_beef(height: u32) -> (Beef, String, String) {
        let raw_tx = MINIMAL_TX_BYTES.to_vec();
        let txid = compute_txid(&raw_tx);

        // For a coinbase-only block, the merkle root is the txid itself
        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let merkle_root = bump.compute_root(Some(&txid)).unwrap();

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(bump);
        beef.merge_raw_tx(raw_tx, Some(bump_index));

        (beef, txid, merkle_root)
    }

    // =========================================================================
    // Existing tests
    // =========================================================================

    #[tokio::test]
    async fn test_verify_disabled_mode_skips_verification() {
        // Create an empty BEEF
        let mut beef = Beef::default();
        let tracker = AlwaysInvalidChainTracker;
        let known = HashSet::new();

        // Even with an invalid tracker, Disabled mode should pass
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Disabled, &known)
                .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_verify_empty_beef_returns_false() {
        let mut beef = Beef::default();
        let tracker = AlwaysValidChainTracker;
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
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

    // =========================================================================
    // New comprehensive BEEF verification tests
    // =========================================================================

    /// Test 1: Valid BEEF with correct merkle proof passes verification.
    #[tokio::test]
    async fn test_valid_beef_happy_path() {
        let height = 800_000u32;
        let (mut beef, txid, merkle_root) = build_single_proven_beef(height);

        // Set up a MockChainTracker that knows about this merkle root
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root.clone());

        let known = HashSet::new();
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        assert!(result.unwrap(), "Expected true for valid BEEF");

        // Also verify the individual txid proof
        let txid_result = verify_txid_merkle_proof(&beef, &txid, &tracker).await;
        assert!(txid_result.is_ok(), "Expected Ok, got: {:?}", txid_result);
        assert!(txid_result.unwrap(), "Expected true for valid txid proof");
    }

    /// Test 2: BEEF where the computed merkle root doesn't match the block header.
    #[tokio::test]
    async fn test_beef_invalid_merkle_root() {
        let height = 800_000u32;
        let (mut beef, txid, _merkle_root) = build_single_proven_beef(height);

        // Set up a MockChainTracker with a DIFFERENT root for this height
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, "ff".repeat(32));

        let known = HashSet::new();
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        // Should fail because the merkle root doesn't match
        assert!(result.is_err(), "Expected error for invalid merkle root");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid merkle root"),
            "Error should mention invalid merkle root, got: {}",
            err_msg
        );

        // Also verify the individual txid proof fails
        let txid_result = verify_txid_merkle_proof(&beef, &txid, &tracker).await;
        assert!(
            txid_result.is_err(),
            "Expected error for invalid txid proof"
        );
    }

    /// Test 3: BEEF that references inputs not included in the BEEF.
    /// When a non-proven tx has an input referencing a txid not in the BEEF,
    /// verify_valid returns invalid structure.
    #[tokio::test]
    async fn test_beef_missing_intermediate_txs() {
        let height = 800_000u32;

        // Build a BEEF with a transaction that has an input pointing to a missing txid
        let tx = Transaction::from_hex(TEST_TX_HEX).unwrap();
        let _txid = tx.id();

        // The transaction references input c997a5...3704 which is NOT in the BEEF
        let mut beef = Beef::new();
        beef.merge_transaction(tx);

        // This BEEF has no BUMPs, so no proofs. The tx has a dangling input reference.
        // verify_beef_merkle_proofs returns Ok(false) because bumps is empty.
        let tracker = AlwaysValidChainTracker;
        let known = HashSet::new();
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        // No BUMPs => returns Ok(false)
        assert!(result.is_ok());
        assert!(!result.unwrap());

        // Now add a bogus BUMP that doesn't match this tx
        let fake_txid = "aa".repeat(32);
        let bump = MerklePath::from_coinbase_txid(&fake_txid, height);
        beef.merge_bump(bump);

        // Now we have a BUMP but the structure is invalid (the tx references
        // missing inputs and has no proof itself)
        let result2 =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        // Should fail because BEEF structure is invalid
        assert!(
            result2.is_err(),
            "Expected error for BEEF with missing intermediate txs"
        );
    }

    /// Test 4: BEEF with no BUMPs should fail verification (returns false).
    #[tokio::test]
    async fn test_beef_empty_bumps() {
        let raw_tx = MINIMAL_TX_BYTES.to_vec();

        let mut beef = Beef::new();
        beef.merge_raw_tx(raw_tx, None); // No bump index

        let tracker = AlwaysValidChainTracker;
        let known = HashSet::new();
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        // No BUMPs => returns Ok(false) indicating nothing to verify
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "BEEF with no BUMPs should return false (no proofs)"
        );
    }

    /// Test 5: BEEF where some txs have proofs and some don't (partial proof).
    /// The unproven tx is a txid-only entry, so it is valid with allow_txid_only.
    #[tokio::test]
    async fn test_partial_beef() {
        let height = 800_000u32;
        let (mut beef, _proven_txid, merkle_root) = build_single_proven_beef(height);

        // Add a txid-only entry (no proof needed, represents a known tx)
        let known_txid = "bb".repeat(32);
        beef.merge_txid_only(known_txid.clone());

        // The BEEF has one proven tx and one txid-only tx
        // verify_valid(true) should be fine since we allow txid_only
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);

        let known = HashSet::new();
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(
            result.is_ok(),
            "Expected Ok for partial BEEF, got: {:?}",
            result
        );
        assert!(result.unwrap(), "Expected true for valid partial BEEF");
    }

    /// Test 6: Truncated BEEF binary data should fail to parse.
    #[tokio::test]
    async fn test_malformed_beef_truncated() {
        // Build valid BEEF binary then truncate it
        let height = 800_000u32;
        let (mut beef, _, _) = build_single_proven_beef(height);
        let binary = beef.to_binary();

        // Truncate to half the data
        let truncated = &binary[..binary.len() / 2];

        let parse_result = Beef::from_binary(truncated);
        assert!(parse_result.is_err(), "Truncated BEEF should fail to parse");
    }

    /// Test 7: BEEF with trailing garbage bytes.
    /// Note: the BEEF parser reads exactly what it needs and stops, so extra
    /// bytes after valid data may not cause an error at parse time. We verify
    /// the parsed BEEF still validates correctly despite the extra bytes.
    #[tokio::test]
    async fn test_malformed_beef_extra_bytes() {
        let height = 800_000u32;
        let (mut beef, _, merkle_root) = build_single_proven_beef(height);
        let mut binary = beef.to_binary();

        // Append garbage bytes
        binary.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]);

        // The parser may or may not reject this depending on whether it checks
        // for unconsumed bytes. Either way, if it parses, the BEEF itself should
        // still be structurally valid.
        match Beef::from_binary(&binary) {
            Ok(mut parsed_beef) => {
                // If parsing succeeds, the parsed BEEF should still verify correctly
                let mut tracker = MockChainTracker::new(height + 1);
                tracker.add_root(height, merkle_root);
                let known = HashSet::new();

                let result = verify_beef_merkle_proofs(
                    &mut parsed_beef,
                    &tracker,
                    BeefVerificationMode::Strict,
                    &known,
                )
                .await;

                // The core data is valid, so verification should pass
                assert!(
                    result.is_ok(),
                    "Parsed BEEF should still verify: {:?}",
                    result
                );
            }
            Err(_) => {
                // Parser rejected extra bytes - this is also acceptable behavior
            }
        }
    }

    /// Test 8: BEEF containing the same tx twice.
    /// merge_raw_tx replaces existing entries with the same txid, so we
    /// verify the BEEF ends up with exactly one copy.
    #[tokio::test]
    async fn test_beef_duplicate_transactions() {
        let height = 800_000u32;
        let raw_tx = MINIMAL_TX_BYTES.to_vec();
        let txid = compute_txid(&raw_tx);

        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let merkle_root = bump.compute_root(Some(&txid)).unwrap();

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(bump);

        // Add the same raw tx twice
        beef.merge_raw_tx(raw_tx.clone(), Some(bump_index));
        beef.merge_raw_tx(raw_tx.clone(), Some(bump_index));

        // BEEF should de-duplicate: only one tx entry
        assert_eq!(
            beef.txs.len(),
            1,
            "BEEF should de-duplicate identical transactions"
        );

        // Verification should still pass
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        assert!(result.unwrap(), "Expected true for de-duplicated BEEF");
    }

    /// Test 9: BEEF with exactly one transaction (single-tx BEEF).
    #[tokio::test]
    async fn test_single_tx_beef() {
        let height = 750_000u32;
        let (mut beef, txid, merkle_root) = build_single_proven_beef(height);

        assert_eq!(beef.txs.len(), 1, "Should have exactly one transaction");
        assert_eq!(beef.bumps.len(), 1, "Should have exactly one BUMP");

        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        assert!(result.unwrap(), "Expected true for single-tx BEEF");

        // Verify individual txid
        let txid_result = verify_txid_merkle_proof(&beef, &txid, &tracker).await;
        assert!(txid_result.is_ok());
        assert!(txid_result.unwrap());
    }

    /// Test 10: BEEF with multiple transactions in a chain (parent -> child).
    /// The parent has a merkle proof; the child spends the parent.
    #[tokio::test]
    async fn test_multi_tx_beef() {
        let height = 800_000u32;

        // Build the parent tx (proven)
        let parent_raw = MINIMAL_TX_BYTES.to_vec();
        let parent_txid = compute_txid(&parent_raw);
        let parent_bump = MerklePath::from_coinbase_txid(&parent_txid, height);
        let merkle_root = parent_bump.compute_root(Some(&parent_txid)).unwrap();

        // Build a child tx that spends the parent
        // We need to construct a raw tx whose first input references parent_txid
        let parent_txid_bytes = bsv_rs::primitives::from_hex(&parent_txid).unwrap();
        let mut parent_txid_le = parent_txid_bytes;
        parent_txid_le.reverse(); // txid is stored little-endian in raw tx

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&parent_txid_le); // prev txid (parent)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // satoshis
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(parent_bump);
        beef.merge_raw_tx(parent_raw, Some(bump_index)); // parent (proven)
        beef.merge_raw_tx(child_raw, None); // child (unproven, chains to parent)

        assert_eq!(beef.txs.len(), 2, "Should have two transactions");

        // Verify with a tracker that knows the parent's merkle root
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(
            result.is_ok(),
            "Expected Ok for multi-tx BEEF, got: {:?}",
            result
        );
        assert!(result.unwrap(), "Expected true for valid multi-tx BEEF");
    }

    /// Test 11: Verification with TrustKnown mode.
    /// TrustKnown should still verify merkle roots against the chain tracker,
    /// but at the transaction level, known txids could be skipped.
    #[tokio::test]
    async fn test_beef_verification_trust_known_mode() {
        let height = 800_000u32;
        let (mut beef, txid, merkle_root) = build_single_proven_beef(height);

        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root.clone());

        let mut known = HashSet::new();
        known.insert(txid.clone());

        // TrustKnown mode with a valid tracker should pass
        let result = verify_beef_merkle_proofs(
            &mut beef,
            &tracker,
            BeefVerificationMode::TrustKnown,
            &known,
        )
        .await;

        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        assert!(result.unwrap(), "TrustKnown with valid roots should pass");

        // TrustKnown with invalid tracker should fail (roots are still verified)
        let mut bad_tracker = MockChainTracker::new(height + 1);
        bad_tracker.add_root(height, "cc".repeat(32));

        let result2 = verify_beef_merkle_proofs(
            &mut beef,
            &bad_tracker,
            BeefVerificationMode::TrustKnown,
            &known,
        )
        .await;

        assert!(
            result2.is_err(),
            "TrustKnown with bad root should still fail"
        );
    }

    /// Test 12: Verification when disabled should skip all checks.
    #[tokio::test]
    async fn test_beef_verification_disabled_mode() {
        let height = 800_000u32;
        let (mut beef, _, _) = build_single_proven_beef(height);

        // Use a tracker that returns errors - shouldn't matter since disabled
        let tracker = ErrorChainTracker;
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Disabled, &known)
                .await;

        assert!(result.is_ok(), "Disabled mode should always succeed");
        assert!(
            result.unwrap(),
            "Disabled mode should return true regardless"
        );
    }

    // =========================================================================
    // Additional edge case tests
    // =========================================================================

    /// Verify that verify_txid_merkle_proof returns Ok(false) when the txid
    /// has no BUMP in the BEEF.
    #[tokio::test]
    async fn test_verify_txid_no_bump() {
        let height = 800_000u32;
        let (beef, _, merkle_root) = build_single_proven_beef(height);

        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);

        // Look up a txid that doesn't exist in the BEEF
        let missing_txid = "dd".repeat(32);
        let result = verify_txid_merkle_proof(&beef, &missing_txid, &tracker).await;

        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Should return false for txid with no BUMP"
        );
    }

    /// Verify that a BEEF with a proven tx fails when the chain tracker
    /// returns an error (e.g., network failure).
    #[tokio::test]
    async fn test_beef_chain_tracker_error() {
        let height = 800_000u32;
        let (mut beef, _, _) = build_single_proven_beef(height);

        let tracker = ErrorChainTracker;
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(result.is_err(), "Should propagate chain tracker errors");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("ChainTracker verification failed"),
            "Error should mention ChainTracker failure, got: {}",
            err_msg
        );
    }

    /// Verify that the AlwaysInvalidChainTracker causes verification to fail
    /// for a structurally valid BEEF.
    #[tokio::test]
    async fn test_beef_always_invalid_tracker() {
        let height = 800_000u32;
        let (mut beef, _, _) = build_single_proven_beef(height);

        let tracker = AlwaysInvalidChainTracker;
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(result.is_err(), "Should fail with always-invalid tracker");
    }

    /// Verify BEEF serialization roundtrip preserves verification results.
    #[tokio::test]
    async fn test_beef_roundtrip_serialization() {
        let height = 800_000u32;
        let (mut beef, _txid, merkle_root) = build_single_proven_beef(height);

        // Serialize and deserialize
        let binary = beef.to_binary();
        let mut beef2 = Beef::from_binary(&binary).expect("Should parse BEEF from binary");

        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);
        let known = HashSet::new();

        // Both original and roundtripped should verify
        let result1 =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;
        assert!(result1.is_ok() && result1.unwrap());

        let result2 =
            verify_beef_merkle_proofs(&mut beef2, &tracker, BeefVerificationMode::Strict, &known)
                .await;
        assert!(
            result2.is_ok() && result2.unwrap(),
            "Roundtripped BEEF should also verify"
        );
    }

    /// Verify that all BeefVerificationMode variants serialize correctly.
    #[test]
    fn test_beef_verification_mode_all_variants_serialization() {
        // Strict
        let strict = BeefVerificationMode::Strict;
        let json = serde_json::to_string(&strict).unwrap();
        assert_eq!(json, "\"strict\"");
        let parsed: BeefVerificationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, BeefVerificationMode::Strict);

        // TrustKnown
        let trust_known = BeefVerificationMode::TrustKnown;
        let json = serde_json::to_string(&trust_known).unwrap();
        assert_eq!(json, "\"trustKnown\"");
        let parsed: BeefVerificationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, BeefVerificationMode::TrustKnown);

        // Disabled
        let disabled = BeefVerificationMode::Disabled;
        let json = serde_json::to_string(&disabled).unwrap();
        assert_eq!(json, "\"disabled\"");
        let parsed: BeefVerificationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, BeefVerificationMode::Disabled);
    }

    /// Verify a BEEF with multiple BUMPs at different heights.
    #[tokio::test]
    async fn test_beef_multiple_bumps_different_heights() {
        let height1 = 800_000u32;
        let height2 = 800_001u32;

        // Create two different raw transactions with different content
        let raw_tx1 = MINIMAL_TX_BYTES.to_vec();
        let txid1 = compute_txid(&raw_tx1);

        // Slightly different tx (change locktime)
        let mut raw_tx2 = MINIMAL_TX_BYTES.to_vec();
        let last_idx = raw_tx2.len() - 1;
        raw_tx2[last_idx] = 0x01; // change last byte (locktime)
        let txid2 = compute_txid(&raw_tx2);

        let bump1 = MerklePath::from_coinbase_txid(&txid1, height1);
        let root1 = bump1.compute_root(Some(&txid1)).unwrap();

        let bump2 = MerklePath::from_coinbase_txid(&txid2, height2);
        let root2 = bump2.compute_root(Some(&txid2)).unwrap();

        let mut beef = Beef::new();
        let bi1 = beef.merge_bump(bump1);
        let bi2 = beef.merge_bump(bump2);
        beef.merge_raw_tx(raw_tx1, Some(bi1));
        beef.merge_raw_tx(raw_tx2, Some(bi2));

        assert_eq!(beef.bumps.len(), 2);
        assert_eq!(beef.txs.len(), 2);

        let mut tracker = MockChainTracker::new(height2 + 1);
        tracker.add_root(height1, root1);
        tracker.add_root(height2, root2);
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        assert!(result.unwrap(), "Multi-bump BEEF should verify");
    }

    /// Verify a BEEF where one of multiple roots is invalid fails.
    #[tokio::test]
    async fn test_beef_one_of_multiple_roots_invalid() {
        let height1 = 800_000u32;
        let height2 = 800_001u32;

        let raw_tx1 = MINIMAL_TX_BYTES.to_vec();
        let txid1 = compute_txid(&raw_tx1);

        let mut raw_tx2 = MINIMAL_TX_BYTES.to_vec();
        let last_idx2 = raw_tx2.len() - 1;
        raw_tx2[last_idx2] = 0x01;
        let txid2 = compute_txid(&raw_tx2);

        let bump1 = MerklePath::from_coinbase_txid(&txid1, height1);
        let root1 = bump1.compute_root(Some(&txid1)).unwrap();

        let bump2 = MerklePath::from_coinbase_txid(&txid2, height2);

        let mut beef = Beef::new();
        let bi1 = beef.merge_bump(bump1);
        let bi2 = beef.merge_bump(bump2);
        beef.merge_raw_tx(raw_tx1, Some(bi1));
        beef.merge_raw_tx(raw_tx2, Some(bi2));

        // Only add root1, NOT root2
        let mut tracker = MockChainTracker::new(height2 + 1);
        tracker.add_root(height1, root1);
        // height2 root is missing from the tracker

        let known = HashSet::new();
        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(
            result.is_err(),
            "Should fail when one of multiple roots is invalid"
        );
    }

    /// Verify that a txid-only BEEF entry has no BUMP and returns Ok(false)
    /// from verify_txid_merkle_proof.
    #[tokio::test]
    async fn test_verify_txid_only_entry() {
        let mut beef = Beef::new();
        let txid = "ee".repeat(32);
        beef.merge_txid_only(txid.clone());

        // Add a dummy BUMP so bumps is not empty
        let dummy_txid = "ff".repeat(32);
        let bump = MerklePath::from_coinbase_txid(&dummy_txid, 100);
        beef.merge_bump(bump);

        let tracker = AlwaysValidChainTracker;

        // The txid-only entry has no BUMP, so verify_txid_merkle_proof should return Ok(false)
        let result = verify_txid_merkle_proof(&beef, &txid, &tracker).await;
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "txid-only entry should return false (no proof)"
        );
    }

    /// Verify using a real transaction (TEST_TX_HEX) with a proper coinbase proof.
    #[tokio::test]
    async fn test_beef_with_real_transaction() {
        let height = 100_000u32;
        let tx = Transaction::from_hex(TEST_TX_HEX).unwrap();
        let txid = tx.id();

        // Create a coinbase-style merkle proof for this txid
        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let merkle_root = bump.compute_root(Some(&txid)).unwrap();
        assert_eq!(merkle_root, txid, "For coinbase-only block, root == txid");

        let mut beef = Beef::new();
        let bump_idx = beef.merge_bump(bump);

        // We need to add the tx's input too since it's not proven
        // The input txid is c997a5e5... which we add as txid-only
        let input_txid = "0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9";
        beef.merge_txid_only(input_txid.to_string());
        beef.merge_transaction(tx);

        // Set bump_index on the real tx
        if let Some(beef_tx) = beef.find_txid_mut(&txid) {
            beef_tx.set_bump_index(Some(bump_idx));
        }

        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, merkle_root);
        let known = HashSet::new();

        let result =
            verify_beef_merkle_proofs(&mut beef, &tracker, BeefVerificationMode::Strict, &known)
                .await;

        assert!(
            result.is_ok(),
            "Expected Ok for real-tx BEEF, got: {:?}",
            result
        );
        assert!(result.unwrap(), "Real-tx BEEF should verify");
    }
}
