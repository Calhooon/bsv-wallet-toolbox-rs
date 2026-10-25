#[cfg(test)]
mod tests {
    use super::super::*;

    // =========================================================================
    // send_waiting_transactions doubleSpend + UTXO verification
    // =========================================================================

    // NOTE: A full integration test for send_waiting_transactions encountering a
    // doubleSpend with UTXO-verified rollback would require:
    //   1. A proven parent tx in `proven_txs` (with raw_tx + merkle_path for BEEF rebuild)
    //   2. A child tx in `transactions` + `proven_tx_reqs` with status='unprocessed'
    //   3. The child's raw_tx must be parseable and reference the parent's txid as input
    //   4. Outputs for the parent marked as spent_by the child
    //   5. Mock services: post_beef → double-spend, get_status_for_txids → no match,
    //      is_utxo → Sequence(true, false) for partial restore
    //
    // The core UTXO-verified rollback logic is already tested by:
    //   - test_double_spend_partial_utxo_verification (partial is_utxo results)
    //   - test_double_spend_restores_inputs (all inputs verified)
    //   - test_double_spend_no_services_keeps_inputs_locked (fail-safe)
    //
    // The send_waiting path reuses the same pattern inline. A full end-to-end
    // test is deferred because it requires BEEF rebuild infrastructure (proven_txs
    // with valid merkle paths) wired through the entire broadcast pipeline.

    // =========================================================================
    // validate_beef_for_broadcast Tests
    // =========================================================================

    /// Minimal valid coinbase-like transaction bytes (no real inputs).
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
    fn test_compute_txid(raw_tx: &[u8]) -> String {
        use bsv_rs::primitives::{sha256d, to_hex};
        let hash = sha256d(raw_tx);
        let mut reversed = hash;
        reversed.reverse();
        to_hex(&reversed)
    }

    /// Test validate_beef_for_broadcast with a valid BEEF: 1 unproven leaf + 1 proven parent.
    #[test]
    fn test_validate_beef_for_broadcast_valid() {
        use bsv_rs::transaction::{Beef, MerklePath};

        let height = 800_000u32;

        // Build the parent tx (proven, coinbase-like)
        let parent_raw = MINIMAL_TX_BYTES.to_vec();
        let parent_txid = test_compute_txid(&parent_raw);
        let parent_bump = MerklePath::from_coinbase_txid(&parent_txid, height);

        // Build a child tx that spends the parent
        let parent_txid_bytes = bsv_rs::primitives::from_hex(&parent_txid).unwrap();
        let mut parent_txid_le = parent_txid_bytes;
        parent_txid_le.reverse(); // txid stored little-endian in raw tx

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&parent_txid_le); // prev txid (parent)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // sats
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = test_compute_txid(&child_raw);

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(parent_bump);
        beef.merge_raw_tx(parent_raw, Some(bump_index)); // parent (proven)
        beef.merge_raw_tx(child_raw, None); // child (unproven leaf)

        // Validation should pass: 1 unproven leaf whose input parent is in the BEEF
        let result = validate_beef_for_broadcast(&beef, &child_txid);
        assert!(
            result.is_ok(),
            "Expected valid BEEF, got error: {:?}",
            result.err()
        );
    }

    /// Test validate_beef_for_broadcast with 0 unproven transactions (all proven).
    #[test]
    fn test_validate_beef_for_broadcast_no_unproven() {
        use bsv_rs::transaction::{Beef, MerklePath};

        let height = 800_000u32;
        let raw_tx = MINIMAL_TX_BYTES.to_vec();
        let txid = test_compute_txid(&raw_tx);
        let bump = MerklePath::from_coinbase_txid(&txid, height);

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(bump);
        beef.merge_raw_tx(raw_tx, Some(bump_index)); // proven — no unproven leaves

        let result = validate_beef_for_broadcast(&beef, &txid);
        assert!(
            result.is_err(),
            "Expected error for BEEF with no unproven leaf"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("no unproven leaf"),
            "Error should mention 'no unproven leaf', got: {}",
            err
        );
    }

    /// Test validate_beef_for_broadcast with a missing parent (input source not in BEEF).
    #[test]
    fn test_validate_beef_for_broadcast_missing_parent() {
        use bsv_rs::transaction::Beef;

        // Build a child tx that references a parent NOT in the BEEF.
        // Use a dummy parent txid that won't be in the BEEF.
        let fake_parent_txid = "ab".repeat(32);
        let fake_parent_bytes = bsv_rs::primitives::from_hex(&fake_parent_txid).unwrap();
        let mut fake_parent_le = fake_parent_bytes;
        fake_parent_le.reverse();

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&fake_parent_le); // prev txid (missing from BEEF)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // sats
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = test_compute_txid(&child_raw);

        let mut beef = Beef::new();
        beef.merge_raw_tx(child_raw, None); // child only, no parent

        let result = validate_beef_for_broadcast(&beef, &child_txid);
        assert!(
            result.is_err(),
            "Expected error for BEEF with missing parent"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("missing source"),
            "Error should mention 'missing source', got: {}",
            err
        );
    }
}
