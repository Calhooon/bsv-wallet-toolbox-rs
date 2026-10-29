use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_build_input_beef_recursive_ancestor() {
    // Test that BEEF construction recursively fetches ancestors for unproven transactions
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    // Create a chain: proven_tx -> unproven_tx (which we spend)
    // The unproven_tx's input txid points to proven_tx

    // First, create the proven ancestor transaction (txid1)

    let raw_tx1 = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let txid1_owned = txid_of(&raw_tx1);
    let txid1 = txid1_owned.as_str();
    let merkle_path1 = synth_bump(100_000, txid1).to_binary();
    seed_proven_tx(&storage, txid1, &raw_tx1, &merkle_path1).await;

    // Create an unproven transaction (txid2) that spends from txid1

    // Build a raw tx that has txid1 as its input
    let mut raw_tx2 = vec![];
    raw_tx2.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    raw_tx2.push(0x01); // 1 input
                        // Input txid (txid1 in little-endian bytes)
    let txid1_bytes = hex::decode(txid1).unwrap();
    let mut txid1_le = txid1_bytes.clone();
    txid1_le.reverse(); // Convert to little-endian
    raw_tx2.extend_from_slice(&txid1_le);
    raw_tx2.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout 0
    raw_tx2.push(0x00); // empty script
    raw_tx2.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
    raw_tx2.push(0x01); // 1 output
    raw_tx2.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 1000 satoshis
    raw_tx2.push(0x00); // empty script
    raw_tx2.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    let txid2_owned = txid_of(&raw_tx2);
    let txid2 = txid2_owned.as_str();
    let now = Utc::now();
    sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_tx2', 0, 1000, 1, 0, 'Unproven tx spending proven', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid2)
        .bind(&raw_tx2)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    // Now build BEEF for txid2 - it should include both txid2 (unproven) and txid1 (proven with proof)
    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: txid2.to_string(),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        None,
    )
    .await
    .unwrap();

    // Should return Some BEEF data containing both transactions
    assert!(result.is_some());
    let beef_bytes = result.unwrap();
    // BEEF should be larger than just one transaction since it includes the ancestor chain
    assert!(beef_bytes.len() > 4);
}

// =============================================================================
// Tests for BEEF ancestry construction fixes
// =============================================================================

#[tokio::test]
async fn test_build_beef_includes_unconfirmed_parent() {
    // Create a chain: confirmed parent (with merkle proof) -> unconfirmed child -> grandchild
    // Build BEEF for the grandchild and verify it includes both the child (raw) and parent (with proof)
    use bsv_rs::primitives::sha256d;

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    // Helper to compute txid from raw bytes
    let compute_txid = |raw: &[u8]| -> String {
        let mut v = sha256d(raw).to_vec();
        v.reverse();
        hex::encode(v)
    };

    // 1. Create a confirmed parent transaction with merkle proof
    let parent_raw_tx = hex::decode(
        "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
    )
    .unwrap();
    let parent_txid = compute_txid(&parent_raw_tx);
    let parent_merkle_path = synth_bump(100_000, &parent_txid).to_binary();
    seed_proven_tx(&storage, &parent_txid, &parent_raw_tx, &parent_merkle_path).await;

    // 2. Create an unconfirmed child tx that spends from parent
    let parent_txid_bytes = hex::decode(&parent_txid).unwrap();
    let mut parent_le = parent_txid_bytes.clone();
    parent_le.reverse();

    let mut child_raw_tx = vec![];
    child_raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    child_raw_tx.push(0x01); // 1 input
    child_raw_tx.extend_from_slice(&parent_le); // parent txid LE
    child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout 0
    child_raw_tx.push(0x00); // empty script
    child_raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
    child_raw_tx.push(0x01); // 1 output
    child_raw_tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    child_raw_tx.push(0x00); // empty script
    child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    let child_txid = compute_txid(&child_raw_tx);

    let now = Utc::now();
    sqlx::query(
        r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_child', 0, 1000, 1, 0, 'Unconfirmed child', ?, ?, ?, ?)
            "#,
    )
    .bind(user.user_id)
    .bind(&child_txid)
    .bind(&child_raw_tx)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();

    // 3. Build BEEF for the grandchild (which spends from child)
    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: child_txid.clone(),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        None,
    )
    .await
    .unwrap();

    // BEEF should contain both the child (raw) and parent (with proof)
    assert!(result.is_some());
    let beef_bytes = result.unwrap();
    let beef = Beef::from_binary(&beef_bytes).unwrap();

    // Verify both transactions are in the BEEF
    assert!(
        beef.find_txid(&child_txid).is_some(),
        "BEEF must contain the unconfirmed child tx"
    );
    assert!(
        beef.find_txid(&parent_txid).is_some(),
        "BEEF must contain the confirmed parent tx"
    );

    // Verify the BEEF has at least 2 transactions (child + parent)
    assert!(
        beef.txs.len() >= 2,
        "BEEF must contain at least 2 transactions (child + parent), got {}",
        beef.txs.len()
    );
}

#[tokio::test]
async fn test_build_beef_errors_on_missing_direct_input() {
    // Try to build BEEF for a tx whose direct input (depth 0) has no raw_tx in storage.
    // Should return an error, not silent success.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let missing_txid = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: missing_txid.to_string(),
        vout: 0,
        satoshis: 50_000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        None,
    )
    .await;

    // Should be an error — not silent success
    assert!(result.is_err(), "Must error when direct input is missing");
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("Cannot find raw tx for direct input"),
        "Error message must mention the missing direct input: {}",
        err
    );
    assert!(
        err.to_string().contains(missing_txid),
        "Error message must include the txid: {}",
        err
    );
}

#[tokio::test]
async fn test_build_beef_refuses_missing_deep_ancestor() {
    // Build BEEF where a depth-2 ancestor is missing.
    // The walk may log the missing ancestor, but final validation must
    // refuse the incomplete assembly.
    use bsv_rs::primitives::sha256d;

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    let compute_txid = |raw: &[u8]| -> String {
        let mut v = sha256d(raw).to_vec();
        v.reverse();
        hex::encode(v)
    };

    // Create a chain: missing_ancestor -> parent_tx -> child_tx (which we spend)
    // The missing_ancestor is NOT stored, so depth-2 lookup will fail.

    // Use a fixed 32-byte LE txid for the missing ancestor (not in any table)
    let missing_ancestor_txid = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let ancestor_bytes = hex::decode(missing_ancestor_txid).unwrap();
    let mut ancestor_le = ancestor_bytes.clone();
    ancestor_le.reverse();

    // Parent tx that references the missing ancestor
    let mut parent_raw_tx = vec![];
    parent_raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    parent_raw_tx.push(0x01); // 1 input
    parent_raw_tx.extend_from_slice(&ancestor_le); // references missing ancestor
    parent_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
    parent_raw_tx.push(0x00); // empty script
    parent_raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
    parent_raw_tx.push(0x01); // 1 output
    parent_raw_tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    parent_raw_tx.push(0x00); // empty script
    parent_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    let parent_txid = compute_txid(&parent_raw_tx);

    let now = Utc::now();
    sqlx::query(
        r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_parent', 0, 1000, 1, 0, 'Parent tx', ?, ?, ?, ?)
            "#,
    )
    .bind(user.user_id)
    .bind(&parent_txid)
    .bind(&parent_raw_tx)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();

    // Child tx that references parent
    let parent_bytes = hex::decode(&parent_txid).unwrap();
    let mut parent_le = parent_bytes.clone();
    parent_le.reverse();

    let mut child_raw_tx = vec![];
    child_raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    child_raw_tx.push(0x01); // 1 input
    child_raw_tx.extend_from_slice(&parent_le); // references parent
    child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
    child_raw_tx.push(0x00); // empty script
    child_raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
    child_raw_tx.push(0x01); // 1 output
    child_raw_tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    child_raw_tx.push(0x00); // empty script
    child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    let child_txid = compute_txid(&child_raw_tx);

    sqlx::query(
        r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_child', 0, 1000, 1, 0, 'Child tx', ?, ?, ?, ?)
            "#,
    )
    .bind(user.user_id)
    .bind(&child_txid)
    .bind(&child_raw_tx)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();

    // Build BEEF for child_tx — depth 0 = child_tx (found), depth 1 = parent_tx (found),
    // depth 2 = missing_ancestor (NOT found — should warn, not error)
    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: child_txid.clone(),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        None,
    )
    .await;

    assert!(
        matches!(result, Err(Error::ValidationError(ref e)) if e.contains("structure is invalid")),
        "missing ancestry must never produce a tx answer: {result:?}"
    );
}

#[tokio::test]
async fn test_input_beef_with_a_bump_carrying_its_txid_unflagged_is_linked_not_refused() {
    // P0-1d (2026-10-08, the beta write soak): the template's BEEF came
    // back as the next action's inputBEEF with its proven ancestor `b`
    // unlinked (b's leaf in the one BUMP was a plain sibling hash) and
    // was refused: "missing inputs [a]". The reference links a
    // transaction to any BUMP whose level-0 leaf carries its txid and
    // flags the leaf (ts-stack@edf6e03 packages/sdk/src/transaction/Beef.ts:1329-1341).
    use super::super::sibling_bump_tests::{
        leaf_flagged, path_flagging, siblings, soak_storage, HEIGHT,
    };
    let x = siblings();
    let mut user = Beef::new();
    user.merge_bump(path_flagging(&x.a.1, &x.b.1, &x.a.1));
    user.merge_raw_tx(x.b.0.clone(), None);
    user.merge_raw_tx(x.t.0.clone(), None);
    assert!(
        user.find_txid(&x.b.1).unwrap().bump_index().is_none(),
        "precondition: the soak shape, b unlinked"
    );
    let user_bytes = user.to_binary();
    let mut tracker = bsv_rs::transaction::MockChainTracker::new(1_000_000);
    tracker.add_root(HEIGHT, x.root.clone());
    let inputs = vec![ExtendedInput {
        vin: 0,
        txid: x.t.1.clone(),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    // The soak wallet (holding a's and b's proofs) and a wallet that
    // holds nothing: the BEEF alone carries the evidence.
    let empty = StorageSqlx::in_memory().await.unwrap();
    empty.migrate("test", "000000").await.unwrap();
    empty.make_available().await.unwrap();
    for (name, storage) in [("soak", soak_storage(&x).await), ("empty", empty)] {
        let mut conn = storage.pool().acquire().await.unwrap();
        let out = build_input_beef(
            &mut conn,
            Some(storage.writer()),
            Some(&tracker),
            &inputs,
            &[],
            Some(&user_bytes),
            &[],
            false,
            None,
        )
        .await;
        let bytes = match out {
            Ok(Some(b)) => b,
            other => panic!("{name}: the input BEEF is accepted, got {other:?}"),
        };
        let mut beef = Beef::from_binary(&bytes).unwrap();
        assert!(
            beef.find_txid(&x.b.1).unwrap().bump_index().is_some() && leaf_flagged(&beef, &x.b.1),
            "{name}: b is linked and its leaf flagged"
        );
        assert!(beef.verify_valid(true).valid, "{name}: valid");
    }
}

#[tokio::test]
async fn test_build_input_beef_trims_known_txids_to_txid_only() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(2);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_010).await;
    seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, None).await;

    let inputs = vec![ExtendedInput {
        vin: 0,
        txid: chain[1].1.clone(),
        vout: 0,
        satoshis: 1_000,
        locking_script: vec![0x51],
        unlocking_script_length: 107,
        input_description: Some("spend".to_string()),
        output: None,
    }];

    let mut conn = storage.pool().acquire().await.unwrap();
    let untrimmed = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &inputs,
        &[],
        None,
        &[],
        false,
        None,
    )
    .await
    .unwrap()
    .expect("a BEEF is produced");

    let trimmed = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &inputs,
        &[],
        None,
        &[chain[0].1.clone()],
        false,
        None,
    )
    .await
    .unwrap()
    .expect("a BEEF is produced");

    assert!(
        trimmed.len() < untrimmed.len(),
        "a known txid drops its raw bytes: {} vs {}",
        trimmed.len(),
        untrimmed.len()
    );
    let parsed = Beef::from_binary(&trimmed).unwrap();
    let known = parsed
        .find_txid(&chain[0].1)
        .expect("the known ancestor is still referenced");
    assert!(known.is_txid_only(), "and is referenced by txid alone");
}

#[tokio::test]
async fn test_build_input_beef_ten_hops_stops_at_the_proven_hop() {
    // The reported defect end to end: ten hops of one covenant UTXO, the
    // middle ones mined. The BEEF for the next spend must carry hops 5..9
    // and nothing older.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(10);
    for (i, (raw, txid)) in chain.iter().enumerate() {
        if i == 5 {
            seed_proven_chain_tx(&storage, txid, raw, 965_200).await;
            // A proof the funnel stored, so checked: with no tracker,
            // compaction attaches only a checked proof (P0-1c).
            super::super::reorg_tests::mark_checked(&storage, txid, 965_200).await;
        } else {
            // Every hop stored the whole ancestry it saw at build time.
            let mut stored = Beef::new();
            for (r, _) in chain.iter().take(i) {
                stored.merge_raw_tx(r.clone(), None);
            }
            let stored_bytes = if i == 0 {
                None
            } else {
                Some(stored.to_binary())
            };
            seed_unproven_tx(&storage, txid, raw, stored_bytes.as_deref()).await;
        }
    }

    let inputs = vec![ExtendedInput {
        vin: 0,
        txid: chain[9].1.clone(),
        vout: 0,
        satoshis: 1_000,
        locking_script: vec![0x51],
        unlocking_script_length: 107,
        input_description: Some("next hop".to_string()),
        output: None,
    }];

    let mut conn = storage.pool().acquire().await.unwrap();
    let bytes = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &inputs,
        &[],
        None,
        &[],
        false,
        None,
    )
    .await
    .unwrap()
    .expect("a BEEF is produced");

    let beef = Beef::from_binary(&bytes).unwrap();
    let kept = beef_txids(&beef);
    let expected: HashSet<String> = chain[5..].iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(
        kept, expected,
        "exactly hops 5..9; the proven hop terminates the closure"
    );
}
