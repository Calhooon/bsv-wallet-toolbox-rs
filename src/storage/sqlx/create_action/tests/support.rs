use super::*;
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::wallet::CreateActionOutput;

/// Minimal coinbase-like raw_tx for seeded test transactions.
/// Has one coinbase input (all-zero txid, skipped by parse_input_txids)
/// and one output, so BEEF construction can include it without recursing.
pub(super) fn seed_raw_tx() -> Vec<u8> {
    hex::decode(
        "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
    )
    .unwrap()
}

// Helper function to seed a change output for testing
pub(super) async fn seed_change_output(storage: &StorageSqlx, user_id: i64, satoshis: i64) {
    let now = Utc::now();
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .unwrap();

    // Seed matching transaction bytes and a valid proof for the funding parent.
    let raw_tx = seed_raw_tx();
    let txid = txid_of(&raw_tx);
    seed_proven_chain_tx(storage, &txid, &raw_tx, 100_000).await;
    let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'completed', 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    let transaction_id = tx_result.last_insert_rowid();

    // Create a change output
    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    sqlx::query(
            r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                txid, type, spendable, change, derivation_prefix, derivation_suffix,
                provided_by, purpose, output_description, created_at, updated_at
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix123', 'suffix456', 'storage', 'change', 'seeded change', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(basket.basket_id)
        .bind(satoshis)
        .bind(&locking_script)
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
}

// Helper function to seed a proven transaction with merkle proof
pub(super) async fn seed_proven_tx(
    storage: &StorageSqlx,
    txid: &str,
    raw_tx: &[u8],
    merkle_path: &[u8],
) {
    let now = Utc::now();

    sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, 100000, 0, '0000000000000000000000000000000000000000000000000000000000000abc', '0000000000000000000000000000000000000000000000000000000000000def', ?, ?, ?, ?)
            "#,
        )
        .bind(txid)
        .bind(merkle_path)
        .bind(raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
}

// =============================================================================
// Tests for validate_stored_beef (Layer 4: pre-merge BEEF validation)
// =============================================================================

/// Helper: build a valid single-tx BEEF with a real merkle path.
/// Returns (beef_bytes, txid, height, merkle_root).
pub(super) fn build_test_beef_with_bump(height: u32, _fake_root: &str) -> (Vec<u8>, String) {
    use bsv_rs::transaction::MerklePath;

    // Coinbase tx (block 1) — a real transaction for structural validity
    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    // Build a minimal merkle path: single leaf (the txid itself) at index 0
    // The computed root will be the txid itself (single-tx block).
    // We create a MerklePath with the given height so verify_valid extracts
    // the (height, computed_root) pair for validation.
    let mut beef = Beef::new();
    let mp = MerklePath {
        block_height: height,
        path: vec![vec![bsv_rs::transaction::MerklePathLeaf {
            offset: 0,
            hash: Some(txid.to_string()),
            txid: true,
            duplicate: false,
        }]],
    };
    let bump_idx = beef.merge_bump(mp);
    beef.merge_raw_tx(raw_tx, Some(bump_idx));

    (beef.to_binary(), txid.to_string())
}

/// Helper: seed a transaction with input_beef in the DB (simulates stored ancestor BEEF).
pub(super) async fn seed_tx_with_input_beef(
    storage: &StorageSqlx,
    txid: &str,
    input_beef: &[u8],
    raw_tx: &[u8],
) {
    let now = Utc::now();
    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    sqlx::query(
        r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                                      version, lock_time, description, txid, raw_tx, input_beef,
                                      created_at, updated_at)
            VALUES (?, 'completed', ?, 0, 5000000000, 1, 0, 'Test tx', ?, ?, ?, ?, ?)
            "#,
    )
    .bind(user.user_id)
    .bind(format!("ref_{}", &txid[..8]))
    .bind(txid)
    .bind(raw_tx)
    .bind(input_beef)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
}

// =============================================================================
// Tests for nosend UTXO exclusion (nosend outputs must NOT be selected)
// =============================================================================

/// Helper: seed a change output whose parent transaction has the given status.
/// Uses a unique txid derived from `tag` to avoid collisions.
pub(super) async fn seed_change_output_with_status(
    storage: &StorageSqlx,
    user_id: i64,
    satoshis: i64,
    status: &str,
    tag: &str,
) {
    let now = Utc::now();
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .unwrap();

    // Build a unique 64-hex-char txid from the tag
    let txid = format!("{:0>64}", tag);
    let raw_tx = seed_raw_tx();

    let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, ?, 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    let transaction_id = tx_result.last_insert_rowid();

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    sqlx::query(
            r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                txid, type, spendable, change, derivation_prefix, derivation_suffix,
                provided_by, purpose, output_description, created_at, updated_at
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix123', 'suffix456', 'storage', 'change', 'seeded change', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(basket.basket_id)
        .bind(satoshis)
        .bind(&locking_script)
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
}

// =========================================================================
// UTXO selection preference tests — confirmed (completed) vs unconfirmed
// =========================================================================

/// Helper: insert a transaction with a given status and unique reference/txid
/// derived from `tag`, then insert a change output with the given satoshis
/// in the default basket. Returns (transaction_id, output_id).
pub(super) async fn seed_tagged_change_output(
    storage: &StorageSqlx,
    user_id: i64,
    satoshis: i64,
    status: &str,
    tag: &str,
) -> (i64, i64) {
    let now = Utc::now();
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .unwrap();

    let txid = format!("{:0>64}", tag);
    let reference = format!("seed_ref_{}", tag);
    let raw_tx = seed_raw_tx();

    let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, ?, ?, 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(&reference)
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    let transaction_id = tx_result.last_insert_rowid();

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let out_result = sqlx::query(
            r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                txid, type, spendable, change, derivation_prefix, derivation_suffix,
                provided_by, purpose, output_description, created_at, updated_at
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix123', 'suffix456', 'storage', 'change', 'seeded change', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(basket.basket_id)
        .bind(satoshis)
        .bind(&locking_script)
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    let output_id = out_result.last_insert_rowid();
    (transaction_id, output_id)
}

/// Helper: create a dummy spending transaction to use as the `transaction_id`
/// parameter for `allocate_change_input` (so `spent_by` FK is valid).
pub(super) async fn create_spending_tx(storage: &StorageSqlx, user_id: i64, tag: &str) -> i64 {
    let now = Utc::now();
    let txid = format!("{:0>64}", format!("spend_{}", tag));
    let reference = format!("spending_ref_{}", tag);
    let result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at)
            VALUES (?, 'unsigned', ?, 1, 0, 1, 0, 'Spending tx', ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&reference)
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
    result.last_insert_rowid()
}

// =========================================================================
// Caller-complete funding rule (sighash-aware)
// =========================================================================

/// A structurally valid `<DER sig><sighash>` push with 32-byte r and s.
pub(super) fn fake_der_signature(sighash: u8) -> Vec<u8> {
    let mut sig = vec![0x30, 0x44, 0x02, 0x20];
    sig.extend(std::iter::repeat_n(0x11u8, 32));
    sig.extend([0x02, 0x20]);
    sig.extend(std::iter::repeat_n(0x22u8, 32));
    sig.push(sighash);
    sig
}

/// `<sig> <pubkey>` — a P2PKH-shaped unlocking script signed with `sighash`.
pub(super) fn p2pkh_unlock(sighash: u8) -> Vec<u8> {
    let sig = fake_der_signature(sighash);
    let mut script = vec![sig.len() as u8];
    script.extend(sig);
    script.push(33);
    script.extend(std::iter::repeat_n(0x02u8, 33));
    script
}

/// Seed a spendable, NON-change output (a covenant UTXO the caller will spend
/// with its own unlocking script). Returns the parent txid (hex).
pub(super) async fn seed_custom_output(
    storage: &StorageSqlx,
    user_id: i64,
    satoshis: i64,
    locking_script: &[u8],
    tag: &str,
) -> String {
    let now = Utc::now();
    let mut tx = bsv_rs::transaction::Transaction::from_binary(&seed_raw_tx()).unwrap();
    tx.lock_time = u32::from_str_radix(tag, 16).unwrap();
    tx.outputs[0].satoshis = Some(satoshis as u64);
    tx.outputs[0].locking_script =
        bsv_rs::script::LockingScript::from_binary(locking_script).unwrap();
    tx.invalidate_caches();
    let raw_tx = tx.to_binary();
    let txid = tx.id();
    seed_proven_chain_tx(storage, &txid, &raw_tx, 100_001 + tx.lock_time).await;
    let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'completed', ?, 1, ?, 1, 0, 'Seed covenant transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(format!("seed_custom_{}", tag))
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
    let transaction_id = tx_result.last_insert_rowid();

    sqlx::query(
        r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                txid, type, spendable, change, provided_by, purpose, output_description,
                created_at, updated_at
            )
            VALUES (?, ?, NULL, 0, ?, ?, ?, 'custom', 1, 0, 'you', '', 'seeded covenant', ?, ?)
            "#,
    )
    .bind(user_id)
    .bind(transaction_id)
    .bind(satoshis)
    .bind(locking_script)
    .bind(&txid)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();

    txid
}

pub(super) fn covenant_input(
    txid: &str,
    unlocking_script: Vec<u8>,
) -> bsv_rs::wallet::CreateActionInput {
    bsv_rs::wallet::CreateActionInput {
        outpoint: bsv_rs::wallet::Outpoint::from_string(&format!("{}.0", txid)).unwrap(),
        input_description: "covenant input".to_string(),
        unlocking_script: Some(unlocking_script),
        unlocking_script_length: None,
        sequence_number: None,
    }
}

pub(super) fn p2pkh_output(satoshis: u64, description: &str) -> CreateActionOutput {
    CreateActionOutput {
        locking_script: hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap(),
        satoshis,
        output_description: description.to_string(),
        basket: None,
        custom_instructions: None,
        tags: None,
    }
}

pub(super) async fn caller_complete_storage() -> (StorageSqlx, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    (storage, user.user_id)
}

// =========================================================================
// BEEF closure: a proven ancestor terminates the chain
//
// Mirrors the TypeScript builder in
// packages/wallet/wallet-toolbox/src/storage/methods/getBeefForTransaction.ts:
// a stored proven transaction contributes its raw bytes plus its BUMP and
// returns no dependencies, an unproven one contributes its bytes and its
// input txids, and knownTxids collapse to txid-only entries.
// =========================================================================

/// A minimal spend of `parent:vout`: one input, one OP_TRUE output.
/// Returns the raw bytes and the txid.
pub(super) fn synth_tx(parent_txid: &str, vout: u32, satoshis: u64) -> (Vec<u8>, String) {
    let mut raw: Vec<u8> = Vec::new();
    raw.extend_from_slice(&1u32.to_le_bytes()); // version
    raw.push(1); // input count
    let mut prev = hex::decode(parent_txid).unwrap();
    prev.reverse();
    raw.extend_from_slice(&prev);
    raw.extend_from_slice(&vout.to_le_bytes());
    raw.push(0); // empty unlocking script
    raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // sequence
    raw.push(1); // output count
    raw.extend_from_slice(&satoshis.to_le_bytes());
    raw.push(1); // locking script length
    raw.push(0x51); // OP_TRUE
    raw.extend_from_slice(&0u32.to_le_bytes()); // locktime

    let txid = txid_of(&raw);
    (raw, txid)
}

pub(super) fn txid_of(raw: &[u8]) -> String {
    use bsv_rs::primitives::hash::sha256d;
    let mut h = sha256d(raw);
    h.reverse();
    hex::encode(h)
}

/// A single-leaf merkle path: the txid is the only transaction in its block,
/// so the computed root is the txid itself.
pub(super) fn synth_bump(height: u32, txid: &str) -> MerklePath {
    MerklePath {
        block_height: height,
        path: vec![vec![bsv_rs::transaction::MerklePathLeaf {
            offset: 0,
            hash: Some(txid.to_string()),
            txid: true,
            duplicate: false,
        }]],
    }
}

/// A chain of `n` spends. `chain[0]` is the oldest. Every hop spends the
/// previous hop's output 0, which is exactly the shape a covenant UTXO
/// traded from wallet to wallet produces.
pub(super) fn synth_chain(n: usize) -> Vec<(Vec<u8>, String)> {
    let mut out = Vec::new();
    let mut parent = "11".repeat(32);
    for i in 0..n {
        let (raw, txid) = synth_tx(&parent, 0, 10_000 - i as u64);
        parent = txid.clone();
        out.push((raw, txid));
    }
    out
}

pub(super) fn beef_txids(beef: &Beef) -> HashSet<String> {
    beef.txs.iter().map(|t| t.txid()).collect()
}

pub(super) async fn seed_proven_chain_tx(
    storage: &StorageSqlx,
    txid: &str,
    raw_tx: &[u8],
    height: u32,
) {
    let bump = synth_bump(height, txid);
    let now = Utc::now();
    sqlx::query(
        r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root,
                                    merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)
            "#,
    )
    .bind(txid)
    .bind(height as i64)
    .bind(format!("block_{}", height))
    .bind(txid)
    .bind(bump.to_binary())
    .bind(raw_tx)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
}

pub(super) async fn seed_unproven_tx(
    storage: &StorageSqlx,
    txid: &str,
    raw_tx: &[u8],
    input_beef: Option<&[u8]>,
) {
    let now = Utc::now();
    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();
    sqlx::query(
        r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                                      version, lock_time, description, txid, raw_tx, input_beef,
                                      created_at, updated_at)
            VALUES (?, 'unproven', ?, 1, 1000, 1, 0, 'chain hop', ?, ?, ?, ?, ?)
            "#,
    )
    .bind(user.user_id)
    .bind(format!("ref_{}", &txid[..8]))
    .bind(txid)
    .bind(raw_tx)
    .bind(input_beef)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
}

// Synthetic witnesses for public toolbox issue #23. No captured bytes.
pub(super) fn l2_23_input(txid: &str) -> ExtendedInput {
    ExtendedInput {
        vin: 0,
        txid: txid.to_string(),
        vout: 0,
        satoshis: 1_000,
        locking_script: vec![0x51],
        unlocking_script_length: 0,
        input_description: None,
        output: None,
    }
}

pub(super) fn l2_23_args(tx: Vec<u8>) -> bsv_rs::wallet::InternalizeActionArgs {
    use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput};
    InternalizeActionArgs {
        tx,
        description: "synthetic ancestry witness".to_string(),
        labels: None,
        seek_permission: None,
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "witness".to_string(),
                custom_instructions: None,
                tags: None,
            }),
        }],
    }
}

pub(super) async fn l2_23_storage() -> StorageSqlx {
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate("test", "000000").await.unwrap();
    s.make_available().await.unwrap();
    s
}

pub(super) async fn l2_23_assembly(
    tracker: bool,
) -> (StorageSqlx, Vec<u8>, Vec<(Vec<u8>, String)>) {
    use bsv_rs::transaction::MockChainTracker;
    let s = l2_23_storage().await;
    let chain = synth_chain(3);
    seed_proven_chain_tx(&s, &chain[0].1, &chain[0].0, 100).await;
    let mut stub = Beef::new();
    stub.merge_raw_tx(chain[1].0.clone(), None);
    seed_unproven_tx(&s, &chain[1].1, &chain[1].0, Some(&stub.to_binary())).await;
    seed_unproven_tx(&s, &chain[2].1, &chain[2].0, None).await;
    let mut t = MockChainTracker::new(101);
    t.add_root(100, chain[0].1.clone());
    let mut conn = s.pool().acquire().await.unwrap();
    let bytes = build_input_beef(
        &mut conn,
        Some(s.writer()),
        if tracker { Some(&t) } else { None },
        &[l2_23_input(&chain[2].1)],
        &[],
        None,
        &[],
        false,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    drop(conn);
    (s, bytes, chain)
}

/// Move a seeded transaction's record back in time so the proof lookup
/// does not skip it as too young to be mined.
pub(super) async fn backdate_tx(storage: &StorageSqlx, txid: &str, minutes: i64) {
    let then = Utc::now() - chrono::Duration::minutes(minutes);
    sqlx::query("UPDATE transactions SET created_at = ? WHERE txid = ?")
        .bind(then)
        .bind(txid)
        .execute(storage.writer())
        .await
        .unwrap();
}
