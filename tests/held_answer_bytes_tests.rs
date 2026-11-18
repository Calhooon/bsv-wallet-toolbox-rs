//! BYTES ARE A TRANSACTION'S ONLY WHEN THEY HASH TO ITS TXID.
//!
//! Three readers take the store's copy of a transaction by its txid and
//! act on the outpoints it names: the take's recovery of a `failed` row
//! (its inputs marked spent by it again), a checked proof's completion of
//! a `failed` row (its inputs marked, the losers queued) and the competitor
//! check (a stored competitor's bytes). Each reads bytes only when they
//! hash to the txid. Here every copy held for the txid names another coin
//! of the wallet's, under a txid it does not hash to: that coin is
//! untouched.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use std::sync::Arc;

use abort_take::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::MerklePath;
use bsv_wallet_toolbox_rs::{BroadcastOutcome, MonitorStorage, StorageSqlx, WalletStorageWriter};

/// The sender's aborted action (released), and the bytes of a transaction
/// spending the parent's output 2, which hash to another txid.
async fn aborted() -> (Fixture, common::SpendParts, Vec<u8>) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("bytes-hash", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    store.set_chain_tracker(Arc::new(Headers)).await;
    MonitorStorage::set_max_acceptable_proof_height(&store, u32::MAX)
        .await
        .unwrap();
    let f = seed(&store, &parts, Cause::AbortFree).await;
    abort_now(&f, Cause::AbortFree).await;
    let other = common::competitor_of(&parts.parent_txid, 2).to_binary();
    (f, parts, other)
}

/// The sender's coin at the parent's output 2, spent by `spent_by`.
async fn coin_two(f: &Fixture, spent_by: Option<i64>) -> i64 {
    let x = f.x.user_id.unwrap();
    let parent: i64 = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ?",
    )
    .bind(&f.parent_txid)
    .bind(x)
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    insert_output(
        &f.store,
        x,
        parent,
        &f.parent_txid,
        2,
        1000,
        (spent_by.is_none(), spent_by),
    )
    .await
}

/// Every copy of the txid's transaction the store holds becomes `bytes`.
async fn hold(f: &Fixture, bytes: &[u8]) {
    for table in ["proven_tx_reqs", "transactions"] {
        sqlx::query(&format!("UPDATE {table} SET raw_tx = ? WHERE txid = ?"))
            .bind(bytes)
            .bind(&f.txid)
            .execute(f.store.writer())
            .await
            .unwrap();
    }
}

async fn coin(f: &Fixture, id: i64) -> (bool, Option<i64>) {
    sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(id)
        .fetch_one(f.store.pool())
        .await
        .unwrap()
}

async fn history(f: &Fixture, txid: &str) -> String {
    sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(f.store.pool())
        .await
        .unwrap()
}

/// The take's recovery: the row is not recovered and nothing is marked;
/// the cause on the request's history.
#[tokio::test]
async fn a_take_reads_no_outpoint_from_bytes_that_do_not_hash() {
    let (f, _parts, other) = aborted().await;
    let z = coin_two(&f, None).await;
    hold(&f, &other).await;
    f.store
        .update_transaction_status_after_post(
            &f.txid,
            &BroadcastOutcome::Success,
            &[common::accepted(&f.txid)],
            None,
        )
        .await
        .unwrap();
    let got = read(&f, false).await;
    let notes = history(&f, &f.txid).await;
    println!(
        "take: {got:?}\ncoin two {:?}\nhistory {notes}",
        coin(&f, z).await
    );
    assert_eq!(coin(&f, z).await, (true, None), "the other coin untouched");
    assert_eq!(got.row.as_deref(), Some("failed"));
    assert_eq!(got.request.as_deref(), Some("invalid"));
    assert_eq!(
        got.inputs,
        vec![(true, "none".to_string()), (true, "none".to_string())]
    );
    assert!(!got.recovered_note);
    assert!(notes.contains("takeNotRecovered") && notes.contains("noHeldBytesHashToTxid"));
}

/// A checked proof's completion over the `failed` row: the rows completed,
/// no input marked from the bytes, the note says so.
#[tokio::test]
async fn a_proofs_completion_marks_nothing_from_bytes_that_do_not_hash() {
    let (f, _parts, other) = aborted().await;
    let z = coin_two(&f, None).await;
    hold(&f, &other).await;
    let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
    let root = path.compute_root(Some(&f.txid)).unwrap();
    let outcome = f
        .store
        .ingest_merkle_proof(
            &f.txid,
            &path.to_binary().unwrap(),
            PROVEN_HEIGHT,
            &"bb".repeat(32),
            Some(&root),
        )
        .await
        .unwrap();
    let got = read(&f, false).await;
    let notes = history(&f, &f.txid).await;
    println!(
        "proof: {outcome:?} {got:?}\ncoin two {:?}\nhistory {notes}",
        coin(&f, z).await
    );
    assert_eq!(coin(&f, z).await, (true, None), "the other coin untouched");
    assert_eq!(got.row.as_deref(), Some("completed"));
    assert!(notes.contains("\"inputsMarked\":false"), "{notes}");
}

/// A checked proof's completion queues no loser named by bytes that do not
/// hash: another action spending the parent's output 2 is not queued.
#[tokio::test]
async fn a_proofs_completion_queues_no_loser_from_bytes_that_do_not_hash() {
    let (f, parts, other) = aborted().await;
    let x = f.x.user_id.unwrap();
    let loser_txid = "1e".repeat(32);
    let loser = insert_tx(&f.store, x, &loser_txid, "sending", &[1], true).await;
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, \
         created_at, updated_at) VALUES (?, 'sending', 0, '{}', 0, '{}', ?, ?, ?)",
    )
    .bind(&loser_txid)
    .bind(&parts.raw)
    .bind(chrono::Utc::now())
    .bind(chrono::Utc::now())
    .execute(f.store.writer())
    .await
    .unwrap();
    let z = coin_two(&f, Some(loser)).await;
    hold(&f, &other).await;
    let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
    let root = path.compute_root(Some(&f.txid)).unwrap();
    f.store
        .ingest_merkle_proof(
            &f.txid,
            &path.to_binary().unwrap(),
            PROVEN_HEIGHT,
            &"bb".repeat(32),
            Some(&root),
        )
        .await
        .unwrap();
    let loser_notes = history(&f, &loser_txid).await;
    println!(
        "loser history {loser_notes}; coin two {:?}",
        coin(&f, z).await
    );
    assert!(!loser_notes.contains("competitorsQueued"), "{loser_notes}");
    assert_eq!(coin(&f, z).await, (false, Some(loser)));
}

/// The competitor check: a stored transaction whose bytes spend the
/// sender's input but do not hash to its txid is no competitor, so the
/// wallet's own take of the sender's live action counts.
#[tokio::test]
async fn a_stored_competitor_whose_bytes_do_not_hash_is_no_competitor() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("bytes-hash", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    store.set_chain_tracker(Arc::new(Headers)).await;
    // The sender's live action, its inputs spent by it.
    let f = seed(&store, &parts, Cause::AbortFree).await;
    // A payer's row of the parent's output 0, spent by a proven
    // transaction whose stored bytes (a spend of that outpoint) hash to
    // another txid than its own.
    let fake_txid = "fa".repeat(32);
    let (p, _) = store
        .find_or_insert_user(&PrivateKey::random().public_key().to_hex())
        .await
        .unwrap();
    let p_parent = insert_tx(
        &store,
        p.user_id,
        &parts.parent_txid,
        "completed",
        &[1],
        false,
    )
    .await;
    let fake_row = insert_tx(
        &store,
        p.user_id,
        &fake_txid,
        "completed",
        &f.competitor,
        true,
    )
    .await;
    let path = MerklePath::from_coinbase_txid(&fake_txid, PROVEN_HEIGHT);
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, ?, 0, ?, ?, ?, ?)",
    )
    .bind(&fake_txid)
    .bind(PROVEN_HEIGHT)
    .bind("bb".repeat(32))
    .bind(path.compute_root(Some(&fake_txid)).unwrap())
    .bind(path.to_binary().unwrap())
    .bind(&f.competitor)
    .execute(store.writer())
    .await
    .unwrap();
    insert_output(
        &store,
        p.user_id,
        p_parent,
        &parts.parent_txid,
        0,
        1000,
        (false, Some(fake_row)),
    )
    .await;
    store
        .update_transaction_status_after_post(
            &f.txid,
            &BroadcastOutcome::Success,
            &[common::accepted(&f.txid)],
            None,
        )
        .await
        .unwrap();
    let got = read(&f, false).await;
    println!("competitor: {got:?}");
    assert!(!got.take_not_counted, "{got:?}");
    assert_eq!(got.row.as_deref(), Some("unproven"));
    assert_eq!(got.request.as_deref(), Some("unmined"));
}
