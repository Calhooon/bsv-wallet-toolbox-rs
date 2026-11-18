//! WHAT THE ABORT ENDED WITH IT (the keeper's reading 20 of section 30,
//! Q2): today's answer, pinned, no build.
//!
//! The state: X's action R is posted and its own output (vout 1) funds X's
//! descendant D, also posted; the host aborts R while its post is in
//! flight, and the abort ends D with it (`retire_poisoned_descendants`: D
//! `failed`, its request `invalid`, its own outputs closed, its input from
//! R left spent by D, "from the poisoned set, stays dead", with no
//! re-check scheduled). Then an instance takes R; later one takes D.
//!
//! The answer: R's take recovers R alone (unproven, its request unmined,
//! its inputs marked again, `takenAfterAbort`); D stays ended. R's output
//! that D spent reads as a coin whose spender was given up: not spendable,
//! spent by the `failed` D, no `locked_input_checks` row, so offered by
//! nothing and asked about by nothing. D's own take recovers nothing: the
//! abort wrote no mark on D's request, and by its state D's input is
//! still locked (spent by D), so the take is a hint and D's request keeps
//! its `invalid` word.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use std::sync::Arc;

use abort_take::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_wallet_toolbox_rs::{BroadcastOutcome, StorageSqlx, WalletStorageWriter};

async fn word(store: &StorageSqlx, sql: &str, id: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(id)
        .fetch_optional(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_roots_take_recovers_the_root_alone_and_the_descendant_stays_ended() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, "ZGVzYw==", "ZW5kZWQ=");
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("descendant", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    store.set_chain_tracker(Arc::new(Headers)).await;
    let f = seed(&store, &parts, Cause::AbortFree).await;
    let x = f.x.user_id.unwrap();

    // D spends R's own output (vout 1) and has its own change.
    let d_bytes = common::competitor_of(&f.txid, 1).to_binary();
    let d_txid = bsv_rs::transaction::Transaction::from_binary(&d_bytes)
        .unwrap()
        .id();
    let d = insert_tx(&store, x, &d_txid, "sending", &d_bytes, true).await;
    sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
        .bind(d)
        .bind(f.own_output)
        .execute(store.writer())
        .await
        .unwrap();
    let d_change = insert_output(&store, x, d, &d_txid, 0, 400, (true, None)).await;
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
         raw_tx, created_at, updated_at) VALUES (?, 'sending', 0, '{}', 0, '{}', ?, ?, ?)",
    )
    .bind(&d_txid)
    .bind(&d_bytes)
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap();

    // The host aborts R; the abort ends D with it.
    abort_now(&f, Cause::AbortFree).await;
    let d_row = "SELECT status FROM transactions WHERE txid = ?";
    let d_req = "SELECT status FROM proven_tx_reqs WHERE txid = ?";
    assert_eq!(
        word(&store, d_row, &d_txid).await.as_deref(),
        Some("failed")
    );
    assert_eq!(
        word(&store, d_req, &d_txid).await.as_deref(),
        Some("invalid")
    );

    // An instance takes R.
    store
        .update_transaction_status_after_post(
            &f.txid,
            &BroadcastOutcome::Success,
            &[common::accepted(&f.txid)],
            None,
        )
        .await
        .unwrap();
    let r = read(&f, false).await;
    println!("ROOT after its take: {r:?}");
    assert_eq!(r.row.as_deref(), Some("unproven"), "{r:?}");
    assert_eq!(r.request.as_deref(), Some("unmined"), "{r:?}");
    assert!(r.taken_after_abort, "{r:?}");

    // D stays ended; R's output D spent is a coin whose spender was given up.
    let coin = |id: i64| {
        let store = store.clone();
        async move {
            let (spendable, spent_by): (bool, Option<i64>) =
                sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
                    .bind(id)
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
            let checks: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM locked_input_checks WHERE output_id = ?")
                    .bind(id)
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
            (spendable, spent_by, checks)
        }
    };
    let given_up = coin(f.own_output).await;
    println!(
        "DESCENDANT after the root's take: row {:?} request {:?}; the root's output it spent (spendable, spent_by, checks) {:?}; D's change {:?}",
        word(&store, d_row, &d_txid).await,
        word(&store, d_req, &d_txid).await,
        given_up,
        coin(d_change).await
    );
    assert_eq!(
        word(&store, d_row, &d_txid).await.as_deref(),
        Some("failed")
    );
    assert_eq!(
        word(&store, d_req, &d_txid).await.as_deref(),
        Some("invalid")
    );
    assert_eq!(given_up, (false, Some(d), 0));
    assert_eq!(r.own_output_spendable, Some(false), "{r:?}");
    assert!(!coin(d_change).await.0);

    // D's own take: no mark on D's request, its input still locked by it.
    store
        .update_transaction_status_after_post(
            &d_txid,
            &BroadcastOutcome::Success,
            &[common::accepted(&d_txid)],
            None,
        )
        .await
        .unwrap();
    let history = word(
        &store,
        "SELECT history FROM proven_tx_reqs WHERE txid = ?",
        &d_txid,
    )
    .await
    .unwrap_or_default();
    println!(
        "DESCENDANT after its own take: row {:?} request {:?} history {history}",
        word(&store, d_row, &d_txid).await,
        word(&store, d_req, &d_txid).await
    );
    assert_eq!(
        word(&store, d_row, &d_txid).await.as_deref(),
        Some("failed")
    );
    assert_eq!(
        word(&store, d_req, &d_txid).await.as_deref(),
        Some("invalid")
    );
    assert!(history.contains("takeHeldAsHint"), "{history}");
    assert!(!history.contains("\"abort\""), "{history}");
    assert_eq!(coin(f.own_output).await, (false, Some(d), 0));
}
