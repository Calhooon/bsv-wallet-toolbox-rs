//! WHAT THE ABORT ENDED WITH IT (the keeper's readings 20, Q2, and 24 of
//! section 30).
//!
//! The state: X's action R is posted and its own output (vout 1) funds X's
//! descendant D; the host aborts R while its post is in flight, and the
//! abort ends D with it (`retire_poisoned_descendants`: D `failed`, its
//! request `invalid`, its own outputs closed, its input from R left spent
//! by D). Then an instance takes R.
//!
//! R's take recovers R (unproven, its request unmined, its inputs marked
//! again, `takenAfterAbort`); D stays ended. R's output that D spent
//! follows D's own end, by what the store still holds of D's post. The
//! release needs a positive ground; a missing record is not one:
//!
//! - never posted (D's request's whole history reads and shows no post; or
//!   no request beside a row of D that was never signed, no txid and no
//!   bytes): released in the write that recovers R (no spender, spendable
//!   as R's other outputs are), and a later action can take it;
//! - may have left (a post note on D's request's history, an instance's
//!   recorded word for D's txid), or the store cannot tell (a signed D
//!   whose request is gone, the purge's delete of an ended request
//!   included; a history kept whole under `previous`): it stays spent by D
//!   (it returns by D's own take, its proof, or a competitor's proof
//!   against it).
//!
//! The rule reads R's own outputs alone: an output of D that D's own
//! descendant spent is left as it is.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use std::sync::Arc;

use abort_take::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_wallet_toolbox_rs::storage::PurgeParams;
use bsv_wallet_toolbox_rs::{BroadcastOutcome, MonitorStorage, StorageSqlx, WalletStorageWriter};

async fn word(store: &StorageSqlx, sql: &str, id: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(id)
        .fetch_optional(store.pool())
        .await
        .unwrap()
}

/// (spendable, spent_by, locked-input checks) of an output.
async fn coin(store: &StorageSqlx, id: i64) -> (bool, Option<i64>, i64) {
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

/// D's request's record of a post, as each writer leaves it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Posted {
    /// No request beside a signed row (its txid and bytes on it).
    NoRequest,
    /// No request beside a row never signed: no txid, no bytes.
    NeverSigned,
    /// The wallet's own post's note, on a request the purge task then
    /// deletes (ended, older than the purge's age) ahead of R's take.
    PostedThenPurged,
    /// A request whose older history is kept whole under `previous`.
    PreviousHistory,
    /// A request with no post on record (a `noSend` action's).
    Never,
    /// The wallet's own post's note on its history.
    OwnPostNote,
    /// Send-waiting's post's note on its history.
    SendWaitingNote,
    /// A take recorded for D's txid.
    Seen,
}

struct Scene {
    store: StorageSqlx,
    f: Fixture,
    d: i64,
    d_change: i64,
    /// D's own descendant's spend of D's change, when the scene has one.
    grand: Option<i64>,
}

async fn scene(posted: Posted, grand: bool) -> Scene {
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
    let grand = if grand {
        let g_bytes = common::competitor_of(&d_txid, 0).to_binary();
        let g_txid = bsv_rs::transaction::Transaction::from_binary(&g_bytes)
            .unwrap()
            .id();
        let g = insert_tx(&store, x, &g_txid, "sending", &g_bytes, true).await;
        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
            .bind(g)
            .bind(d_change)
            .execute(store.writer())
            .await
            .unwrap();
        Some(g)
    } else {
        None
    };
    if posted == Posted::NeverSigned {
        sqlx::query(
            "UPDATE transactions SET txid = NULL, raw_tx = NULL, status = 'unsigned' \
             WHERE transaction_id = ?",
        )
        .bind(d)
        .execute(store.writer())
        .await
        .unwrap();
    }
    if !matches!(posted, Posted::NoRequest | Posted::NeverSigned) {
        let (word, attempts, history) = match posted {
            Posted::PreviousHistory => ("nosend", 0, r#"{"previous":"an older history"}"#.to_string()),
            Posted::Never => ("nosend", 0, "{}".to_string()),
            Posted::OwnPostNote | Posted::PostedThenPurged => (
                "sending",
                0,
                r#"{"notes":[{"what":"processActionPost","attempts":0}]}"#.to_string(),
            ),
            Posted::SendWaitingNote => (
                "unsent",
                1,
                r#"{"notes":[{"what":"sendWaitingHint","attempts":1}]}"#.to_string(),
            ),
            _ => ("sending", 0, "{}".to_string()),
        };
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
             raw_tx, created_at, updated_at) VALUES (?, ?, ?, ?, 0, '{}', ?, ?, ?)",
        )
        .bind(&d_txid)
        .bind(word)
        .bind(attempts)
        .bind(history)
        .bind(&d_bytes)
        .bind(now)
        .bind(now)
        .execute(store.writer())
        .await
        .unwrap();
    }
    if posted == Posted::Seen {
        sqlx::query(
            "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arc', 'seen')",
        )
        .bind(&d_txid)
        .execute(store.writer())
        .await
        .unwrap();
    }

    // The host aborts R; the abort ends D with it.
    abort_now(&f, Cause::AbortFree).await;
    if posted == Posted::NeverSigned {
        // The abort's walk follows txids; the unsigned descendant's end is
        // its own (the abandoned-action task's `failed`), written here with
        // its input from R left spent by it.
        sqlx::query("UPDATE transactions SET status = 'failed' WHERE transaction_id = ?")
            .bind(d)
            .execute(store.writer())
            .await
            .unwrap();
    }
    let d_word: Option<String> =
        sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(d)
            .fetch_optional(store.pool())
            .await
            .unwrap();
    assert_eq!(d_word.as_deref(), Some("failed"));
    if posted == Posted::PostedThenPurged {
        // D's ended request ages past the purge's 30 days; the real purge
        // deletes it, and with it the note of D's post.
        let old = chrono::Utc::now() - chrono::Duration::days(40);
        sqlx::query("UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ?")
            .bind(old)
            .bind(&d_txid)
            .execute(store.writer())
            .await
            .unwrap();
        let purged = MonitorStorage::purge_data(
            &store,
            PurgeParams {
                max_age_days: 30,
                purge_completed: true,
                purge_failed: true,
            },
        )
        .await
        .unwrap();
        println!("the purge: {}", purged.log.trim());
        assert_eq!(
            word(
                &store,
                "SELECT status FROM proven_tx_reqs WHERE txid = ?",
                &d_txid
            )
            .await,
            None
        );
    }

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
    assert_eq!(r.row.as_deref(), Some("unproven"), "{r:?}");
    assert_eq!(r.request.as_deref(), Some("unmined"), "{r:?}");
    assert!(r.taken_after_abort, "{r:?}");
    Scene {
        store,
        f,
        d,
        d_change,
        grand,
    }
}

/// D stays ended whatever its coin reads.
async fn assert_ended(s: &Scene) {
    let d_word: Option<String> =
        sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(s.d)
            .fetch_optional(s.store.pool())
            .await
            .unwrap();
    assert_eq!(d_word.as_deref(), Some("failed"));
}

/// Never posted, on a positive ground (a request whose history shows no
/// post; no request beside a row never signed): R's output D spent is
/// released in the recovery's write, and a later action of X's takes it.
#[tokio::test]
async fn a_given_up_spend_whose_bytes_never_left_is_released() {
    for posted in [Posted::NeverSigned, Posted::Never] {
        let s = scene(posted, false).await;
        let given_up = coin(&s.store, s.f.own_output).await;
        let history = word(
            &s.store,
            "SELECT history FROM proven_tx_reqs WHERE txid = ?",
            &s.f.txid,
        )
        .await
        .unwrap_or_default();
        println!("{posted:?}: the root's output D spent {given_up:?}; root history {history}");
        assert_ended(&s).await;
        assert_eq!(given_up, (true, None, 0), "{posted:?}");
        assert!(
            history.contains(&format!("\"givenUpSpends\":[{}]", s.f.own_output)),
            "{history}"
        );
        // A later action takes it: the allocator offers it as change.
        let x = s.f.x.user_id.unwrap();
        let basket = s.store.find_or_create_default_basket(x).await.unwrap();
        sqlx::query(
            "UPDATE outputs SET basket_id = ?, derivation_prefix = 'cA==', \
             derivation_suffix = 'cw==' WHERE output_id = ?",
        )
        .bind(basket.basket_id)
        .bind(s.f.own_output)
        .execute(s.store.writer())
        .await
        .unwrap();
        let later = insert_tx(&s.store, x, &"1a".repeat(32), "unsigned", &[1], true).await;
        let taken = s
            .store
            .allocate_change_input(x, basket.basket_id, 1, false, later)
            .await
            .unwrap();
        assert_eq!(taken.map(|o| o.output_id), Some(s.f.own_output));
        assert_eq!(
            coin(&s.store, s.f.own_output).await,
            (false, Some(later), 0)
        );
    }
}

/// May have left: R's output D spent stays spent by D.
#[tokio::test]
async fn a_given_up_spend_whose_bytes_may_have_left_is_withheld() {
    for posted in [Posted::OwnPostNote, Posted::SendWaitingNote, Posted::Seen] {
        withheld(posted).await;
    }
}

/// A posted descendant whose ended request the purge deleted: the record
/// of its post is gone with the request, and a missing record is not a no.
#[tokio::test]
async fn a_posted_descendant_whose_request_the_purge_deleted_is_withheld() {
    withheld(Posted::PostedThenPurged).await;
}

/// A signed descendant with no request: the store cannot tell.
#[tokio::test]
async fn a_signed_descendant_with_no_request_is_withheld() {
    withheld(Posted::NoRequest).await;
}

/// A history kept whole under `previous` is one the store cannot read.
#[tokio::test]
async fn a_descendant_whose_history_is_kept_under_previous_is_withheld() {
    withheld(Posted::PreviousHistory).await;
}

async fn withheld(posted: Posted) {
    {
        let s = scene(posted, false).await;
        let given_up = coin(&s.store, s.f.own_output).await;
        println!("{posted:?}: the root's output D spent {given_up:?}");
        assert_ended(&s).await;
        assert_eq!(given_up, (false, Some(s.d), 0), "{posted:?}");
        assert!(!coin(&s.store, s.d_change).await.0);
    }
}

/// The rule is applied once, to the recovered row's own outputs: D's
/// output that D's own descendant spent is left as it is.
#[tokio::test]
async fn the_rule_reads_the_recovered_rows_outputs_alone() {
    let s = scene(Posted::Never, true).await;
    let grand = s.grand.unwrap();
    let d_change = coin(&s.store, s.d_change).await;
    println!(
        "the root's output D spent {:?}; D's change its descendant spent {d_change:?}",
        coin(&s.store, s.f.own_output).await
    );
    assert_ended(&s).await;
    assert_eq!(coin(&s.store, s.f.own_output).await, (true, None, 0));
    assert_eq!(d_change, (false, Some(grand), 0));
}
