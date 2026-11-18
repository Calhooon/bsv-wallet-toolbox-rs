//! A CHECKED PROOF COMPLETES EVERY ROW OF ITS TXID IN THE STORE, A
//! `failed` ONE INCLUDED (the keeper's readings 19 and 20 of section 30).
//!
//! The state: X's action reads `failed` (the host's abort with its mark; a
//! release with no mark; the retire path's `failed`, its inputs locked; a
//! release with a competitor's checked proof on record; the abort with an
//! input spent meanwhile by another action of X), or another user's
//! received row of the txid reads `failed`; its request was raised
//! `unmined` (another user's presentation), so the proof path asks; a
//! checked proof arrives (`ingest_merkle_proof`).
//!
//! The rule: every row of the txid `completed` with its proof, its inputs
//! marked spent by it (they are, on chain), an input another action spent
//! left with that action and the proven txid queued on that action's
//! request as its competitor (the competitor proof ask retires it), its own
//! outputs that nothing spent spendable again and counted, the cause on
//! the request's history (`provenOverFailed`).

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
use bsv_wallet_toolbox_rs::{MonitorStorage, StorageSqlx, WalletStorageWriter};

/// The rows' states the proof meets.
const STATES: [Cause; 6] = [
    Cause::AbortFree,
    Cause::Unmarked,
    Cause::Refusal,
    Cause::Evidence,
    Cause::AbortSpentByOther,
    Cause::Received,
];

/// One state's reading after the proof.
#[derive(Debug)]
struct Proved {
    reading: Reading,
    /// The other action's request names the proven txid as its competitor.
    loser_queued: bool,
    proven_over_failed: bool,
}

async fn proved(cause: Cause) -> Proved {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, "cHJvb2Y=", "ZmFpbGVk");
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("proof-failed", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    store.set_chain_tracker(Arc::new(Headers)).await;
    MonitorStorage::set_max_acceptable_proof_height(&store, u32::MAX)
        .await
        .unwrap();
    let fixture = seed(&store, &parts, cause).await;
    if cause.aborts() {
        abort_now(&fixture, cause).await;
    }
    if cause == Cause::AbortSpentByOther {
        // The meanwhile spender's own request, which the proof's loser
        // queue writes to.
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
             raw_tx, created_at, updated_at) VALUES (?, 'sending', 0, '{}', 0, '{}', X'01', ?, ?)",
        )
        .bind("0d".repeat(32))
        .bind(chrono::Utc::now())
        .bind(chrono::Utc::now())
        .execute(store.writer())
        .await
        .unwrap();
    }
    // Raised by another user's presentation: the proof path asks for it.
    sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined' WHERE txid = ?")
        .bind(&fixture.txid)
        .execute(store.writer())
        .await
        .unwrap();
    let path = MerklePath::from_coinbase_txid(&fixture.txid, PROVEN_HEIGHT);
    let outcome = store
        .ingest_merkle_proof(
            &fixture.txid,
            &path.to_binary().unwrap(),
            PROVEN_HEIGHT,
            &"cc".repeat(32),
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            outcome,
            bsv_wallet_toolbox_rs::ProofIngestOutcome::Ingested(_)
        ),
        "{cause:?}: the checked proof is stored: {outcome:?}"
    );
    let reading = read(&fixture, cause == Cause::Received).await;
    let other: Option<String> =
        sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
            .bind("0d".repeat(32))
            .fetch_optional(store.pool())
            .await
            .unwrap();
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&fixture.txid)
        .fetch_one(store.pool())
        .await
        .unwrap();
    Proved {
        reading,
        loser_queued: other.is_some_and(|h| h.contains(&fixture.txid)),
        proven_over_failed: history.contains("provenOverFailed"),
    }
}

fn judge(cause: Cause, got: &Proved) -> Option<String> {
    let r = &got.reading;
    let own = |zero: &str| vec![(false, zero.to_string()), (false, "own".to_string())];
    let ok = match cause {
        Cause::Received => {
            r.row.as_deref() == Some("completed")
                && r.request.as_deref() == Some("completed")
                && got.proven_over_failed
        }
        Cause::AbortSpentByOther => {
            r.row.as_deref() == Some("completed")
                && r.request.as_deref() == Some("completed")
                && r.inputs == own("other")
                && r.own_output_spendable == Some(true)
                && got.loser_queued
                && got.proven_over_failed
        }
        _ => {
            r.row.as_deref() == Some("completed")
                && r.request.as_deref() == Some("completed")
                && r.inputs == own("own")
                && r.own_output_spendable == Some(true)
                && r.balance == 500
                && got.proven_over_failed
        }
    };
    (!ok).then(|| format!("{cause:?}: {got:?}"))
}

#[tokio::test]
async fn a_checked_proof_completes_a_failed_row_in_every_state() {
    let mut wrong = Vec::new();
    for cause in STATES {
        let got = proved(cause).await;
        let r = &got.reading;
        println!(
            "PROOF {cause:?} -> row {:?} request {:?} inputs {:?} own output {:?} balance {} provenOverFailed {} loser queued {}",
            r.row, r.request, r.inputs, r.own_output_spendable, r.balance, got.proven_over_failed, got.loser_queued
        );
        if let Some(w) = judge(cause, &got) {
            wrong.push(w);
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}
