//! A REFUTED PROOF BESIDE A `failed` ROW OF THE WALLET'S OWN RECOVERS
//! TOWARD CAUTION (the keeper's reading 26 of section 30).
//!
//! The state: a request reading `completed` beside a proof this store has
//! not checked, and the wallet's own `failed` outgoing row of that txid.
//! The tracker refutes the proof's root, and the demotion leaves the
//! request `unmined`: the store's word for "the network may hold it". In
//! the demotion's own write the `failed` row is read by its state, as a
//! take reads it:
//!
//! - released (with the abort's mark or with none): recovered by the one
//!   recovery: the row `unproven`, each free input marked spent by it
//!   again, an input another action spent meanwhile left with that action,
//!   its own outputs opened, the cause `recoveredOnRefutedProof` on the
//!   request's history, the request `unmined`;
//! - an input still locked by it (all of them, or some): it stays ended,
//!   every free input of it marked spent by it again, the request at the
//!   ended row's word (`invalid`) unless a live row of the txid needs it
//!   asking;
//! - evidence on record (a `spent-proven` check on one of its inputs): not
//!   recovered, the request `invalid`, a `refutedProofNotRecovered` note.
//!
//! After the demotion no cell reads a request asking for a proof beside a
//! `failed` row with a free input.
//!
//! The matrix: the row's six states by another user's live row of the txid
//! (a held receipt, `sending`) present or not, through the status review's
//! check 3; and the demotion's other door (`demote_stale_proof`) over the
//! released state.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use std::sync::Arc;

use abort_take::*;
use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath};
use bsv_wallet_toolbox_rs::{MonitorStorage, StorageSqlx, WalletStorageWriter};

/// Every root is refuted.
struct Refutes;

#[async_trait]
impl ChainTracker for Refutes {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Ok(false)
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(PROVEN_HEIGHT + 100)
    }
}

const STATES: [Cause; 6] = [
    Cause::AbortFree,
    Cause::Unmarked,
    Cause::AbortSpentByOther,
    Cause::Mixed,
    Cause::Refusal,
    Cause::Evidence,
];

/// How the refuted proof is met.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Door {
    /// The status review's check 3.
    Review,
    /// The demotion called by txid (the reorg path's, the host's).
    Demote,
}

struct Cell {
    f: Fixture,
    /// Another user's row of the txid, when the cell seeds one.
    other: Option<i64>,
}

/// The pair: the `failed` row in `cause`'s state beside a request
/// `completed` linked to an unchecked proof row; `linked` links the other
/// user's row to that proof too.
async fn pair(cause: Cause, other_live: bool, linked: bool) -> Cell {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("refuted-proof", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    store.set_chain_tracker(Arc::new(Refutes)).await;
    let f = seed(&store, &parts, cause).await;
    if cause.aborts() {
        abort_now(&f, cause).await;
    }
    let other = if other_live {
        let y_key = PrivateKey::random().public_key().to_hex();
        let (y, _) = store.find_or_insert_user(&y_key).await.unwrap();
        Some(insert_tx(&store, y.user_id, &f.txid, "sending", &parts.raw, false).await)
    } else {
        None
    };
    let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
    let ptx = sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, ?, 0, ?, ?, ?, ?)",
    )
    .bind(&f.txid)
    .bind(PROVEN_HEIGHT)
    .bind("bb".repeat(32))
    .bind(path.compute_root(Some(&f.txid)).unwrap())
    .bind(path.to_binary().unwrap())
    .bind(&parts.raw)
    .execute(store.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query("UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ? WHERE txid = ?")
        .bind(ptx)
        .bind(&f.txid)
        .execute(store.writer())
        .await
        .unwrap();
    if let (Some(other), true) = (other, linked) {
        sqlx::query("UPDATE transactions SET proven_tx_id = ? WHERE transaction_id = ?")
            .bind(ptx)
            .bind(other)
            .execute(store.writer())
            .await
            .unwrap();
    }
    Cell { f, other }
}

async fn refute(cell: &Cell, door: Door) {
    match door {
        Door::Review => {
            MonitorStorage::review_status(&cell.f.store).await.unwrap();
        }
        Door::Demote => {
            assert!(
                MonitorStorage::demote_stale_proof(&cell.f.store, &cell.f.txid)
                    .await
                    .unwrap()
            );
        }
    }
}

async fn history(f: &Fixture) -> String {
    sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&f.txid)
        .fetch_one(f.store.pool())
        .await
        .unwrap()
}

async fn other_word(cell: &Cell) -> Option<String> {
    let other = cell.other?;
    sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
        .bind(other)
        .fetch_optional(cell.f.store.pool())
        .await
        .unwrap()
}

fn pairs(a: (bool, &str), b: (bool, &str)) -> Vec<(bool, String)> {
    vec![(a.0, a.1.to_string()), (b.0, b.1.to_string())]
}

/// The cell's rule; `None` when it holds, else what is wrong.
fn judge(cause: Cause, other_live: bool, got: &Reading, history: &str) -> Option<String> {
    let name = format!("{cause:?} other_live={other_live}");
    if got.half() {
        return Some(format!(
            "{name}: a request asking beside a failed row with a free input: {got:?}"
        ));
    }
    let recovered = history.contains("recoveredOnRefutedProof");
    let not_recovered = history.contains("refutedProofNotRecovered");
    let own = pairs((false, "own"), (false, "own"));
    let ok = match cause {
        Cause::AbortFree | Cause::Unmarked => {
            got.row.as_deref() == Some("unproven")
                && got.request.as_deref() == Some("unmined")
                && got.inputs == own
                && got.own_output_spendable == Some(true)
                && got.balance == 500
                && recovered
        }
        Cause::AbortSpentByOther => {
            got.row.as_deref() == Some("unproven")
                && got.request.as_deref() == Some("unmined")
                && got.inputs == pairs((false, "other"), (false, "own"))
                && recovered
        }
        Cause::Mixed | Cause::Refusal => {
            let word = if other_live { "unmined" } else { "invalid" };
            got.row.as_deref() == Some("failed")
                && got.request.as_deref() == Some(word)
                && got.inputs == own
                && got.own_output_spendable == Some(false)
                && got.balance == 0
                && !recovered
        }
        Cause::Evidence => {
            got.row.as_deref() == Some("failed")
                && got.request.as_deref() == Some("invalid")
                && got.inputs == pairs((true, "none"), (true, "none"))
                && got.own_output_spendable == Some(false)
                && !recovered
                && not_recovered
        }
        _ => false,
    };
    (!ok).then(|| format!("{name}: {got:?} history {history}"))
}

#[tokio::test]
async fn a_refuted_proof_beside_a_failed_row_recovers_toward_caution() {
    let mut wrong = Vec::new();
    println!("| state | other user's live row | row | request | inputs | own output | balance | other row |");
    println!("|---|---|---|---|---|---|---|---|");
    for cause in STATES {
        for other_live in [false, true] {
            let cell = pair(cause, other_live, false).await;
            refute(&cell, Door::Review).await;
            let got = read(&cell.f, false).await;
            let history = history(&cell.f).await;
            let other = other_word(&cell).await;
            println!(
                "| {cause:?} | {other_live} | {:?} | {:?} | {:?} | {:?} | {} | {other:?} |",
                got.row, got.request, got.inputs, got.own_output_spendable, got.balance
            );
            if let Some(w) = judge(cause, other_live, &got, &history) {
                wrong.push(w);
            }
            // The other user's row was linked to no proof: the demotion
            // leaves it as it read.
            if other_live && other.as_deref() != Some("sending") {
                wrong.push(format!("{cause:?}: the other user's row reads {other:?}"));
            }
            // The next pass moves nothing of the sender's.
            MonitorStorage::review_status(&cell.f.store).await.unwrap();
            let next = read(&cell.f, false).await;
            if (next.row.clone(), next.inputs.clone()) != (got.row.clone(), got.inputs.clone()) {
                wrong.push(format!(
                    "{cause:?} other_live={other_live}: the next pass moved {next:?}"
                ));
            }
            if next.half() {
                wrong.push(format!(
                    "{cause:?} other_live={other_live}: half after the next pass"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The demotion called by txid meets the same pair and writes the same.
#[tokio::test]
async fn the_demotion_by_txid_recovers_the_same_pair() {
    for cause in [Cause::AbortFree, Cause::Refusal] {
        let cell = pair(cause, false, false).await;
        refute(&cell, Door::Demote).await;
        let got = read(&cell.f, false).await;
        let history = history(&cell.f).await;
        println!("{cause:?} by txid: {got:?}");
        assert_eq!(judge(cause, false, &got, &history), None);
    }
}

/// A recovered row's request is asked for its proof and no count ends it:
/// past the proof task's attempt limit it still reads `unmined` beside the
/// `unproven` row with its inputs marked.
#[tokio::test]
async fn a_row_recovered_on_a_refuted_proof_is_not_ended_by_the_count() {
    let cell = pair(Cause::AbortFree, false, false).await;
    refute(&cell, Door::Review).await;
    sqlx::query("UPDATE proven_tx_reqs SET attempts = 100000 WHERE txid = ?")
        .bind(&cell.f.txid)
        .execute(cell.f.store.writer())
        .await
        .unwrap();
    MonitorStorage::review_status(&cell.f.store).await.unwrap();
    let got = read(&cell.f, false).await;
    let history = history(&cell.f).await;
    assert_eq!(judge(Cause::AbortFree, false, &got, &history), None);
}

/// A HELD RECEIPT STAYS HELD: another user's received row of the txid that
/// reads `sending` (no instance has taken it) and is linked to the refuted
/// proof keeps `sending` through the demotion, with its link gone; it is
/// not counted on a proof that was just refuted.
#[tokio::test]
async fn the_demotion_leaves_a_held_receipt_held() {
    for door in [Door::Review, Door::Demote] {
        let cell = pair(Cause::AbortFree, true, true).await;
        refute(&cell, door).await;
        let other = cell.other.unwrap();
        let (word, link): (String, Option<i64>) = sqlx::query_as(
            "SELECT status, proven_tx_id FROM transactions WHERE transaction_id = ?",
        )
        .bind(other)
        .fetch_one(cell.f.store.pool())
        .await
        .unwrap();
        println!("{door:?}: the held receipt reads {word} link {link:?}");
        assert_eq!((word.as_str(), link), ("sending", None), "{door:?}");
        // The sender's own row is recovered beside it.
        let got = read(&cell.f, false).await;
        assert_eq!(got.row.as_deref(), Some("unproven"), "{got:?}");
    }
}
