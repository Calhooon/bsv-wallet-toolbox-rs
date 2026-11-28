//! A held receipt ends on evidence about its transaction, not on a
//! request's word alone (the keeper's reading 27, section 30's reading 11;
//! `word_scope.rs`).
//!
//! Two users of one store hold a row of one txid P: the sender X its own
//! action (`is_outgoing = 1`, `sending`, spending X's coin C), the receiver
//! Y a received row (`is_outgoing = 0`, held `sending`, or `unproven` where
//! an instance took it) with a 600-satoshi payment output. The table: the
//! request's writer (X's abort; a broadcaster's fatal word on X's post; the
//! host's retire; a competitor's checked proof through the proof task) by
//! the writer that acts next (the status review; the retire path) by Y's
//! word before. Each cell reads Y's row, Y's output and Y's balance
//! (`listOutputs`), and X's row. A word ends X's rows alone; the
//! competitor's checked proof ends both. Then, for each word cell, the
//! receipt's own ends: its proof arrives (every row `completed`, Y counted)
//! or a competitor's checked proof is stored (the next review fails Y's row
//! and voids its output).

use std::sync::Arc;

use bsv_rs::transaction::{Beef, MockChainTracker};
use bsv_rs::wallet::AbortActionArgs;
use chrono::{Duration, Utc};

use super::create_action::find_or_insert_output_basket;
use super::reorg_tests::{
    child_spending, list_outputs_args, open_gate, single_leaf_bump, storage, txid_of, COINBASE_HEX,
    COINBASE_TXID,
};
use super::void_lineage_tests::{apply_rejected_naming, serving};
use super::StorageSqlx;
use crate::services::mock::MockResponse;
use crate::services::traits::GetStatusForTxidsResult;
use crate::services::{TxStatusDetail, WalletServices};
use crate::storage::traits::{WalletStorageProvider, WalletStorageReader, WalletStorageWriter};
use crate::storage::MonitorStorage;
use crate::AuthId;

/// The request's writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writer {
    /// X's own abort of its `sending` action (the request `invalid`).
    Abort,
    /// A broadcaster's fatal word on X's post (`mark_transaction_rejected`).
    Refusal,
    /// The host's retire, no status source holding P (`invalid`).
    Retire,
    /// A competitor's checked proof through the proof task (`doubleSpend`).
    Competitor,
}

/// The writer that acts next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Next {
    Review,
    Retire,
}

const WRITERS: [Writer; 4] = [
    Writer::Abort,
    Writer::Refusal,
    Writer::Retire,
    Writer::Competitor,
];
const NEXTS: [Next; 2] = [Next::Review, Next::Retire];
const WORDS: [&str; 2] = ["sending", "unproven"];
const PAYMENT: i64 = 600;
const HEIGHT: u32 = 900;
/// P's own proof height (the mock tracker keeps one root a height).
const P_HEIGHT: u32 = 901;

/// P: spends C (the coinbase's output 0); X's change at vout 0, Y's
/// payment at vout 1.
fn p_raw() -> Vec<u8> {
    let mut raw = child_spending(COINBASE_TXID);
    let tail = raw.split_off(raw.len() - 4 - 1 - 8 - 1);
    raw.push(2);
    raw.extend_from_slice(&tail[1..10]);
    raw.extend_from_slice(&(PAYMENT as u64).to_le_bytes());
    raw.push(0);
    raw.extend_from_slice(&tail[10..]);
    raw
}

/// R: another spend of C, the competitor.
fn r_raw() -> Vec<u8> {
    let mut raw = child_spending(COINBASE_TXID);
    let value_at = 4 + 1 + 36 + 1 + 4 + 1;
    raw[value_at..value_at + 8].copy_from_slice(&777u64.to_le_bytes());
    raw
}

struct Cell {
    s: StorageSqlx,
    x: AuthId,
    y: AuthId,
    p: String,
    x_row: i64,
    y_row: i64,
    y_out: i64,
}

async fn user(s: &StorageSqlx, key: &str) -> (AuthId, i64) {
    let (u, _) = s.find_or_insert_user(key).await.unwrap();
    let basket = {
        let mut conn = s.writer().acquire().await.unwrap();
        find_or_insert_output_basket(&mut conn, u.user_id, "default")
            .await
            .unwrap()
            .basket_id
    };
    (AuthId::with_user_id(key, u.user_id), basket)
}

#[allow(clippy::too_many_arguments)]
async fn row(
    s: &StorageSqlx,
    user: &AuthId,
    txid: &str,
    status: &str,
    raw: &[u8],
    outgoing: bool,
) -> i64 {
    let old = Utc::now() - Duration::days(2);
    sqlx::query_scalar(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, 'held receipt scope', 0, 1, 0, ?5, ?6, ?7, ?7) RETURNING transaction_id",
    )
    .bind(user.user_id.unwrap())
    .bind(txid)
    .bind(status)
    .bind(format!("ref-{}-{}", &txid[..8], user.user_id.unwrap()))
    .bind(raw)
    .bind(outgoing)
    .bind(old)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn output(
    s: &StorageSqlx,
    user: &AuthId,
    basket: i64,
    owner: i64,
    txid: &str,
    vout: i64,
    satoshis: i64,
    purpose: &str,
    (spendable, spent_by): (bool, Option<i64>),
) -> i64 {
    let old = Utc::now() - Duration::days(2);
    sqlx::query_scalar(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, derivation_prefix, derivation_suffix, txid, spent_by, locking_script, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, 'storage', ?7, 'P2PKH', 'prefix', 'suffix', ?8, ?9, x'51', ?10, ?10) RETURNING output_id",
    )
    .bind(user.user_id.unwrap())
    .bind(owner)
    .bind(basket)
    .bind(spendable)
    .bind(vout)
    .bind(satoshis)
    .bind(purpose)
    .bind(txid)
    .bind(spent_by)
    .bind(old)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

fn services(raw: &[u8], proof_of: &str) -> Arc<dyn WalletServices> {
    Arc::new(
        serving(raw, Some(single_leaf_bump(HEIGHT, proof_of)))
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "status".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid_of(&p_raw()),
                    status: "unknown".to_string(),
                    ..Default::default()
                }],
            }))
            .build(),
    )
}

/// X's action P `sending` spending C, its request `unsent`; Y's received
/// row of P at `word` with its payment output.
async fn seed(word: &str) -> Cell {
    let store = storage().await;
    let mut tracker = MockChainTracker::new(1000);
    let p_txid = txid_of(&p_raw());
    let r_txid = txid_of(&r_raw());
    tracker.add_root(HEIGHT, r_txid.clone());
    tracker.add_root(P_HEIGHT, p_txid.clone());
    store.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&store, 1000).await;
    let (sender, sender_basket) = user(&store, &"a".repeat(66)).await;
    let (receiver, receiver_basket) = user(&store, &"b".repeat(66)).await;
    let coinbase = hex::decode(COINBASE_HEX).unwrap();
    let cb = row(
        &store,
        &sender,
        COINBASE_TXID,
        "completed",
        &coinbase,
        false,
    )
    .await;
    let x_row = row(&store, &sender, &p_txid, "sending", &p_raw(), true).await;
    output(
        &store,
        &sender,
        sender_basket,
        cb,
        COINBASE_TXID,
        0,
        5000,
        "change",
        (false, Some(x_row)),
    )
    .await;
    output(
        &store,
        &sender,
        sender_basket,
        x_row,
        &p_txid,
        0,
        1000,
        "change",
        (true, None),
    )
    .await;
    let y_row = row(&store, &receiver, &p_txid, word, &p_raw(), false).await;
    let y_out = output(
        &store,
        &receiver,
        receiver_basket,
        y_row,
        &p_txid,
        1,
        PAYMENT,
        "receive",
        (true, None),
    )
    .await;
    let mut beef = Beef::new();
    beef.merge_raw_tx(coinbase, None).unwrap();
    let old = Utc::now() - Duration::days(2);
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, input_beef, created_at, updated_at) \
         VALUES (?, 'unsent', 0, '{}', 0, '{}', ?, ?, ?, ?)",
    )
    .bind(&p_txid)
    .bind(p_raw())
    .bind(beef.to_binary().unwrap())
    .bind(old)
    .bind(old)
    .execute(store.writer())
    .await
    .unwrap();
    WalletStorageProvider::set_services(&store, services(&r_raw(), &r_txid));
    Cell {
        s: store,
        x: sender,
        y: receiver,
        p: p_txid,
        x_row,
        y_row,
        y_out,
    }
}

/// The host's retire, no status source holding P; `false` where refused.
async fn retire(c: &Cell) -> bool {
    let services = c.s.get_services().unwrap();
    c.s.retire_undeliverable_txid(services.as_ref(), &c.p, "invalid")
        .await
        .is_ok()
}

/// The word written; `false` where the writer refused it (the abort and
/// the retire refuse while a row of the txid reads `unproven`, a take).
async fn write_word(c: &Cell, writer: Writer) -> bool {
    let s = &c.s;
    match writer {
        Writer::Abort => {
            let reference: String =
                sqlx::query_scalar("SELECT reference FROM transactions WHERE transaction_id = ?")
                    .bind(c.x_row)
                    .fetch_one(s.pool())
                    .await
                    .unwrap();
            s.abort_action(&c.x, AbortActionArgs { reference })
                .await
                .is_ok()
        }
        Writer::Refusal => MonitorStorage::mark_transaction_rejected(s, &c.p, false)
            .await
            .unwrap(),
        Writer::Retire => retire(c).await,
        Writer::Competitor => {
            let r = txid_of(&r_raw());
            // Arcade's pushed REJECTED 466 naming R: R queued for the
            // proof ask, no word written.
            apply_rejected_naming(s, &c.p, &r).await;
            s.synchronize_transaction_statuses().await.unwrap();
            true
        }
    }
}

async fn act_next(c: &Cell, next: Next) {
    let s = &c.s;
    match next {
        Next::Review => {
            MonitorStorage::review_status(s).await.unwrap();
        }
        Next::Retire => {
            let _ = retire(c).await;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Reading {
    x_row: String,
    y_row: String,
    y_out_spendable: bool,
    y_balance: i64,
}

async fn read(c: &Cell) -> Reading {
    let s = &c.s;
    let status = |id: i64| async move {
        sqlx::query_scalar::<_, String>("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(id)
            .fetch_one(c.s.pool())
            .await
            .unwrap()
    };
    let y_out_spendable: bool =
        sqlx::query_scalar("SELECT spendable FROM outputs WHERE output_id = ?")
            .bind(c.y_out)
            .fetch_one(s.pool())
            .await
            .unwrap();
    let listed = s.list_outputs(&c.y, list_outputs_args()).await.unwrap();
    Reading {
        x_row: status(c.x_row).await,
        y_row: status(c.y_row).await,
        y_out_spendable,
        y_balance: listed.outputs.iter().map(|o| o.satoshis as i64).sum(),
    }
}

/// What the rule says of a cell: a word ends X's row and leaves Y's as it
/// was; the competitor's checked proof ends both; a refused word moves
/// nothing.
fn want(writer: Writer, word: &str, written: bool) -> Reading {
    let counted = if word == "unproven" { PAYMENT } else { 0 };
    if !written {
        return Reading {
            x_row: "sending".into(),
            y_row: word.into(),
            y_out_spendable: true,
            y_balance: counted,
        };
    }
    match writer {
        Writer::Competitor => Reading {
            x_row: "failed".into(),
            y_row: "failed".into(),
            y_out_spendable: false,
            y_balance: 0,
        },
        _ => Reading {
            x_row: "failed".into(),
            y_row: word.into(),
            y_out_spendable: true,
            y_balance: counted,
        },
    }
}

/// The table: every writer by every next writer by Y's word before.
#[tokio::test]
async fn a_word_ends_only_the_senders_rows_and_evidence_ends_every_row() {
    let mut wrong = Vec::new();
    for writer in WRITERS {
        for next in NEXTS {
            for word in WORDS {
                let c = seed(word).await;
                let written = write_word(&c, writer).await;
                act_next(&c, next).await;
                let got = read(&c).await;
                let want = want(writer, word, written);
                if got != want {
                    wrong.push(format!(
                        "{writer:?} (written {written}) then {next:?}, Y {word}: {got:?}, want {want:?}"
                    ));
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "cells off the rule:\n{}",
        wrong.join("\n")
    );
}

/// The receipt's own ends beside a word: its proof completes every row of
/// the txid and counts Y's payment; a competitor's checked proof stored
/// (a third user's spend of C, proven) is evidence, and the next review
/// fails Y's row and voids its output.
#[tokio::test]
async fn a_held_receipt_beside_a_word_ends_on_its_proof_or_a_competitors() {
    let mut wrong = Vec::new();
    for writer in [Writer::Abort, Writer::Refusal, Writer::Retire] {
        for word in WORDS {
            // Its proof arrives.
            let c = seed(word).await;
            let _ = write_word(&c, writer).await;
            MonitorStorage::review_status(&c.s).await.unwrap();
            let bump = single_leaf_bump(P_HEIGHT, &c.p);
            c.s.ingest_merkle_proof(&c.p, &bump, P_HEIGHT, &"0".repeat(64), None)
                .await
                .unwrap();
            let got = read(&c).await;
            if (got.y_row.as_str(), got.y_balance) != ("completed", PAYMENT) {
                wrong.push(format!("{writer:?}, Y {word}, its proof: {got:?}"));
            }

            // A competitor's checked proof is stored.
            // (A refused word leaves the request asking: the competitor's
            // end there is the proof task's, the table's Competitor row.)
            let c = seed(word).await;
            if !write_word(&c, writer).await {
                continue;
            }
            MonitorStorage::review_status(&c.s).await.unwrap();
            let (z, zb) = user(&c.s, &"c".repeat(66)).await;
            let coinbase = hex::decode(COINBASE_HEX).unwrap();
            let z_cb = row(&c.s, &z, COINBASE_TXID, "completed", &coinbase, false).await;
            let r = txid_of(&r_raw());
            let z_r = row(&c.s, &z, &r, "unproven", &r_raw(), true).await;
            output(
                &c.s,
                &z,
                zb,
                z_cb,
                COINBASE_TXID,
                0,
                5000,
                "change",
                (false, Some(z_r)),
            )
            .await;
            let bump = single_leaf_bump(HEIGHT, &r);
            c.s.ingest_merkle_proof(&r, &bump, HEIGHT, &"0".repeat(64), None)
                .await
                .unwrap();
            MonitorStorage::review_status(&c.s).await.unwrap();
            let got = read(&c).await;
            if (got.y_row.as_str(), got.y_out_spendable, got.y_balance) != ("failed", false, 0) {
                wrong.push(format!(
                    "{writer:?}, Y {word}, a competitor's proof: {got:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "ends off the rule:\n{}", wrong.join("\n"));
}
