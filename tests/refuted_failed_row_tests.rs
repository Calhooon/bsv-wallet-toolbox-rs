//! A refuted proof row beside a `failed` row of its txid, in the status
//! review's pass (check 3's kind: a `completed` request linking the proof
//! row; the linked kind: the row links it, beside a request no task asks
//! or none). In the pass's one write, no service asked, the `failed` row
//! is recovered only when it is the wallet's own outgoing row, the request
//! does not read `doubleSpend`, its bytes parse and hash to its txid, and
//! an input of its user's is free: it reads `unproven`, its link cleared,
//! its own outputs as the failure left them, each free input of its user
//! spent by it again (a coin another action has taken, and another user's
//! row at the same outpoint, are left), and the request asks for its proof
//! (`unmined`). In every other cell the row stays `failed`, nothing is
//! re-marked, and the request does not ask: `invalid` beside a `completed`
//! request, its own word beside a request no task asks, none where none
//! is held.
//!
//! How a recovered row ends is this line's rule for any asking request:
//! the proof task asks at every header, past its count too, writing no
//! word for the count; the host's retire (`retire_undeliverable_txid`)
//! fails it with its inputs locked and scheduled for the locked-input
//! re-check, which gives a coin back on the chain's word.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod proof_fixture;
mod sync_support;
use proof_fixture::*;
use sync_support::exec;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    /// A `completed` request links the proof row (check 3's kind).
    Check3,
    /// The row links the proof row; its request reads `invalid`.
    Linked,
    /// The row links the proof row; its request reads `doubleSpend`.
    LinkedDoubleSpend,
    /// The row links the proof row; the txid holds no request.
    LinkedNoRequest,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Row {
    /// An outgoing row whose input the failure released.
    Released,
    /// An outgoing row whose input is still spent by it.
    Locked,
    /// An outgoing row whose released input another action has taken.
    Taken,
    /// A received row, a coin of its user's free at its input outpoint.
    Received,
    /// An outgoing row with a released input whose bytes do not parse.
    NoBytes,
    /// An outgoing row with a released input whose bytes are another
    /// transaction's.
    OtherBytes,
    /// An outgoing row with a released input; another user of the store
    /// holds a free output row at the same outpoint.
    TwoUsers,
}

const ROWS: [Row; 7] = [
    Row::Released,
    Row::Locked,
    Row::Taken,
    Row::Received,
    Row::NoBytes,
    Row::OtherBytes,
    Row::TwoUsers,
];

/// The outpoint `n`'s bytes spend.
fn input_of(n: u8) -> (String, u32) {
    let (_, raw, _, _) = mined(n);
    let tx = bsv_rs::transaction::Transaction::from_binary(&raw).unwrap();
    let i = &tx.inputs[0];
    (i.source_txid.clone().unwrap(), i.source_output_index)
}

/// W, with a refuting tracker, holding `n` `failed` as `row`, beside its
/// proof row (no record) as `kind`. Returns W, the headers, the failed
/// row's id, the input coin's id and the other user's coin's id.
async fn shape(
    kind: Kind,
    row: Row,
    n: u8,
) -> (
    Arc<StorageSqlx>,
    Arc<Headers>,
    i64,
    Option<i64>,
    Option<i64>,
) {
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::Refutes, &headers).await;
    let (txid, raw, _, _) = mined(n);
    let outgoing = i64::from(row != Row::Received);
    let stored = match row {
        Row::NoBytes => vec![0xff, 0xff],
        Row::OtherBytes => mined(n + 100).1,
        _ => raw.clone(),
    };
    let t = exec(&w, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,'failed','wf',{outgoing},-1000,1,0,'pay','{txid}',X'{}',{NOW},{NOW})", hex::encode(&stored))).await;
    // Its own output, as the failure left it.
    exec(&w, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES (1,{t},0,1,0,1000,'storage','change','P2PKH','{txid}',1,0,X'51',{NOW},{NOW})")).await;
    let mut other_user_coin = None;
    let coin = {
        let (src, vout) = input_of(n);
        let s = exec(&w, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,created_at,updated_at) VALUES (1,'completed','src',0,2000,1,0,'in','{src}',{NOW},{NOW})")).await;
        let (spendable, spent_by) = match row {
            Row::Released | Row::Received | Row::NoBytes | Row::OtherBytes | Row::TwoUsers => {
                ("1", "NULL".to_string())
            }
            Row::Locked => ("0", t.to_string()),
            Row::Taken => {
                let other = exec(&w, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,created_at,updated_at) VALUES (1,'unsigned','other',1,-1,1,0,'other','{}',{NOW},{NOW})", "ab".repeat(32))).await;
                ("0", other.to_string())
            }
        };
        if row == Row::TwoUsers {
            let (bob, _) = w.find_or_insert_user(sync_support::BOB).await.unwrap();
            let b = bob.user_id;
            let bs = exec(&w, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,created_at,updated_at) VALUES ({b},'completed','bsrc',0,2000,1,0,'in','{src}',{NOW},{NOW})")).await;
            other_user_coin = Some(exec(&w, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES ({b},{bs},1,1,{vout},2000,'storage','change','P2PKH','{src}',1,0,X'51',{NOW},{NOW})")).await);
        }
        Some(exec(&w, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,spent_by,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES (1,{s},{spendable},{spent_by},1,{vout},2000,'storage','change','P2PKH','{src}',1,0,X'51',{NOW},{NOW})")).await)
    };
    let p = proof_row(&w, n, OLD).await;
    match kind {
        Kind::Check3 => proof_request(&w, n, "completed", Some(p)).await,
        Kind::Linked => proof_request(&w, n, "invalid", None).await,
        Kind::LinkedDoubleSpend => proof_request(&w, n, "doubleSpend", None).await,
        Kind::LinkedNoRequest => {}
    }
    exec(
        &w,
        &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
    )
    .await;
    (w, headers, t, coin, other_user_coin)
}

/// An output row as `spendable <0|1> spent_by <none|it|other>`.
async fn coin_state(w: &StorageSqlx, c: i64, t: i64) -> String {
    let (sp, by): (i64, Option<i64>) =
        ::sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(c)
            .fetch_one(w.pool())
            .await
            .unwrap();
    let by = match by {
        None => "none".to_string(),
        Some(b) if b == t => "it".to_string(),
        Some(_) => "other".to_string(),
    };
    format!("spendable {sp} spent_by {by}")
}

/// The failed row, its own output, the input coin and the request.
async fn state(w: &StorageSqlx, t: i64, coin: Option<i64>) -> String {
    let (status, link): (String, Option<i64>) =
        ::sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE transaction_id = ?")
            .bind(t)
            .fetch_one(w.pool())
            .await
            .unwrap();
    let own: i64 = ::sqlx::query_scalar("SELECT spendable FROM outputs WHERE transaction_id = ?")
        .bind(t)
        .fetch_one(w.pool())
        .await
        .unwrap();
    let coin = match coin {
        None => "none".to_string(),
        Some(c) => coin_state(w, c, t).await,
    };
    let req: Option<String> = ::sqlx::query_scalar(
        "SELECT r.status FROM proven_tx_reqs r JOIN transactions t ON t.txid = r.txid WHERE t.transaction_id = ?",
    )
    .bind(t)
    .fetch_optional(w.pool())
    .await
    .unwrap();
    let req = req.unwrap_or_else(|| "none".to_string());
    format!(
        "row {status}:{} | own output spendable {own} | input {coin} | req {req}",
        if link.is_some() { "linked" } else { "-" }
    )
}

/// The rule's answer for one cell: the row, its own output, its input
/// and the request after the pass, and whether the row is recovered.
fn expected(kind: Kind, row: Row) -> (String, bool) {
    let recovered = matches!(row, Row::Released | Row::TwoUsers) && kind != Kind::LinkedDoubleSpend;
    if recovered {
        return (
            "row unproven:- | own output spendable 0 | input spendable 0 spent_by it | req unmined"
                .to_string(),
            true,
        );
    }
    let input = match row {
        Row::Locked => "spendable 0 spent_by it",
        Row::Taken => "spendable 0 spent_by other",
        _ => "spendable 1 spent_by none",
    };
    let req = match kind {
        Kind::Check3 | Kind::Linked => "invalid",
        Kind::LinkedDoubleSpend => "doubleSpend",
        Kind::LinkedNoRequest => "none",
    };
    (
        format!("row failed:- | own output spendable 0 | input {input} | req {req}"),
        false,
    )
}

#[tokio::test]
async fn a_refuted_proof_recovers_only_the_wallets_own_released_row() {
    let mut n = 0u8;
    let mut wrong = Vec::new();
    for kind in [
        Kind::Check3,
        Kind::Linked,
        Kind::LinkedDoubleSpend,
        Kind::LinkedNoRequest,
    ] {
        for row in ROWS {
            n += 1;
            let (w, headers, t, coin, other_user_coin) = shape(kind, row, n).await;
            let start = state(&w, t, coin).await;
            MonitorStorage::review_status(&*w).await.unwrap();
            let after = state(&w, t, coin).await;
            let other = match other_user_coin {
                Some(c) => coin_state(&w, c, t).await,
                None => "none".to_string(),
            };
            println!(
                "TABLE {kind:?} {row:?}: {start} => {after} | other user's coin {other} ({} reads) | {}",
                headers.reads(),
                proof_state(&w, n).await
            );
            let (want, recovered) = expected(kind, row);
            if after != want {
                wrong.push(format!("{kind:?} {row:?}: got {after}, want {want}"));
            }
            if headers.reads() != 1 {
                wrong.push(format!(
                    "{kind:?} {row:?}: {} header reads",
                    headers.reads()
                ));
            }
            if other_user_coin.is_some() && other != "spendable 1 spent_by none" {
                wrong.push(format!(
                    "{kind:?} {row:?}: the other user's coin reads {other}"
                ));
            }
            if proof_state(&w, n).await != "proof rows 0 record none" {
                wrong.push(format!("{kind:?} {row:?}: {}", proof_state(&w, n).await));
            }
            // No cell leaves a request asking beside a `failed` row.
            if after.starts_with("row failed") && after.ends_with("req unmined") {
                wrong.push(format!(
                    "{kind:?} {row:?}: a request asks beside a failed row"
                ));
            }
            if recovered {
                let history = history(&w, n).await;
                if !history.contains("refutedProofRecoversFailedRow") {
                    wrong.push(format!("{kind:?} {row:?}: no recovery note: {history}"));
                }
            }
            // A second pass reads no header and moves nothing.
            MonitorStorage::review_status(&*w).await.unwrap();
            if headers.reads() != 1 || state(&w, t, coin).await != after {
                wrong.push(format!("{kind:?} {row:?}: a second pass moved the cell"));
            }
        }
    }
    println!("WITNESS recovery table: {n} cells");
    assert_eq!(n, 4 * 7);
    assert!(
        wrong.is_empty(),
        "cells off the rule:\n{}",
        wrong.join("\n")
    );
}

/// The end, walked once: the recovered row asked past the proof task's
/// count (no word written), then the host's retire: the row `failed`, its
/// own output as it was, the input it re-marked locked and scheduled for
/// the locked-input re-check, which gives it back on the chain's word.
#[tokio::test]
async fn a_recovered_row_ends_as_any_asked_transaction_does() {
    let n = 95;
    let (w, _headers, t, coin, _) = shape(Kind::Check3, Row::Released, n).await;
    MonitorStorage::review_status(&*w).await.unwrap();
    let recovered = state(&w, t, coin).await;
    let (txid, _, _, _) = mined(n);
    exec(
        &w,
        &format!("UPDATE proven_tx_reqs SET attempts = 1000 WHERE txid = '{txid}'"),
    )
    .await;
    w.synchronize_transaction_statuses().await.unwrap();
    MonitorStorage::review_status(&*w).await.unwrap();
    let past_count = state(&w, t, coin).await;
    let out = w
        .retire_undeliverable_txid(&MockWalletServices::new(), &txid, "invalid")
        .await
        .unwrap();
    let retired = state(&w, t, coin).await;
    let pending = w.locked_inputs_pending().await.unwrap();
    println!("WITNESS end: recovered {recovered}");
    println!("WITNESS end: past the count {past_count}");
    println!("WITNESS end: retired {out:?} {retired}, locked inputs scheduled {pending}");
    assert_eq!(
        recovered,
        "row unproven:- | own output spendable 0 | input spendable 0 spent_by it | req unmined"
    );
    assert_eq!(past_count, recovered, "a count writes no word");
    assert_eq!(
        retired,
        "row failed:- | own output spendable 0 | input spendable 0 spent_by it | req invalid"
    );
    assert_eq!(
        pending, 1,
        "the input it re-marked is scheduled for the re-check"
    );
    exec(
        &w,
        "UPDATE locked_input_checks SET next_check_at = '2000-01-01T00:00:00Z'",
    )
    .await;
    let services = MockWalletServices::builder()
        .is_utxo_response(MockResponse::Success(true))
        .build();
    let report = w.recheck_locked_inputs(&services, 10, true).await.unwrap();
    let rechecked = state(&w, t, coin).await;
    println!("WITNESS end: the re-check {report:?} {rechecked}");
    assert_eq!(
        rechecked,
        "row failed:- | own output spendable 0 | input spendable 1 spent_by none | req invalid",
        "the chain's word gives the coin back"
    );
}
