//! A competitor's checked proof ends every row of its txid, once a
//! request: the descendants of every ended row are walked, and the proof
//! task asks for the competitor and retires the request once, however many
//! users hold a row of the txid.
//!
//! The store of `held_receipt_scope_tests`.

use std::sync::Arc;

use chrono::Utc;

use super::held_receipt_scope_tests::{
    output, r_raw, read, row, seed, write_word, Cell, Writer, HEIGHT,
};
use super::reorg_tests::{single_leaf_bump, txid_of};
use super::void_lineage_tests::serving;
use crate::services::WalletServices;
use crate::storage::MonitorStorage;

/// Y's own child G, spending Y's payment: its row, output and request.
async fn y_child(c: &Cell) -> String {
    let g_raw = super::checked_mark_tests::spending(&[(&c.p, 1)], &[500]);
    let g = txid_of(&g_raw);
    let basket: i64 = sqlx::query_scalar("SELECT basket_id FROM outputs WHERE output_id = ?")
        .bind(c.y_out)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    let g_row = row(&c.s, &c.y, &g, "sending", &g_raw, true).await;
    sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
        .bind(g_row)
        .bind(c.y_out)
        .execute(c.s.writer())
        .await
        .unwrap();
    output(
        &c.s,
        &c.y,
        basket,
        g_row,
        &g,
        0,
        500,
        "change",
        (true, None),
    )
    .await;
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) \
         VALUES (?, 'unsent', 0, '{}', 0, '{}', ?, ?, ?)",
    )
    .bind(&g)
    .bind(&g_raw)
    .bind(Utc::now())
    .bind(Utc::now())
    .execute(c.s.writer())
    .await
    .unwrap();
    g
}

/// On a competitor's checked proof every row of P ends, and the received
/// row's own descendants are walked with it: Y's child G is retired.
#[tokio::test]
async fn a_checked_proof_walks_the_descendants_of_every_ended_row() {
    let c = seed("unproven").await;
    let g = y_child(&c).await;
    write_word(&c, Writer::Competitor).await;
    let got = read(&c).await;
    assert_eq!(
        (got.x_row.as_str(), got.y_row.as_str()),
        ("failed", "failed")
    );
    assert_eq!(
        (
            super::checked_mark_tests::tx_word(&c.s, &g).await,
            super::checked_mark_tests::req_word(&c.s, &g).await
        ),
        ("failed".to_string(), "invalid".to_string()),
        "Y's child is retired with the row it spends"
    );
}

/// Two rows of one txid (X's action and Y's receipt): the proof task asks
/// for the competitor once and retires the request once.
#[tokio::test]
async fn the_ask_retires_once_a_request() {
    let c = seed("sending").await;
    let r = txid_of(&r_raw());
    super::void_lineage_tests::apply_rejected_naming(&c.s, &c.p, &r).await;
    let mock = Arc::new(
        super::checked_mark_tests::status_unknown(serving(
            &r_raw(),
            Some(single_leaf_bump(HEIGHT, &r)),
        ))
        .build(),
    );
    crate::storage::traits::WalletStorageProvider::set_services(
        &c.s,
        mock.clone() as Arc<dyn WalletServices>,
    );
    let before: i64 = sqlx::query_scalar("SELECT attempts FROM proven_tx_reqs WHERE txid = ?")
        .bind(&c.p)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    let out = c.s.synchronize_transaction_statuses().await.unwrap();
    let words = out.iter().filter(|t| t.txid == c.p).count();
    let after: i64 = sqlx::query_scalar("SELECT attempts FROM proven_tx_reqs WHERE txid = ?")
        .bind(&c.p)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    assert_eq!(
        (words, mock.call_count("get_raw_tx"), after - before),
        (1, 1, 0),
        "one word, one ask and the attempts as the request held them"
    );
    let got = read(&c).await;
    assert_eq!(
        (got.x_row.as_str(), got.y_row.as_str()),
        ("failed", "failed")
    );
}
