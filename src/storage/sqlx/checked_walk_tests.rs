//! A competitor's checked proof ends every row of its txid: the
//! descendants of every ended row are walked.
//!
//! The store of `held_receipt_scope_tests`.

use chrono::Utc;

use super::held_receipt_scope_tests::{output, read, row, seed, write_word, Cell, Writer};
use super::reorg_tests::txid_of;

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
