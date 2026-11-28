//! The release search's mark (`stored_spenders::MARK`): its plan seeks and
//! never walks `transactions`, it answers what the statement it replaced
//! answered, and it moves on every kind of write of a transaction row that
//! moved it before.

use chrono::{DateTime, Duration, Utc};

use super::reorg_tests::{seed_user, storage};
use super::stored_spenders::{mark_of, MARK};
use super::StorageSqlx;

/// The statement the mark replaced: one walk of every row of `transactions`.
/// Kept here as the oracle of the mark's meaning.
const WALKED_MARK: &str = "SELECT COALESCE(MAX(transaction_id), 0), \
     COALESCE(CAST(MAX(updated_at) AS TEXT), '') FROM transactions \
     WHERE transaction_id != ?";

async fn walked(s: &StorageSqlx, except: Option<i64>) -> (i64, String) {
    sqlx::query_as(WALKED_MARK)
        .bind(except.unwrap_or(-1))
        .fetch_one(s.writer())
        .await
        .unwrap()
}

async fn sought(s: &StorageSqlx, except: Option<i64>) -> (i64, String) {
    let mut conn = s.writer().acquire().await.unwrap();
    mark_of(&mut conn, except).await.unwrap()
}

async fn insert(s: &StorageSqlx, user: i64, n: i64, at: DateTime<Utc>) -> i64 {
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, \
         version, lock_time, description, txid, raw_tx, input_beef, created_at, updated_at) \
         VALUES (?, 'completed', ?, 0, 1000, 1, 0, 'a row of the mark', ?, x'00', x'00', ?, ?)",
    )
    .bind(user)
    .bind(format!("mark-ref-{n}"))
    .bind(format!("{n:064x}"))
    .bind(at)
    .bind(at)
    .execute(s.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

/// Every line of `EXPLAIN QUERY PLAN` of the mark.
async fn plan(s: &StorageSqlx) -> Vec<String> {
    sqlx::query_as::<_, (i64, i64, i64, String)>(&format!("EXPLAIN QUERY PLAN {MARK}"))
        .bind(-1i64)
        .bind(-1i64)
        .fetch_all(s.writer())
        .await
        .unwrap()
        .into_iter()
        .map(|(_, _, _, detail)| detail)
        .collect()
}

/// The mark is read by seeks: no line of its plan scans `transactions`, and
/// the stamp half seeks the index of migration 007. The statement it
/// replaced is shown to scan, on the same store, so the check reads plans.
#[tokio::test]
async fn the_mark_seeks_and_never_scans_transactions() {
    let s = storage().await;
    let lines = plan(&s).await;
    assert!(
        lines.iter().all(|l| !l.starts_with("SCAN transactions")),
        "the mark scans transactions: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("USING COVERING INDEX idx_transactions_updated_at")),
        "the stamp half seeks the index: {lines:?}"
    );
    let walked: Vec<String> =
        sqlx::query_as::<_, (i64, i64, i64, String)>(&format!("EXPLAIN QUERY PLAN {WALKED_MARK}"))
            .bind(-1i64)
            .fetch_all(s.writer())
            .await
            .unwrap()
            .into_iter()
            .map(|(_, _, _, d)| d)
            .collect();
    assert!(
        walked.iter().any(|l| l.starts_with("SCAN transactions")),
        "the oracle walks the table: {walked:?}"
    );
}

/// One kind of write of a transaction row, in the shape of the store's own
/// statement (named), bound to `(at, row)`.
struct Write {
    name: &'static str,
    sql: &'static str,
}

const WRITES: &[Write] = &[
    Write {
        name: "processAction signs (process_action.rs: txid, status, raw_tx, input_beef)",
        sql: "UPDATE transactions SET txid = txid, status = 'sending', raw_tx = x'01', \
              input_beef = NULL, updated_at = ? WHERE transaction_id = ?",
    },
    Write {
        name: "a status word (process_action, abort_action, the monitor's tasks, poisoned_chain)",
        sql: "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE transaction_id = ?",
    },
    Write {
        name: "createAction stores the input BEEF (create_action.rs)",
        sql: "UPDATE transactions SET input_beef = x'02', updated_at = ? WHERE transaction_id = ?",
    },
    Write {
        name: "createAction's satoshis (create_action.rs)",
        sql: "UPDATE transactions SET satoshis = 7, updated_at = ? WHERE transaction_id = ?",
    },
    Write {
        name: "internalizeAction's description of a known row (internalize_action.rs)",
        sql:
            "UPDATE transactions SET description = 'again', updated_at = ? WHERE transaction_id = ?",
    },
    Write {
        name: "a proof links the row (storage_sqlx.rs, invalid_beside_proof.rs)",
        sql: "UPDATE transactions SET status = 'completed', proven_tx_id = NULL, updated_at = ? \
              WHERE transaction_id = ?",
    },
    Write {
        name: "the purge of completed data (storage_sqlx.rs)",
        sql: "UPDATE transactions SET raw_tx = NULL, input_beef = NULL, updated_at = ? \
              WHERE transaction_id = ?",
    },
    Write {
        name: "sync's merge of a row (sync.rs, the whole row)",
        sql: "UPDATE transactions SET description = 'merged', is_outgoing = 1, updated_at = ? \
              WHERE transaction_id = ?",
    },
];

/// A store of five rows; the mark read leaving out row 2. Each write moves
/// the mark when it lands on a row the mark reads (row 1, the oldest, and
/// row 5, the newest) and leaves it still when it lands on row 2, the row
/// left out; an insert and a delete of the newest row move it too. At every
/// step the seeks answer what the walk answered.
#[tokio::test]
async fn the_mark_moves_on_every_kind_of_write() {
    let mut failed = Vec::new();
    let mut cases: Vec<(String, &'static str, usize, bool)> = Vec::new();
    for w in WRITES {
        for (row, moves) in [(0usize, true), (4, true), (1, false)] {
            cases.push((format!("{} on row {}", w.name, row + 1), w.sql, row, moves));
        }
    }
    cases.push((
        "an insert (createAction, internalizeAction, sync)".into(),
        "INSERT",
        0,
        true,
    ));
    cases.push(("a delete of the newest row".into(), "DELETE", 4, true));
    cases.push((
        "sync's merge that leaves updated_at (sync.rs), blind before and after".into(),
        "UPDATE transactions SET proven_tx_id = NULL, raw_tx = COALESCE(raw_tx, x'03') \
         WHERE transaction_id = ? AND ? IS NOT NULL",
        0,
        false,
    ));
    for (name, sql, row, moves) in cases {
        let s = storage().await;
        let (user, _) = seed_user(&s).await;
        let base = Utc::now() - Duration::hours(1);
        let mut ids = Vec::new();
        for n in 0..5 {
            ids.push(insert(&s, user, n, base + Duration::seconds(n)).await);
        }
        let except = Some(ids[1]);
        let before = sought(&s, except).await;
        if before != walked(&s, except).await {
            println!("FAIL {name}: the seeks and the walk differ before the write");
            failed.push(name);
            continue;
        }
        let at = Utc::now();
        match sql {
            "INSERT" => {
                insert(&s, user, 99, at).await;
            }
            "DELETE" => {
                sqlx::query("DELETE FROM transactions WHERE transaction_id = ?")
                    .bind(ids[row])
                    .execute(s.writer())
                    .await
                    .unwrap();
            }
            sql if sql.contains("AND ? IS NOT NULL") => {
                sqlx::query(sql)
                    .bind(ids[row])
                    .bind(1i64)
                    .execute(s.writer())
                    .await
                    .unwrap();
            }
            sql => {
                sqlx::query(sql)
                    .bind(at)
                    .bind(ids[row])
                    .execute(s.writer())
                    .await
                    .unwrap();
            }
        }
        let after = sought(&s, except).await;
        let oracle = walked(&s, except).await;
        let moved = after != before;
        let ok = after == oracle && moved == moves;
        println!(
            "{} {name}: moved {moved}, the walk agrees {}",
            if ok { "ok  " } else { "FAIL" },
            after == oracle
        );
        if !ok {
            failed.push(name);
        }
    }
    assert!(failed.is_empty(), "rows red: {failed:?}");
}

/// The row left out holds the newest stamp alone, a row ties it, and the
/// table holds only that row or none: the seeks answer what the walk did.
#[tokio::test]
async fn the_mark_answers_the_walk_at_its_edges() {
    let s = storage().await;
    assert_eq!(sought(&s, None).await, walked(&s, None).await, "empty");
    assert_eq!(sought(&s, None).await, (0, String::new()));
    let (user, _) = seed_user(&s).await;
    let base = Utc::now();
    let only = insert(&s, user, 0, base).await;
    assert_eq!(
        sought(&s, Some(only)).await,
        walked(&s, Some(only)).await,
        "only the row left out"
    );
    assert_eq!(sought(&s, Some(only)).await, (0, String::new()));
    let older = insert(&s, user, 1, base - Duration::seconds(5)).await;
    assert_eq!(
        sought(&s, Some(only)).await,
        walked(&s, Some(only)).await,
        "the newest left out"
    );
    let tie = insert(&s, user, 2, base).await;
    for except in [None, Some(only), Some(older), Some(tie)] {
        assert_eq!(
            sought(&s, except).await,
            walked(&s, except).await,
            "{except:?}"
        );
    }
}
