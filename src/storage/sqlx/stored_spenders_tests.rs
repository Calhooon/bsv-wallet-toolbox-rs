//! `stored_spenders_of`: which transactions of a store that are not failed
//! spend an outpoint, by their bytes.

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use sqlx::Row;

use super::reorg_tests::{seed_user, storage};
use super::stored_spenders::{outpoint_bytes, stored_spenders_of, StoredSpenders};
use super::StorageSqlx;
use crate::storage::traits::WalletStorageWriter;

const SOURCE: &str = "0102030405060708091011121314151617181920212223242526272829303132";

/// A transaction spending each of `inputs`, paying one output of `script`.
fn tx_of(inputs: &[(&str, u32)], script_hex: &str) -> (Vec<u8>, String) {
    let mut tx = Transaction::new();
    for (txid, vout) in inputs {
        let mut input = TransactionInput::new(txid.to_string(), *vout);
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        tx.inputs.push(input);
    }
    tx.outputs.push(TransactionOutput {
        satoshis: Some(1000),
        locking_script: LockingScript::from_hex(script_hex).unwrap(),
        change: false,
    });
    let txid = tx.id();
    (tx.to_binary(), txid)
}

const P2PKH: &str = "76a914000000000000000000000000000000000000000088ac";

async fn insert_tx(
    s: &StorageSqlx,
    user_id: i64,
    txid: &str,
    status: &str,
    raw: Option<&[u8]>,
    proven_tx_id: Option<i64>,
) -> i64 {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
         version, lock_time, is_outgoing, raw_tx, proven_tx_id, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'a stored spender', 0, 1, 0, 1, ?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(txid)
    .bind(status)
    .bind(format!("ref-{txid}"))
    .bind(raw)
    .bind(proven_tx_id)
    .bind(now)
    .bind(now)
    .execute(s.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn insert_proven(s: &StorageSqlx, txid: &str, raw: &[u8]) -> i64 {
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, 100, 0, ?, ?, x'00', ?)",
    )
    .bind(txid)
    .bind("00".repeat(32))
    .bind("00".repeat(32))
    .bind(raw)
    .execute(s.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn spenders(s: &StorageSqlx, txid: &str, vout: u32) -> StoredSpenders {
    s.stored_spenders(txid, vout).await.unwrap()
}

fn ids(a: &StoredSpenders) -> Vec<i64> {
    a.spenders.iter().map(|x| x.transaction_id).collect()
}

#[test]
fn the_outpoint_is_the_reversed_txid_then_the_index_little_endian() {
    let b = outpoint_bytes(SOURCE, 0x0102_0304).unwrap();
    assert_eq!(b[0], 0x32);
    assert_eq!(b[31], 0x01);
    assert_eq!(&b[32..], &[0x04, 0x03, 0x02, 0x01]);
    assert!(outpoint_bytes("00", 0).is_err());
    assert!(outpoint_bytes(&"zz".repeat(32), 0).is_err());
}

#[tokio::test]
async fn an_outpoint_spent_names_its_spender() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    let (raw, txid) = tx_of(&[(SOURCE, 1)], P2PKH);
    let id = insert_tx(&s, user_id, &txid, "unproven", Some(&raw), None).await;

    let a = spenders(&s, SOURCE, 1).await;
    assert_eq!(ids(&a), vec![id]);
    assert_eq!(a.spenders[0].txid.as_deref(), Some(txid.as_str()));
    assert_eq!(a.spenders[0].status, "unproven");
    assert_eq!(a.unreadable, 0);
    assert!(!a.none_held());
}

#[tokio::test]
async fn an_outpoint_not_spent_has_no_spender() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    let (raw, txid) = tx_of(&[(SOURCE, 1)], P2PKH);
    insert_tx(&s, user_id, &txid, "completed", Some(&raw), None).await;

    // Another index of the same transaction, another transaction.
    let other = spenders(&s, SOURCE, 0).await;
    assert_eq!(other, StoredSpenders::default());
    assert!(other.none_held());
    assert!(spenders(&s, &"ab".repeat(32), 1).await.none_held());
}

#[tokio::test]
async fn the_outpoints_bytes_in_an_output_script_are_no_spend() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    // A push of the outpoint's 36 bytes in an output script, then a drop.
    let script = format!(
        "24{}75{}",
        hex::encode(outpoint_bytes(SOURCE, 2).unwrap()),
        P2PKH
    );
    let (raw, txid) = tx_of(&[(&"cd".repeat(32), 0)], &script);
    insert_tx(&s, user_id, &txid, "unproven", Some(&raw), None).await;
    let needle = outpoint_bytes(SOURCE, 2).unwrap();
    assert!(raw.windows(36).any(|w| w == needle.as_slice()));

    // The byte search returns it; the parse rules it out.
    let found = sqlx::query("SELECT COUNT(*) AS n FROM transactions WHERE instr(raw_tx, ?) > 0")
        .bind(needle.to_vec())
        .fetch_one(s.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(found, 1);
    assert!(spenders(&s, SOURCE, 2).await.none_held());
}

#[tokio::test]
async fn a_failed_spender_is_no_spender() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    let (raw, txid) = tx_of(&[(SOURCE, 3)], P2PKH);
    let id = insert_tx(&s, user_id, &txid, "failed", Some(&raw), None).await;
    assert!(spenders(&s, SOURCE, 3).await.none_held());

    sqlx::query("UPDATE transactions SET status = 'nosend' WHERE transaction_id = ?")
        .bind(id)
        .execute(s.writer())
        .await
        .unwrap();
    assert_eq!(ids(&spenders(&s, SOURCE, 3).await), vec![id]);
}

#[tokio::test]
async fn a_spender_whose_bytes_are_only_in_its_proof_row_is_read() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    let (raw, txid) = tx_of(&[(SOURCE, 4)], P2PKH);
    let proven = insert_proven(&s, &txid, &raw).await;
    // A completed row the purge cleared: its bytes are the proof row's.
    let id = insert_tx(&s, user_id, &txid, "completed", None, Some(proven)).await;

    let a = spenders(&s, SOURCE, 4).await;
    assert_eq!(ids(&a), vec![id]);
    assert_eq!(a.unreadable, 0);
}

#[tokio::test]
async fn a_transaction_with_no_bytes_is_counted_unreadable() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    // An action created and not signed: no bytes anywhere.
    insert_tx(&s, user_id, &"ef".repeat(32), "unsigned", None, None).await;
    // A failed one with no bytes is not counted.
    insert_tx(&s, user_id, &"fe".repeat(32), "failed", None, None).await;

    let a = spenders(&s, SOURCE, 0).await;
    assert!(a.spenders.is_empty());
    assert_eq!(a.unreadable, 1);
    assert!(!a.none_held());
}

#[tokio::test]
async fn bytes_carrying_the_outpoint_that_do_not_parse_are_unreadable() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    let mut raw = outpoint_bytes(SOURCE, 5).unwrap().to_vec();
    raw.extend_from_slice(&[0xff; 3]);
    insert_tx(&s, user_id, &"aa".repeat(32), "unproven", Some(&raw), None).await;

    let a = spenders(&s, SOURCE, 5).await;
    assert!(a.spenders.is_empty());
    assert_eq!(a.unreadable, 1);
}

/// The cost of one search at 10,000 and 100,000 stored transactions, on a
/// file store, on a reader connection and on the writer. Run by hand:
/// `cargo test --lib stored_spenders_cost -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn stored_spenders_cost() {
    for n in [10_000usize, 100_000] {
        let dir = std::env::temp_dir().join(format!("stored-spenders-cost-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let s = StorageSqlx::open(path.to_str().unwrap()).await.unwrap();
        s.migrate("cost", &"0".repeat(64)).await.unwrap();
        s.make_available().await.unwrap();
        let (user_id, _) = seed_user(&s).await;

        let mut w = s.write_tx().await.unwrap();
        let now = chrono::Utc::now();
        for i in 0..n {
            // Two inputs, one output: 128 bytes.
            let a = format!("{:064x}", i * 2 + 1);
            let b = format!("{:064x}", i * 2 + 2);
            let (raw, txid) = tx_of(&[(&a, 0), (&b, 1)], P2PKH);
            sqlx::query(
                "INSERT INTO transactions (user_id, txid, status, reference, description, \
                 satoshis, version, lock_time, is_outgoing, raw_tx, created_at, updated_at) \
                 VALUES (?, ?, 'completed', ?, 'cost', 0, 1, 0, 1, ?, ?, ?)",
            )
            .bind(user_id)
            .bind(&txid)
            .bind(format!("r{i}"))
            .bind(raw)
            .bind(now)
            .bind(now)
            .execute(&mut *w)
            .await
            .unwrap();
        }
        w.commit().await.unwrap();
        let (rows, bytes): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), SUM(length(raw_tx)) FROM transactions WHERE status != 'failed'",
        )
        .fetch_one(s.pool())
        .await
        .unwrap();

        // One outpoint spent by the last row, one spent by none.
        let last = format!("{:064x}", n * 2);
        let mut timings = Vec::new();
        for (label, txid, vout) in [("spent", last.as_str(), 1u32), ("free", SOURCE, 0)] {
            let mut reader = s.pool().acquire().await.unwrap();
            let _ = stored_spenders_of(&mut reader, txid, vout).await.unwrap();
            let t = std::time::Instant::now();
            let ans = stored_spenders_of(&mut reader, txid, vout).await.unwrap();
            let outside = t.elapsed();
            drop(reader);
            let mut w = s.write_tx().await.unwrap();
            let t = std::time::Instant::now();
            let ans_w = stored_spenders_of(&mut w, txid, vout).await.unwrap();
            let inside = t.elapsed();
            w.commit().await.unwrap();
            assert_eq!(ans, ans_w);
            assert_eq!(ans.spenders.len(), usize::from(label == "spent"));
            timings.push(format!(
                "{label}: reader {:.1} ms, writer {:.1} ms",
                outside.as_secs_f64() * 1e3,
                inside.as_secs_f64() * 1e3
            ));
        }
        println!(
            "COST n={n}: {rows} rows not failed, {bytes} bytes of raw_tx; {}",
            timings.join("; ")
        );
        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
