//! Opening a store that needs nothing writes nothing.
//!
//! A host's command that opens the store while another process holds the
//! file's write lock answered `Migration error: 006_held_receipts: ...
//! database is locked` where every door answers the store's busy word:
//! migration 006 is a data statement (an `UPDATE`) run at every
//! `make_available()`, and an `UPDATE` takes the file's write lock even when
//! no row matches. Now a data statement runs only after a read of its own
//! condition finds a row it would change, and a migration that must write on
//! a busy store answers `StoreBusy`.
//!
//! Every witness here is a FILE-BACKED store in a temp dir (an in-memory
//! store has no file lock to hold), with a second connection of its own
//! holding `BEGIN IMMEDIATE` on the file.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{AbortActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::sqlx::STORE_BUSY_CEILING_MS;
use bsv_wallet_toolbox_rs::{
    Error, Monitor, StorageSqlx, Wallet, WalletStorageReader, WalletStorageWriter,
};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection, SqliteConnection};

/// A wait well under the store's ceiling: an open that needs no write lock
/// takes a few milliseconds, one that waits on it takes the whole ceiling.
const QUICK: Duration = Duration::from_millis(STORE_BUSY_CEILING_MS / 10);

/// The identity of the store's settings row.
const STORAGE_KEY: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// The tables a store at this schema holds (migrations 001 to 005).
const TABLES: &[&str] = &[
    "settings",
    "users",
    "transactions",
    "outputs",
    "output_baskets",
    "proven_txs",
    "proven_tx_reqs",
    "certificates",
    "certificate_fields",
    "tx_labels",
    "tx_labels_map",
    "output_tags",
    "output_tags_map",
    "commissions",
    "sync_states",
    "monitor_events",
    "broadcast_seen",
    "broadcast_prefs",
    "locked_input_checks",
    "monitor_state",
    "proof_root_checks",
];

fn db_path(dir: &tempfile::TempDir) -> String {
    dir.path().join("wallet.db").to_string_lossy().to_string()
}

/// A raw connection of its own on `path`: no busy wait.
async fn raw(path: &str) -> SqliteConnection {
    SqliteConnectOptions::from_str(&format!("sqlite:{path}"))
        .unwrap()
        .pragma("busy_timeout", "0")
        .create_if_missing(true)
        .connect()
        .await
        .unwrap()
}

/// Another process's write lock on `path`: a connection of its own holding
/// `BEGIN IMMEDIATE`. Released by [`release`].
async fn hold_write_lock(path: &str) -> SqliteConnection {
    let mut other = raw(path).await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut other)
        .await
        .unwrap();
    other
}

async fn release(mut other: SqliteConnection) {
    sqlx::query("ROLLBACK").execute(&mut other).await.unwrap();
    other.close().await.unwrap();
}

/// Close both pools of a store, so the next open is the next process's.
async fn close(storage: StorageSqlx) {
    storage.writer().close().await;
    storage.pool().close().await;
}

/// A store at this schema on `path`, made through its own doors, closed.
async fn current_store(path: &str) {
    let storage = StorageSqlx::open(path).await.unwrap();
    storage
        .migrate("open-writes-nothing", STORAGE_KEY)
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    close(storage).await;
}

/// A received row `unproven` with no proof linked, its request `unsent`, no
/// record of a take and no checked proof: what 006 moves. PLANTED: on this
/// line the internalize door writes such a receipt held (`sending`), and no
/// door leaves it `unproven`; a store written by 0.14.0-beta.8 to beta.10
/// holds it so.
async fn plant_held_receipt(path: &str) -> String {
    let txid = "ab".repeat(32);
    let mut c = raw(path).await;
    sqlx::query("INSERT INTO users (identity_key, active_storage) VALUES (?, ?)")
        .bind("03".to_string() + &"cd".repeat(32))
        .bind(STORAGE_KEY)
        .execute(&mut c)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid) \
         VALUES (1, 'unproven', 'planted-ref', 0, 1000, 'a planted receipt', ?)",
    )
    .bind(&txid)
    .execute(&mut c)
    .await
    .unwrap();
    sqlx::query("INSERT INTO proven_tx_reqs (status, txid, raw_tx) VALUES ('unsent', ?, x'00')")
        .bind(&txid)
        .execute(&mut c)
        .await
        .unwrap();
    c.close().await.unwrap();
    txid
}

async fn status_of(path: &str, txid: &str) -> String {
    let mut c = raw(path).await;
    let status: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(&mut c)
        .await
        .unwrap();
    c.close().await.unwrap();
    status
}

/// Open and make available: what a host's command runs at open.
async fn open_and_make_available(path: &str) -> Result<StorageSqlx, Error> {
    let storage = StorageSqlx::open(path).await?;
    storage.make_available().await?;
    Ok(storage)
}

fn assert_busy(err: &Error, what: &str) {
    assert!(
        matches!(err, Error::StoreBusy { .. }),
        "{what}: the store's busy answer, got {err:?}"
    );
}

/// (a) A store at this schema with nothing to move opens and is made
/// available under another process's write lock, at once.
#[tokio::test]
async fn a_current_store_with_nothing_to_move_opens_under_a_held_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    current_store(&path).await;

    let other = hold_write_lock(&path).await;
    let started = Instant::now();
    let opened = open_and_make_available(&path).await;
    let waited = started.elapsed();
    println!("(a) open and make_available under the lock: {waited:?}");
    let storage = opened.expect("a store that needs nothing opens under a held lock");
    assert!(
        waited < QUICK,
        "waited {waited:?}, the lock's ceiling is {STORE_BUSY_CEILING_MS} ms"
    );
    assert_eq!(storage.get_settings().storage_identity_key, STORAGE_KEY);
    close(storage).await;
    release(other).await;
}

/// (b) The same store with one row 006 would move: the open waits to the
/// ceiling and answers the store's busy word; released, the next open moves
/// the row.
#[tokio::test]
async fn a_store_with_a_row_to_move_answers_busy_under_a_held_lock_then_moves_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    current_store(&path).await;
    let txid = plant_held_receipt(&path).await;

    let other = hold_write_lock(&path).await;
    let started = Instant::now();
    let err = open_and_make_available(&path)
        .await
        .err()
        .expect("006 must write and the file is locked");
    let waited = started.elapsed();
    println!("(b) open and make_available under the lock: {waited:?}: {err}");
    assert_busy(&err, "006 under a held lock");
    assert!(
        waited >= Duration::from_millis(STORE_BUSY_CEILING_MS).saturating_sub(QUICK),
        "the write waited the store's ceiling: {waited:?}"
    );
    release(other).await;
    assert_eq!(status_of(&path, &txid).await, "unproven", "nothing written");

    let storage = open_and_make_available(&path)
        .await
        .expect("released, it opens");
    close(storage).await;
    assert_eq!(
        status_of(&path, &txid).await,
        "sending",
        "the next open moves the row"
    );
}

/// (c) A fresh file is opened and migrated with no lock held, every table
/// there; opened while another process holds the lock, it answers the
/// store's busy word.
#[tokio::test]
async fn a_fresh_file_migrates_unlocked_and_answers_busy_locked() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let storage = StorageSqlx::open(&path).await.unwrap();
    storage
        .migrate("open-writes-nothing", STORAGE_KEY)
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    close(storage).await;
    let mut c = raw(&path).await;
    for table in TABLES {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(n, 1, "table {table}");
    }
    c.close().await.unwrap();

    let fresh = tempfile::tempdir().unwrap();
    let path = db_path(&fresh);
    let other = hold_write_lock(&path).await;
    let started = Instant::now();
    let answer = match StorageSqlx::open(&path).await {
        Ok(storage) => {
            let migrated = storage.migrate("open-writes-nothing", STORAGE_KEY).await;
            close(storage).await;
            migrated.err().map(|e| ("migrate", e))
        }
        Err(e) => Some(("open", e)),
    };
    let waited = started.elapsed();
    let (step, err) = answer.expect("a fresh file must be written and the file is locked");
    println!("(c) a fresh file under the lock: {step} after {waited:?}: {err}");
    assert_busy(&err, step);
    release(other).await;
}

/// (d) A `Wallet` and a `Monitor` over a current store are constructed under
/// another process's write lock; the write each makes next answers busy.
#[tokio::test]
async fn a_wallet_and_a_monitor_construct_under_a_held_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let root = PrivateKey::from_bytes(&[7u8; 32]).unwrap();
    {
        let storage = StorageSqlx::open(&path).await.unwrap();
        storage
            .migrate("open-writes-nothing", STORAGE_KEY)
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        // The wallet's user row is written once, at the open that made the store.
        let wallet = Wallet::new(
            Some(root.clone()),
            storage.clone(),
            MockWalletServices::new(),
        )
        .await
        .unwrap();
        drop(wallet);
        close(storage).await;
        // An unsigned action of the wallet's user, for its next write (the
        // abort's) to meet the lock. Planted: a create needs funded change.
        let mut c = raw(&path).await;
        sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description) \
             VALUES (1, 'unsigned', 'planted-unsigned', 1, 0, 'an unsigned action')",
        )
        .execute(&mut c)
        .await
        .unwrap();
        c.close().await.unwrap();
    }

    let other = hold_write_lock(&path).await;
    let started = Instant::now();
    let storage = open_and_make_available(&path)
        .await
        .expect("opens under the lock");
    let monitor_store = Arc::new(storage.clone());
    let wallet = Wallet::new(Some(root.clone()), storage, MockWalletServices::new())
        .await
        .expect("a wallet whose user is stored constructs under the lock");
    let monitor = Monitor::new(
        Arc::clone(&monitor_store),
        Arc::new(MockWalletServices::new()),
    );
    let waited = started.elapsed();
    println!("(d) open, Wallet::new and Monitor::new under the lock: {waited:?}");
    assert!(waited < QUICK, "waited {waited:?}");

    let started = Instant::now();
    let err = wallet
        .abort_action(
            AbortActionArgs {
                reference: "planted-unsigned".to_string(),
            },
            Some("example.com"),
        )
        .await
        .expect_err("the wallet's next write meets the lock");
    println!(
        "(d) the wallet's next write after {:?}: {err}",
        started.elapsed()
    );
    assert!(
        err.to_string().to_lowercase().contains("busy"),
        "the wallet's busy word: {err}"
    );

    let started = Instant::now();
    let err = monitor_store
        .log_monitor_event("open-writes-nothing", None)
        .await
        .expect_err("the monitor's next write meets the lock");
    println!(
        "(d) the monitor's next write after {:?}: {err}",
        started.elapsed()
    );
    assert_busy(&err, "the monitor's next write");
    drop(monitor);
    drop(wallet);
    release(other).await;
}

/// The table of `report-full.md`, by a run: every statement of every
/// migration, sent raw on a connection of its own (no busy wait) to a store
/// already at this schema while another connection holds `BEGIN IMMEDIATE`.
/// A schema statement with its own `IF NOT EXISTS` takes no write lock
/// there; 006's `UPDATE` does, with no row to move; its read does not.
#[tokio::test]
async fn each_migration_statement_on_a_current_store_takes_the_lock_or_not() {
    const FILES: &[(&str, &str)] = &[
        (
            "001_initial",
            include_str!("../src/storage/sqlx/migrations/001_initial.sql"),
        ),
        (
            "002_broadcast_seen",
            include_str!("../src/storage/sqlx/migrations/002_broadcast_seen.sql"),
        ),
        (
            "003_locked_input_checks",
            include_str!("../src/storage/sqlx/migrations/003_locked_input_checks.sql"),
        ),
        (
            "004_monitor_state",
            include_str!("../src/storage/sqlx/migrations/004_monitor_state.sql"),
        ),
        (
            "005_proof_root_checks",
            include_str!("../src/storage/sqlx/migrations/005_proof_root_checks.sql"),
        ),
        (
            "006_held_receipts",
            include_str!("../src/storage/sqlx/migrations/006_held_receipts.sql"),
        ),
    ];
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    current_store(&path).await;
    let other = hold_write_lock(&path).await;
    let mut c = raw(&path).await;
    for (name, sql) in FILES {
        let body: String = sql
            .lines()
            .filter(|l| !l.trim().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for statement in body.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let head: String = statement
                .split_whitespace()
                .take(6)
                .collect::<Vec<_>>()
                .join(" ");
            let schema = statement.to_ascii_uppercase().starts_with("CREATE");
            let outcome = sqlx::query(statement).execute(&mut c).await;
            let locked = outcome
                .as_ref()
                .is_err_and(|e| e.to_string().contains("code: 5"));
            println!(
                "| {name} | {head} | {} | lock: {locked} |",
                if schema { "schema" } else { "data" }
            );
            assert_eq!(locked, !schema, "{name}: {head}: {outcome:?}");
            if !schema {
                let condition = statement.split_once("\nWHERE ").unwrap().1;
                let read = format!("SELECT EXISTS (SELECT 1 FROM transactions WHERE {condition})");
                let pending: bool = sqlx::query_scalar(&read).fetch_one(&mut c).await.unwrap();
                println!("| {name} | its read ahead | read | lock: false, pending: {pending} |");
                assert!(!pending);
            }
        }
    }
    c.close().await.unwrap();
    release(other).await;
}
