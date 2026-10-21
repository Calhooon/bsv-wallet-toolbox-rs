//! The store's write lock (bsv-wallet-cli #44).
//!
//! A served `createAction` answered `database is locked (code 517)`,
//! `SQLITE_BUSY_SNAPSHOT`: its write transaction opened with a deferred
//! `BEGIN`, read under a WAL snapshot, and tried to write after another
//! pooled connection (the monitor's proof task) had committed. `busy_timeout`
//! never applies to 517. The store now writes through one writer connection,
//! every transaction opens `BEGIN IMMEDIATE`, and the readers are
//! `query_only`. These tests reproduce the fault on raw connections, show the
//! same shape through `write_tx` waits and commits, and pin the enforcement.

#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::time::{Duration, Instant};

use bsv_wallet_toolbox_rs::{Error, StorageSqlx};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection, SqliteConnection};

/// A WAL database file under a fresh temp directory, with table `t`.
async fn wal_file() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.db").to_string_lossy().to_string();
    let mut c = raw(&path).await;
    sqlx::query("CREATE TABLE t (x INTEGER NOT NULL)")
        .execute(&mut c)
        .await
        .unwrap();
    c.close().await.unwrap();
    (dir, path)
}

/// A raw connection with the store's pragmas (WAL, `busy_timeout` 5000).
async fn raw(path: &str) -> SqliteConnection {
    SqliteConnectOptions::from_str(&format!("sqlite:{path}"))
        .unwrap()
        .pragma("journal_mode", "WAL")
        .pragma("busy_timeout", "5000")
        .create_if_missing(true)
        .connect()
        .await
        .unwrap()
}

async fn rows(path: &str) -> i64 {
    let mut c = raw(path).await;
    sqlx::query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&mut c)
        .await
        .unwrap()
}

/// The fault, deterministically: A opens a deferred `BEGIN` and reads (its
/// snapshot), B writes and commits, A writes. SQLite answers 517 at once,
/// `busy_timeout` notwithstanding, and the crate's conversion names it
/// `Store busy: sqlite code 517`.
#[tokio::test]
#[allow(clippy::disallowed_methods)] // the deferred BEGIN under test
async fn a_deferred_begin_that_read_first_answers_517_after_another_commit() {
    let (_dir, path) = wal_file().await;
    let mut a = raw(&path).await;
    let mut b = raw(&path).await;

    let mut ta = a.begin().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&mut *ta)
        .await
        .unwrap();

    sqlx::query("INSERT INTO t (x) VALUES (2)")
        .execute(&mut b)
        .await
        .unwrap();

    let started = Instant::now();
    let err = sqlx::query("INSERT INTO t (x) VALUES (1)")
        .execute(&mut *ta)
        .await
        .expect_err("a stale snapshot cannot upgrade to a write");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "517 is immediate: busy_timeout does not apply"
    );
    let code = err
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.to_string());
    assert_eq!(code.as_deref(), Some("517"), "{err}");

    let err = Error::from(err);
    assert!(matches!(err, Error::StoreBusy { code: 517 }), "{err:?}");
    assert!(
        err.to_string().starts_with("Store busy: sqlite code 517"),
        "{err}"
    );
}

/// The same shape through two stores on one file (two processes): A's
/// `write_tx` reads, B's `write_tx` WAITS for A's lock (it does not fail),
/// A writes and commits, then B writes and commits. No 517, no 5.
#[tokio::test]
async fn the_same_shape_through_write_tx_waits_and_both_commit_across_stores() {
    let (_dir, path) = wal_file().await;
    let a = StorageSqlx::open(&path).await.unwrap();
    let b = StorageSqlx::open(&path).await.unwrap();

    let mut ta = a.write_tx().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&mut *ta)
        .await
        .unwrap();

    let contender = tokio::spawn(async move {
        let mut tb = b.write_tx().await?;
        let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&mut *tb)
            .await?;
        sqlx::query("INSERT INTO t (x) VALUES (2)")
            .execute(&mut *tb)
            .await?;
        tb.commit().await?;
        Ok::<_, Error>(())
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !contender.is_finished(),
        "the contender waits on A's write lock"
    );

    sqlx::query("INSERT INTO t (x) VALUES (1)")
        .execute(&mut *ta)
        .await
        .expect("A's write after its read: no 517");
    ta.commit().await.unwrap();

    contender
        .await
        .unwrap()
        .expect("the contender commits after A");
    assert_eq!(rows(&path).await, 2);
}

/// The same shape within one store (the daemon: a served call and the
/// monitor): the contender waits for the one writer connection.
#[tokio::test]
async fn the_same_shape_through_write_tx_waits_and_both_commit_in_one_store() {
    let (_dir, path) = wal_file().await;
    let store = std::sync::Arc::new(StorageSqlx::open(&path).await.unwrap());

    let mut ta = store.write_tx().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&mut *ta)
        .await
        .unwrap();

    let other = std::sync::Arc::clone(&store);
    let contender = tokio::spawn(async move {
        let mut tb = other.write_tx().await?;
        sqlx::query("INSERT INTO t (x) VALUES (2)")
            .execute(&mut *tb)
            .await?;
        tb.commit().await?;
        Ok::<_, Error>(())
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !contender.is_finished(),
        "the contender waits for the writer"
    );

    sqlx::query("INSERT INTO t (x) VALUES (1)")
        .execute(&mut *ta)
        .await
        .expect("no 517");
    ta.commit().await.unwrap();

    contender.await.unwrap().expect("the contender commits");
    assert_eq!(rows(&path).await, 2);
}

/// `pool()` is the reader pool: every connection is `query_only`, so a write
/// sent there fails loudly; the writer writes.
#[tokio::test]
async fn a_write_on_pool_fails_under_query_only() {
    let (_dir, path) = wal_file().await;
    let store = StorageSqlx::open(&path).await.unwrap();

    let err = sqlx::query("INSERT INTO t (x) VALUES (1)")
        .execute(store.pool())
        .await
        .expect_err("the reader refuses a write");
    assert!(err.to_string().contains("readonly"), "{err}");

    let query_only: i64 = sqlx::query_scalar("PRAGMA query_only")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(query_only, 1);
    let query_only: i64 = sqlx::query_scalar("PRAGMA query_only")
        .fetch_one(store.writer())
        .await
        .unwrap();
    assert_eq!(query_only, 0);

    sqlx::query("INSERT INTO t (x) VALUES (1)")
        .execute(store.writer())
        .await
        .expect("the writer writes");
    assert_eq!(rows(&path).await, 1);
}

/// Both pools keep the store's pragmas.
#[tokio::test]
async fn both_pools_keep_the_pragmas() {
    let (_dir, path) = wal_file().await;
    let store = StorageSqlx::open(&path).await.unwrap();
    for pool in [store.pool(), store.writer()] {
        let (journal,): (String,) = sqlx::query_as("PRAGMA journal_mode")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(journal, "wal");
        let (fk,): (i64,) = sqlx::query_as("PRAGMA foreign_keys")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(fk, 1);
        let (busy,): (i64,) = sqlx::query_as("PRAGMA busy_timeout")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(busy, 5000);
        let (sync,): (i64,) = sqlx::query_as("PRAGMA synchronous")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(sync, 1, "NORMAL");
    }
    assert_eq!(store.writer().options().get_max_connections(), 1);
    assert!(store.pool().options().get_max_connections() > 1);
}

/// The Display prefix consumers classify on.
#[test]
fn the_store_busy_display_prefix() {
    for code in [5, 517] {
        assert_eq!(
            Error::StoreBusy { code }.to_string(),
            format!("Store busy: sqlite code {code}")
        );
    }
}

/// Only busy converts to `StoreBusy`: a UNIQUE violation (extended code
/// 2067, a deterministic refusal) converts to the plain SQLx arm.
#[tokio::test]
async fn a_unique_violation_is_not_store_busy() {
    let (_dir, path) = wal_file().await;
    let mut c = raw(&path).await;
    sqlx::query("CREATE TABLE u (x INTEGER NOT NULL UNIQUE)")
        .execute(&mut c)
        .await
        .unwrap();
    sqlx::query("INSERT INTO u (x) VALUES (1)")
        .execute(&mut c)
        .await
        .unwrap();
    let err = sqlx::query("INSERT INTO u (x) VALUES (1)")
        .execute(&mut c)
        .await
        .expect_err("a duplicate");
    let code = err
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.to_string());
    assert_eq!(code.as_deref(), Some("2067"), "{err}");

    let err = Error::from(err);
    assert!(matches!(err, Error::SqlxError(_)), "{err:?}");
    assert!(!err.to_string().starts_with("Store busy"), "{err}");
}

/// The writer not acquired within the store's ceiling (the writer pool's
/// `acquire_timeout`, `STORE_BUSY_CEILING_MS`) answers `StoreBusy` and
/// writes nothing.
#[tokio::test]
async fn the_writer_not_acquired_within_the_ceiling_is_store_busy() {
    let (_dir, path) = wal_file().await;
    let store = StorageSqlx::open(&path).await.unwrap();
    assert_eq!(
        store.writer().options().get_acquire_timeout(),
        Duration::from_millis(bsv_wallet_toolbox_rs::storage::sqlx::STORE_BUSY_CEILING_MS)
    );

    let mut held = store.write_tx().await.unwrap();
    sqlx::query("INSERT INTO t (x) VALUES (1)")
        .execute(&mut *held)
        .await
        .unwrap();

    let started = Instant::now();
    let err = match store.write_tx().await {
        Ok(_) => panic!("the writer is held"),
        Err(e) => e,
    };
    assert!(
        started.elapsed() >= Duration::from_millis(4500),
        "it waited the ceiling"
    );
    assert!(matches!(err, Error::StoreBusy { code: 5 }), "{err:?}");
    assert!(err.to_string().starts_with("Store busy: sqlite code 5"));

    held.rollback().await.unwrap();
    assert_eq!(rows(&path).await, 0);
}
