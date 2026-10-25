//! `StorageSqlx::retire_undeliverable_txid` — THE RELEASE RULE addressed by
//! txid, the path a serving wallet uses when its asynchronous post-broadcast
//! verification comes back definitively absent.
//!
//! Real SQLite, mock chain oracle. Every cell checks the three invariants:
//! alive-check first, no input released at retire (no explorer's word
//! releases one: bsv-wallet-toolbox-rs #42 point 5), own outputs never fund
//! again.

#![cfg(feature = "sqlite")]

use async_trait::async_trait;
use bsv_rs::transaction::ChainTracker;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{
    BlockHeader, FiatCurrency, GetBeefResult, GetMerklePathResult, GetRawTxResult,
    GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefResult, TxStatusDetail,
    UtxoVerdict, WalletServices,
};
use bsv_wallet_toolbox_rs::{
    Error, GetStatusForTxidsResult, Result, RetireOutcome, StorageSqlx, WalletStorageWriter,
};

const PARENT_TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TXID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Seeded {
    storage: StorageSqlx,
    /// The parent's output our tx spends (locked by it: spent_by = tx, spendable 0).
    input_output_id: i64,
    /// Our tx's own change output (spendable 1 before retirement).
    own_output_id: i64,
}

/// An outgoing tx at `sending` with req `unsent` and no accepting word (no
/// instance accepted: still `built`, the only transaction retire honours,
/// #165), spending one parent output and producing one change output.
async fn seed() -> Seeded {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let storage_key = "02".to_string() + &"ab".repeat(32);
    storage
        .migrate("retire-tests", &storage_key)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let identity = "02".to_string() + &"cd".repeat(32);
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("basket");
    let now = chrono::Utc::now();
    let lock = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let parent_id = sqlx::query(
        r#"
        INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
        VALUES (?, 'completed', 'parent', 0, 50000, 1, 0, 'parent', ?, X'01000000', ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind(PARENT_TXID)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .expect("parent tx")
    .last_insert_rowid();

    let tx_id = sqlx::query(
        r#"
        INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
        VALUES (?, 'sending', 'ours', 1, -2000, 1, 0, 'ours', ?, X'01000000', ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind(TXID)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .expect("our tx")
    .last_insert_rowid();

    let input_output_id = sqlx::query(
        r#"
        INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                             txid, type, spendable, change, spent_by, provided_by, purpose,
                             output_description, created_at, updated_at)
        VALUES (?, ?, ?, 0, 50000, ?, ?, 'P2PKH', 0, 1, ?, 'storage', 'change', 'input', ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind(parent_id)
    .bind(basket.basket_id)
    .bind(&lock)
    .bind(PARENT_TXID)
    .bind(tx_id)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .expect("input output")
    .last_insert_rowid();

    let own_output_id = sqlx::query(
        r#"
        INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                             txid, type, spendable, change, provided_by, purpose,
                             output_description, created_at, updated_at)
        VALUES (?, ?, ?, 0, 48000, ?, ?, 'P2PKH', 1, 1, 'storage', 'change', 'our change', ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind(tx_id)
    .bind(basket.basket_id)
    .bind(&lock)
    .bind(TXID)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .expect("own output")
    .last_insert_rowid();

    sqlx::query(
        r#"
        INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
        VALUES (?, 'unsent', 0, '{}', 0, '{}', X'01000000', ?, ?)
        "#,
    )
    .bind(TXID)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .expect("req");

    Seeded {
        storage,
        input_output_id,
        own_output_id,
    }
}

async fn tx_status(storage: &StorageSqlx) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(TXID)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn req_status_attempts(storage: &StorageSqlx) -> (String, i64) {
    sqlx::query_as("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
        .bind(TXID)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn output_state(storage: &StorageSqlx, output_id: i64) -> (i64, Option<i64>) {
    sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

fn alive_oracle() -> MockWalletServices {
    MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: TXID.to_string(),
                status: "known".to_string(),
                depth: None,
                ..Default::default()
            }],
        }))
        .build()
}

#[tokio::test]
async fn absent_tx_is_failed_and_an_explorers_unspent_input_stays_locked() {
    let s = seed().await;
    // Default mock: status lookup finds nothing (not alive), is_utxo → true.
    // The explorer's unspent set is not asked: the input stays locked and
    // is scheduled for the locked-input re-check.
    let services = MockWalletServices::new();

    let outcome = s
        .storage
        .retire_undeliverable_txid(&services, TXID, "invalid")
        .await
        .expect("retire");
    assert_eq!(
        outcome,
        Some(RetireOutcome::Retired {
            restored: 0,
            kept: 1
        })
    );

    assert_eq!(tx_status(&s.storage).await, "failed");
    assert_eq!(
        req_status_attempts(&s.storage).await,
        ("invalid".to_string(), 1)
    );
    let (spendable, spent_by) = output_state(&s.storage, s.input_output_id).await;
    assert_eq!(spendable, 0, "the input stays locked");
    assert!(
        spent_by.is_some(),
        "the input stays locked to the failed tx"
    );
    assert_eq!(
        output_state(&s.storage, s.own_output_id).await,
        (0, None),
        "the failed tx's own change never funds anything again"
    );
    assert_eq!(services.call_count("is_utxo"), 0, "no explorer asked");
}

#[tokio::test]
async fn an_input_the_chain_will_not_vouch_for_stays_locked() {
    let s = seed().await;
    let services = MockWalletServices::builder()
        .is_utxo_response(MockResponse::Success(false))
        .build();

    let outcome = s
        .storage
        .retire_undeliverable_txid(&services, TXID, "invalid")
        .await
        .expect("retire");
    assert_eq!(
        outcome,
        Some(RetireOutcome::Retired {
            restored: 0,
            kept: 1
        })
    );
    assert_eq!(tx_status(&s.storage).await, "failed");
    let (spendable, spent_by) = output_state(&s.storage, s.input_output_id).await;
    assert_eq!(spendable, 0, "an unknown never releases money");
    assert!(spent_by.is_some(), "still locked by the failed tx");
    assert_eq!(output_state(&s.storage, s.own_output_id).await, (0, None));
}

#[tokio::test]
async fn a_tx_the_status_service_knows_is_promoted_not_retired() {
    let s = seed().await;
    let services = alive_oracle();

    let outcome = s
        .storage
        .retire_undeliverable_txid(&services, TXID, "invalid")
        .await
        .expect("retire");
    assert_eq!(outcome, Some(RetireOutcome::Alive));
    assert_eq!(tx_status(&s.storage).await, "unproven");
    assert_eq!(
        req_status_attempts(&s.storage).await,
        ("unmined".to_string(), 0)
    );
    let (spendable, spent_by) = output_state(&s.storage, s.input_output_id).await;
    assert_eq!(spendable, 0);
    assert!(spent_by.is_some(), "nothing released for a live tx");
    assert_eq!(output_state(&s.storage, s.own_output_id).await.0, 1);
    assert_eq!(
        services.call_count("is_utxo"),
        0,
        "no release was attempted"
    );
}

#[tokio::test]
async fn an_unknown_txid_touches_nothing() {
    let s = seed().await;
    let services = MockWalletServices::new();
    let outcome = s
        .storage
        .retire_undeliverable_txid(&services, &"ee".repeat(32), "invalid")
        .await
        .expect("retire");
    assert_eq!(outcome, None);
    assert_eq!(tx_status(&s.storage).await, "sending");
    assert_eq!(output_state(&s.storage, s.input_output_id).await.0, 0);
}

#[tokio::test]
async fn the_req_status_names_the_reason() {
    let s = seed().await;
    let services = MockWalletServices::new();
    s.storage
        .retire_undeliverable_txid(&services, TXID, "doubleSpend")
        .await
        .expect("retire");
    assert_eq!(
        req_status_attempts(&s.storage).await,
        ("doubleSpend".to_string(), 1)
    );
}

#[tokio::test]
async fn an_accepted_tx_is_refused_with_nothing_written_and_no_service_asked() {
    let s = seed().await;
    // A broadcaster accepted it: tx `unproven`, req `unmined` (#165).
    sqlx::query("UPDATE transactions SET status = 'unproven' WHERE txid = ?")
        .bind(TXID)
        .execute(s.storage.writer())
        .await
        .unwrap();
    sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined' WHERE txid = ?")
        .bind(TXID)
        .execute(s.storage.writer())
        .await
        .unwrap();
    let services = alive_oracle();

    let err = s
        .storage
        .retire_undeliverable_txid(&services, TXID, "invalid")
        .await
        .expect_err("an accepted transaction is never retired");
    assert!(
        matches!(err, Error::InvalidTransactionStatus(ref msg) if msg.contains("acceptance")),
        "{err:?}"
    );
    assert_eq!(tx_status(&s.storage).await, "unproven");
    assert_eq!(
        req_status_attempts(&s.storage).await,
        ("unmined".to_string(), 0)
    );
    assert_eq!(
        output_state(&s.storage, s.input_output_id).await,
        (0, Some(own_tx_id(&s.storage).await))
    );
    assert_eq!(output_state(&s.storage, s.own_output_id).await, (1, None));
    assert_eq!(services.total_calls(), 0, "no service asked");
}

async fn own_tx_id(storage: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE txid = ?")
        .bind(TXID)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

/// The race window of `retire_undeliverable_txid`: the alive check asks the
/// status service between the reader's guard and the retire's writes. This
/// service records an instance's acceptance in that window, as a broadcast
/// on another path would, then answers "not found" (the default mock).
struct AcceptsWhileAsked {
    inner: MockWalletServices,
    storage: StorageSqlx,
}

#[async_trait]
impl WalletServices for AcceptsWhileAsked {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        self.inner.get_chain_tracker().await
    }
    async fn get_height(&self) -> Result<u32> {
        self.inner.get_height().await
    }
    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        self.inner.get_header_for_height(height).await
    }
    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        self.inner.hash_to_header(hash).await
    }
    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult> {
        self.inner.get_raw_tx(txid, use_next).await
    }
    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult> {
        self.inner.get_merkle_path(txid, use_next).await
    }
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        self.inner.post_beef(beef, txids).await
    }
    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        self.inner
            .get_utxo_status(output, output_format, outpoint, use_next)
            .await
    }
    async fn get_status_for_txids(
        &self,
        txids: &[String],
        use_next: bool,
    ) -> Result<GetStatusForTxidsResult> {
        self.storage
            .record_broadcast_status(TXID, "instance-a", "accepted")
            .await
            .expect("the acceptance commits while the status service is asked");
        self.inner.get_status_for_txids(txids, use_next).await
    }
    async fn get_bsv_exchange_rate(&self) -> Result<f64> {
        self.inner.get_bsv_exchange_rate().await
    }
    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64> {
        self.inner.get_fiat_exchange_rate(currency, base).await
    }
    fn hash_output_script(&self, script: &[u8]) -> String {
        self.inner.hash_output_script(script)
    }
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> UtxoVerdict {
        self.inner.is_utxo(txid, vout, locking_script).await
    }
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool> {
        self.inner.n_lock_time_is_final(n_lock_time).await
    }
    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool> {
        self.inner.n_lock_time_is_final_for_tx(input).await
    }
    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult> {
        self.inner.get_beef(txid, known_txids).await
    }
}

#[tokio::test]
async fn an_acceptance_committed_during_the_alive_check_refuses_the_retire() {
    let s = seed().await;
    let services = AcceptsWhileAsked {
        inner: MockWalletServices::new(),
        storage: s.storage.clone(),
    };

    let err = s
        .storage
        .retire_undeliverable_txid(&services, TXID, "invalid")
        .await
        .expect_err("the acceptance is read under the retire's write lock");
    assert!(
        matches!(err, Error::InvalidTransactionStatus(ref msg) if msg.contains("acceptance")),
        "{err:?}"
    );
    assert_eq!(
        services.inner.call_count("get_status_for_txids"),
        1,
        "the guard passed before the ask: the window was open"
    );
    assert_eq!(tx_status(&s.storage).await, "sending");
    assert_eq!(
        req_status_attempts(&s.storage).await,
        ("unsent".to_string(), 0)
    );
    assert_eq!(
        output_state(&s.storage, s.input_output_id).await,
        (0, Some(own_tx_id(&s.storage).await))
    );
    assert_eq!(output_state(&s.storage, s.own_output_id).await, (1, None));
}
