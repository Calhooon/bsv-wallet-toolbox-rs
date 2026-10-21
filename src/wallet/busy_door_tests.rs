//! A busy store at the `WalletInterface` boundary (bsv-wallet-cli #44): a
//! `StoreBusy` from the store surfaces as bsv-rs's `WalletBusy` at every door
//! that writes, and every other store error keeps its exact `WalletError`
//! text. The writer is held through `write_tx`, so the door waits the store's
//! ceiling (5 s) and answers busy having written nothing.

use super::*;
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::StorageSqlx;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    AbortActionArgs, BasketInsertion, CreateActionArgs, CreateActionOutput, InternalizeActionArgs,
    InternalizeOutput, SignActionArgs,
};

const ORIGINATOR: &str = "busy.example.com";

/// An unrelated P2PKH locking script.
fn pay_lock() -> Vec<u8> {
    hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// A wallet over a file store (WAL; a held writer does not block readers).
async fn wallet() -> (tempfile::TempDir, Wallet<StorageSqlx, MockWalletServices>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.db").to_string_lossy().to_string();
    let storage = StorageSqlx::open(&path).await.unwrap();
    let root = PrivateKey::random();
    storage
        .migrate("busy-door-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let wallet = Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap();
    (dir, wallet)
}

/// An outgoing `unsigned` action under `reference`, written before the
/// writer is held.
async fn unsigned_action(wallet: &Wallet<StorageSqlx, MockWalletServices>, reference: &str) {
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description) \
         VALUES (?, 'unsigned', ?, 1, 0, 'a busy door test')",
    )
    .bind(wallet.user_id)
    .bind(reference)
    .execute(wallet.storage().writer())
    .await
    .unwrap();
}

/// One spendable 100,000-sat change output on a proven parent, so
/// createAction's pre-check finds the funds and the door reaches the writer.
async fn fund(wallet: &Wallet<StorageSqlx, MockWalletServices>) {
    fund_with(wallet, &pay_lock()).await;
}

/// `fund`, the coin locked by `lock`; returns the coin's output id. A
/// non-P2PKH `lock` (`OP_TRUE`) is signed without the derived key check, so
/// createAction reaches processAction.
async fn fund_with(wallet: &Wallet<StorageSqlx, MockWalletServices>, lock: &[u8]) -> i64 {
    let pay_lock = || lock.to_vec();
    let storage = wallet.storage();
    let basket = storage
        .find_or_create_default_basket(wallet.user_id)
        .await
        .unwrap();
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("00".repeat(32), 0xffff_ffff);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(100_000),
        locking_script: LockingScript::from_binary(&pay_lock()).unwrap(),
        change: false,
    });
    let txid = parent.id();
    let raw = parent.to_binary();
    let bump = MerklePath::from_coinbase_txid(&txid, 100_000);
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
         VALUES (?, 100000, 0, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&txid)
    .bind("aa".repeat(32))
    .bind(&txid)
    .bind(bump.to_binary())
    .bind(&raw)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
    let transaction_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?, 'completed', 'funding-ref', 0, 100000, 1, 0, 'funding', ?, ?, ?, ?)",
    )
    .bind(wallet.user_id)
    .bind(&txid)
    .bind(&raw)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, \
                              txid, type, spendable, change, derivation_prefix, derivation_suffix, \
                              provided_by, purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, 100000, ?, ?, 'P2PKH', 1, 1, 'cHJlZml4', 'c3VmZml4', 'storage', 'change', 'funding', ?, ?)",
    )
    .bind(wallet.user_id)
    .bind(transaction_id)
    .bind(basket.basket_id)
    .bind(pay_lock())
    .bind(&txid)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

fn assert_wallet_busy<T: std::fmt::Debug>(result: bsv_rs::Result<T>) {
    assert_wallet_busy_with(
        result,
        "Store busy: the writer was not acquired within the store's ceiling",
    );
}

fn assert_wallet_busy_with<T: std::fmt::Debug>(result: bsv_rs::Result<T>, want: &str) {
    match result {
        Err(bsv_rs::Error::WalletBusy {
            detail,
            retry_after_ms,
        }) => {
            assert_eq!(detail, want);
            assert_eq!(retry_after_ms, None, "the store sets no retry policy");
        }
        other => panic!("expected WalletBusy, got {other:?}"),
    }
}

#[tokio::test]
async fn create_action_on_a_busy_store_is_wallet_busy() {
    let (_dir, wallet) = wallet().await;
    fund(&wallet).await;
    let held = wallet.storage().write_tx().await.unwrap();
    let result = wallet
        .create_action(
            CreateActionArgs {
                description: "a busy door test".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: pay_lock(),
                    satoshis: 1_000,
                    output_description: "a payment".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: None,
                options: None,
            },
            ORIGINATOR,
        )
        .await;
    assert_wallet_busy(result);
    held.rollback().await.unwrap();

    let (actions,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE is_outgoing = 1")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(actions, 0, "nothing was written");
}

#[tokio::test]
async fn abort_action_on_a_busy_store_is_wallet_busy() {
    let (_dir, wallet) = wallet().await;
    unsigned_action(&wallet, "abort-busy").await;
    let held = wallet.storage().write_tx().await.unwrap();
    let result = wallet
        .abort_action(
            AbortActionArgs {
                reference: "abort-busy".to_string(),
            },
            ORIGINATOR,
        )
        .await;
    assert_wallet_busy(result);
    held.rollback().await.unwrap();

    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM transactions WHERE reference = 'abort-busy'")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(status, "unsigned", "nothing was written");
}

#[tokio::test]
async fn internalize_action_on_a_busy_store_is_wallet_busy() {
    let (_dir, wallet) = wallet().await;
    let mut parent = Transaction::new();
    let mut parent_input = TransactionInput::new("11".repeat(32), 0);
    parent_input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(parent_input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(20_000),
        locking_script: LockingScript::from_binary(&pay_lock()).unwrap(),
        change: false,
    });
    let parent_txid = parent.id();
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(parent_txid.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_binary(&pay_lock()).unwrap(),
        change: false,
    });
    let txid = tx.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_transaction(tx);
    let atomic = beef.to_binary_atomic(&txid).unwrap();

    let held = wallet.storage().write_tx().await.unwrap();
    let result = wallet
        .internalize_action(
            InternalizeActionArgs {
                tx: atomic,
                outputs: vec![InternalizeOutput {
                    output_index: 0,
                    protocol: "basket insertion".to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(BasketInsertion {
                        basket: "busy".to_string(),
                        custom_instructions: None,
                        tags: None,
                    }),
                }],
                description: "a busy door test".to_string(),
                labels: None,
                seek_permission: None,
            },
            ORIGINATOR,
        )
        .await;
    assert_wallet_busy(result);
    held.rollback().await.unwrap();
}

/// signAction on a busy store: busy, the action not aborted, the pending
/// transaction still cached, so the same call may be repeated unchanged.
#[tokio::test]
async fn sign_action_on_a_busy_store_is_wallet_busy_and_keeps_the_action() {
    let (_dir, wallet) = wallet().await;
    unsigned_action(&wallet, "sign-busy").await;
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new("22".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(1_000),
        locking_script: LockingScript::from_binary(&pay_lock()).unwrap(),
        change: false,
    });
    wallet.pending_transactions.write().await.insert(
        "sign-busy".to_string(),
        PendingTransaction {
            reference: "sign-busy".to_string(),
            raw_tx: tx.to_binary(),
            inputs: vec![],
            input_beef: None,
            is_no_send: true,
            is_delayed: false,
            send_with: vec![],
            created_at: Utc::now(),
        },
    );

    let held = wallet.storage().write_tx().await.unwrap();
    let result = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: "sign-busy".to_string(),
                options: None,
            },
            ORIGINATOR,
        )
        .await;
    assert_wallet_busy(result);
    held.rollback().await.unwrap();

    assert!(wallet
        .pending_transactions
        .read()
        .await
        .contains_key("sign-busy"));
    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM transactions WHERE reference = 'sign-busy'")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(status, "unsigned", "not aborted");
}

/// The P2PKH script of the wallet's own BRC-29 key for `fund`'s
/// derivation (prefix `cHJlZml4`, suffix `c3VmZml4`, counterparty self),
/// so the signer signs the coin.
fn own_lock(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> Vec<u8> {
    use bsv_rs::wallet::{Counterparty, KeyDeriverApi, Protocol, SecurityLevel};
    let key = wallet
        .proto_wallet
        .key_deriver()
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            "cHJlZml4 c3VmZml4",
            &Counterparty::Self_,
        )
        .unwrap();
    let hash = bsv_rs::primitives::hash160(&key.public_key().to_compressed());
    [&[0x76, 0xa9, 0x14][..], &hash[..], &[0x88, 0xac][..]].concat()
}

/// A change coin locked to the action under `reference`, as createAction
/// leaves it; returns the coin's output id.
async fn lock_a_coin_to(wallet: &Wallet<StorageSqlx, MockWalletServices>, reference: &str) -> i64 {
    let coin = fund_with(wallet, &pay_lock()).await;
    sqlx::query(
        "UPDATE outputs SET spendable = 0, spent_by = \
         (SELECT transaction_id FROM transactions WHERE reference = ?) WHERE output_id = ?",
    )
    .bind(reference)
    .bind(coin)
    .execute(wallet.storage().writer())
    .await
    .unwrap();
    coin
}

/// The action's status and whether `coin` is still locked to it.
async fn action_and_coin(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    reference: &str,
    coin: i64,
) -> (String, bool) {
    let (status, transaction_id): (String, i64) =
        sqlx::query_as("SELECT status, transaction_id FROM transactions WHERE reference = ?")
            .bind(reference)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    let (spendable, spent_by): (bool, Option<i64>) =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(coin)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    (status, !spendable && spent_by == Some(transaction_id))
}

/// A pending transaction under `reference`: one input, one output.
async fn pending(wallet: &Wallet<StorageSqlx, MockWalletServices>, reference: &str, tag: &str) {
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(tag.repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(1_000),
        locking_script: LockingScript::from_binary(&pay_lock()).unwrap(),
        change: false,
    });
    wallet.pending_transactions.write().await.insert(
        reference.to_string(),
        PendingTransaction {
            reference: reference.to_string(),
            raw_tx: tx.to_binary(),
            inputs: vec![],
            input_beef: None,
            is_no_send: true,
            is_delayed: false,
            send_with: vec![],
            created_at: Utc::now(),
        },
    );
}

/// Review 2 F5 of #44: signAction's processAction answers busy. The action
/// is not abandoned, its coin stays locked to it, and the pending
/// transaction stays cached.
#[tokio::test]
async fn sign_action_busy_in_process_action_keeps_the_action_and_its_coins() {
    let (_dir, wallet) = wallet().await;
    unsigned_action(&wallet, "sign-busy-coin").await;
    let coin = lock_a_coin_to(&wallet, "sign-busy-coin").await;
    pending(&wallet, "sign-busy-coin", "23").await;

    wallet.storage().busy_next_process_action();
    let result = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: "sign-busy-coin".to_string(),
                options: None,
            },
            ORIGINATOR,
        )
        .await;
    assert_wallet_busy_with(result, "Store busy: sqlite code 5");
    assert!(wallet
        .pending_transactions
        .read()
        .await
        .contains_key("sign-busy-coin"));
    assert_eq!(
        action_and_coin(&wallet, "sign-busy-coin", coin).await,
        ("unsigned".to_string(), true),
        "not abandoned, the coin still locked to it"
    );
}

/// Any other processAction error still aborts, as it did: the action is
/// failed and its coin released. (The action has no inputBEEF:
/// processAction refuses it, a validation error, not busy.)
#[tokio::test]
async fn sign_action_on_another_process_action_error_still_aborts() {
    let (_dir, wallet) = wallet().await;
    unsigned_action(&wallet, "sign-other").await;
    let coin = lock_a_coin_to(&wallet, "sign-other").await;
    pending(&wallet, "sign-other", "24").await;

    let err = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: "sign-other".to_string(),
                options: None,
            },
            ORIGINATOR,
        )
        .await
        .unwrap_err();
    match &err {
        bsv_rs::Error::WalletError(text) => assert!(
            text.starts_with("Process action failed: ")
                && text.ends_with("Transaction aborted and UTXOs released."),
            "{text}"
        ),
        other => panic!("expected the abort's WalletError, got {other:?}"),
    }
    assert_eq!(
        action_and_coin(&wallet, "sign-other", coin).await,
        ("failed".to_string(), false),
        "aborted, the coin released"
    );
    let (spendable,): (bool,) = sqlx::query_as("SELECT spendable FROM outputs WHERE output_id = ?")
        .bind(coin)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    assert!(spendable, "the coin is spendable again");
}

/// Every row count `createAction` could touch, the default basket's coin
/// locks and SQLite's sequence counters: equal before and after a refusal
/// means the action wrote nothing.
async fn store_snapshot(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> Vec<String> {
    let mut snapshot = Vec::new();
    for table in [
        "transactions",
        "outputs",
        "tx_labels",
        "tx_labels_map",
        "output_tags",
        "output_tags_map",
        "output_baskets",
        "proven_tx_reqs",
        "commissions",
    ] {
        let (n,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
        snapshot.push(format!("{table}={n}"));
    }
    let locks: Vec<(i64, bool, Option<i64>)> =
        sqlx::query_as("SELECT output_id, spendable, spent_by FROM outputs ORDER BY output_id")
            .fetch_all(wallet.storage().pool())
            .await
            .unwrap();
    snapshot.extend(locks.iter().map(|l| format!("{l:?}")));
    let sequences: Vec<(String, i64)> =
        sqlx::query_as("SELECT name, seq FROM sqlite_sequence ORDER BY name")
            .fetch_all(wallet.storage().pool())
            .await
            .unwrap();
    snapshot.extend(sequences.iter().map(|s| format!("{s:?}")));
    snapshot
}

/// A payment with a label, a new basket and a tag, so every row kind of
/// the action is written when the door commits.
fn labelled_payment(no_send: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "a busy door test".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: pay_lock(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: Some("door-basket".to_string()),
            custom_instructions: None,
            tags: Some(vec!["door-tag".to_string()]),
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["door-label".to_string()]),
        options: Some(bsv_rs::wallet::CreateActionOptions {
            no_send: Some(no_send),
            ..Default::default()
        }),
    }
}

/// The captain's finding on 516195e (review 2 F5 of #44): createAction with
/// signAndProcess is ONE write transaction, the create step, the signing
/// and processAction's writes committed once. A busy at the process step's
/// write rolls all of it back: WalletBusy, and no row of the action exists
/// (transactions, outputs, coin locks, labels, tags, baskets,
/// proven_tx_reqs, the sequence counters all unchanged), nothing cached.
/// The same call repeated unchanged then succeeds.
#[tokio::test]
async fn create_action_busy_in_the_process_step_writes_nothing() {
    let (_dir, wallet) = wallet().await;
    let coin = fund_with(&wallet, &own_lock(&wallet)).await;
    let before = store_snapshot(&wallet).await;

    wallet.storage().busy_next_process_action();
    let result = wallet
        .create_action(labelled_payment(true), ORIGINATOR)
        .await;
    assert_wallet_busy_with(result, "Store busy: sqlite code 5");

    assert_eq!(
        store_snapshot(&wallet).await,
        before,
        "the action wrote nothing"
    );
    let (spendable, spent_by): (bool, Option<i64>) =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(coin)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert!(spendable && spent_by.is_none(), "the coin is not locked");
    assert!(
        wallet.pending_transactions.read().await.is_empty(),
        "nothing cached: there is no action to sign"
    );

    let repeated = wallet
        .create_action(labelled_payment(true), ORIGINATOR)
        .await
        .expect("the same call, repeated unchanged, succeeds");
    assert!(repeated.txid.is_some());
    let (status,): (String,) = sqlx::query_as(
        "SELECT status FROM transactions WHERE is_outgoing = 1 AND description = 'a busy door test'",
    )
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap();
    assert_eq!(status, "nosend");
}

/// A store error that is not busy keeps the exact text the erasure sites
/// gave: `WalletError` with the toolbox Display.
#[tokio::test]
async fn a_store_error_that_is_not_busy_keeps_its_wallet_error_text() {
    let (_dir, wallet) = wallet().await;
    let err = wallet
        .abort_action(
            AbortActionArgs {
                reference: "no-such-action".to_string(),
            },
            ORIGINATOR,
        )
        .await
        .unwrap_err();
    match err {
        bsv_rs::Error::WalletError(text) => assert_eq!(
            text,
            "Entity not found: transaction with id reference or txid 'no-such-action'"
        ),
        other => panic!("expected WalletError, got {other:?}"),
    }
}

/// Every event logged on this thread while the guard lives, its fields as
/// `name=value` text.
struct LogCapture(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl tracing::Subscriber for LogCapture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={:?} ", field.name(), value));
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// bsv-wallet-cli #44: after the broadcast a store fault is never the
/// call's error. The status write after the post answers busy: createAction
/// still answers its success with the txid, the fault is logged at warn with
/// the marker `post_broadcast_status_write_failed` and the txid, the row
/// stays `sending` with its request `unsent`, and the monitor's next
/// send-waiting pass (`send_waiting_transactions`) re-posts it and writes
/// `unproven` / `unmined`.
#[tokio::test]
async fn a_busy_status_write_after_the_broadcast_is_logged_and_healed_by_send_waiting() {
    let (_dir, wallet) = wallet().await;
    fund_with(&wallet, &own_lock(&wallet)).await;
    wallet
        .storage()
        .set_services(std::sync::Arc::new(MockWalletServices::new()));
    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(LogCapture(logs.clone()));

    wallet.storage().busy_next_status_write();
    let result = wallet
        .create_action(labelled_payment(false), ORIGINATOR)
        .await
        .expect("the transaction is on the network: the call succeeds");
    let txid = hex::encode(result.txid.expect("the answer carries the txid"));
    assert!(result.tx.is_some());

    let marked = logs
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains("post_broadcast_status_write_failed"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(marked.len(), 1, "{marked:?}");
    assert!(
        marked[0].contains(&txid) && marked[0].contains("Store busy: sqlite code 5"),
        "{}",
        marked[0]
    );

    let statuses = |wallet: &Wallet<StorageSqlx, MockWalletServices>| {
        let txid = txid.clone();
        let pool = wallet.storage().pool().clone();
        async move {
            sqlx::query_as::<_, (String, String)>(
                "SELECT t.status, r.status FROM transactions t \
                 JOIN proven_tx_reqs r ON r.txid = t.txid WHERE t.txid = ?",
            )
            .bind(&txid)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert_eq!(
        statuses(&wallet).await,
        ("sending".to_string(), "unsent".to_string()),
        "the status write did not land"
    );

    // The monitor's pass comes on its cadence, past the send-waiting
    // task's minimum age.
    let earlier = Utc::now() - chrono::Duration::minutes(10);
    sqlx::query("UPDATE proven_tx_reqs SET created_at = ?, updated_at = ? WHERE txid = ?")
        .bind(earlier)
        .bind(earlier)
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    crate::storage::MonitorStorage::send_waiting_transactions(
        wallet.storage(),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert_eq!(
        statuses(&wallet).await,
        ("unproven".to_string(), "unmined".to_string()),
        "the monitor's send-waiting pass healed the row"
    );
}

/// The same rule at signAction: the status write after its post answers
/// busy, and the call still answers its success with the txid, the marker
/// logged.
#[tokio::test]
async fn a_busy_status_write_after_a_sign_action_broadcast_is_its_success() {
    let (_dir, wallet) = wallet().await;
    fund_with(&wallet, &own_lock(&wallet)).await;
    let mut args = labelled_payment(false);
    args.options = Some(bsv_rs::wallet::CreateActionOptions {
        sign_and_process: Some(false),
        ..Default::default()
    });
    let created = wallet.create_action(args, ORIGINATOR).await.unwrap();
    let reference = String::from_utf8(created.signable_transaction.unwrap().reference).unwrap();

    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(LogCapture(logs.clone()));
    wallet.storage().busy_next_status_write();
    let signed = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference,
                options: None,
            },
            ORIGINATOR,
        )
        .await
        .expect("the transaction is on the network: the call succeeds");
    let txid = hex::encode(signed.txid.expect("the answer carries the txid"));
    assert!(logs
        .lock()
        .unwrap()
        .iter()
        .any(|line| line.contains("post_broadcast_status_write_failed") && line.contains(&txid)));
    let (status,): (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
        .bind(&txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    assert_eq!(status, "sending", "left for the send-waiting task");
}
