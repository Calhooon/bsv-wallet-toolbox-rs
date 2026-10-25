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
    fund_tagged(wallet, lock, 0).await
}

/// `fund_with`, the coin's parent spending outpoint `tag:0` (its own txid
/// per tag), so a wallet can be funded with many coins.
async fn fund_tagged(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    lock: &[u8],
    tag: u32,
) -> i64 {
    let pay_lock = || lock.to_vec();
    let storage = wallet.storage();
    let basket = storage
        .find_or_create_default_basket(wallet.user_id)
        .await
        .unwrap();
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new(format!("{tag:064x}"), 0xffff_ffff);
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
         VALUES (?, 'completed', ?, 0, 100000, 1, 0, 'funding', ?, ?, ?, ?)",
    )
    .bind(wallet.user_id)
    .bind(if tag == 0 {
        "funding-ref".to_string()
    } else {
        format!("funding-ref-{tag}")
    })
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
        locking_script: LockingScript::from_hex("7551").unwrap(),
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

/// The host's offer at the wallet (bsv-wallet-cli #44, bsv-low #828):
/// `create_action_offering` takes change only from the offered output ids.
/// An offer that does not hold the wallet's coin refuses insufficient funds,
/// its coin untouched; the offer that holds it signs and processes as
/// createAction does.
#[tokio::test]
async fn create_action_offering_spends_only_the_offer() {
    let (_dir, wallet) = wallet().await;
    let coin = fund_with(&wallet, &own_lock(&wallet)).await;
    let before = store_snapshot(&wallet).await;

    for offer in [&[][..], &[coin + 1000][..]] {
        match wallet
            .create_action_offering(labelled_payment(true), ORIGINATOR, offer)
            .await
        {
            Err(bsv_rs::Error::WalletError(text)) => {
                assert!(text.starts_with("Insufficient funds: "), "{text}")
            }
            other => panic!("expected the shortfall, got {other:?}"),
        }
        assert_eq!(store_snapshot(&wallet).await, before, "nothing written");
    }

    let result = wallet
        .create_action_offering(labelled_payment(true), ORIGINATOR, &[coin])
        .await
        .expect("the offered coin funds the action");
    assert!(result.txid.is_some());
    let (spent_by,): (Option<i64>,) =
        sqlx::query_as("SELECT spent_by FROM outputs WHERE output_id = ?")
            .bind(coin)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert!(spent_by.is_some(), "the offered coin is spent");
}

/// A broadcast head `A` (`unproven`, reference `reference`) funded by a
/// completed coin, and its descendant `D` (`unproven`) spending `A`'s
/// change, `D` with change of its own; both reqs `unmined`. Returns
/// `(a_txid, d_txid, coin_a, change_d)`.
async fn broadcast_chain(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    reference: &str,
) -> (String, String, i64, i64) {
    let writer = wallet.storage().writer();
    let now = Utc::now();
    let tx = |status: &'static str, reference: String, txid: String| {
        let user_id = wallet.user_id;
        async move {
            sqlx::query(
                "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, created_at, updated_at) \
                 VALUES (?, ?, ?, 1, 1000, 'a busy door test', ?, 1, 0, ?, ?)",
            )
            .bind(user_id)
            .bind(status)
            .bind(reference)
            .bind(txid)
            .bind(now)
            .bind(now)
            .execute(writer)
            .await
            .unwrap()
            .last_insert_rowid()
        }
    };
    let output = |transaction_id: i64, txid: String, spendable: bool, spent_by: Option<i64>| {
        let user_id = wallet.user_id;
        async move {
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, spendable, change, provided_by, purpose, type, txid, spent_by, created_at, updated_at) \
                 VALUES (?, ?, 0, 1000, ?, 1, 'storage', 'change', 'P2PKH', ?, ?, ?, ?)",
            )
            .bind(user_id)
            .bind(transaction_id)
            .bind(spendable)
            .bind(txid)
            .bind(spent_by)
            .bind(now)
            .bind(now)
            .execute(writer)
            .await
            .unwrap()
            .last_insert_rowid()
        }
    };
    let (f_txid, a_txid, d_txid) = ("f".repeat(64), "1".repeat(64), "2".repeat(64));
    let funding = tx("completed", format!("{reference}-funding"), f_txid.clone()).await;
    let a = tx("unproven", reference.to_string(), a_txid.clone()).await;
    let d = tx("unproven", format!("{reference}-desc"), d_txid.clone()).await;
    let coin_a = output(funding, f_txid, false, Some(a)).await;
    output(a, a_txid.clone(), false, Some(d)).await;
    let change_d = output(d, d_txid.clone(), true, None).await;
    for txid in [&a_txid, &d_txid] {
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) \
             VALUES (?, 'unmined', 0, '{}', 0, '{}', X'01000000', ?, ?)",
        )
        .bind(txid)
        .bind(now)
        .bind(now)
        .execute(writer)
        .await
        .unwrap();
    }
    (a_txid, d_txid, coin_a, change_d)
}

/// `(transaction status, req status)` of `txid`.
async fn tx_and_req(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    txid: &str,
) -> (String, String) {
    sqlx::query_as(
        "SELECT t.status, r.status FROM transactions t \
         JOIN proven_tx_reqs r ON r.txid = t.txid WHERE t.txid = ?",
    )
    .bind(txid)
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap()
}

/// Review 3 F1 of bsv-wallet-cli #44: abortAction on a broadcast transaction
/// commits the root's abort, then retires its descendants. A busy at that
/// retirement is after a committed write, so it is never the call's error
/// (a `WalletBusy` would have the host repeat a call whose repeat refuses,
/// the root being `failed`): the abort answers its success, the marker
/// `post_commit_retire_failed` is logged with the txid, the descendant stays
/// `unproven`, and the monitor's review-status pass retires it; a second pass
/// finds nothing.
#[tokio::test]
async fn a_busy_retirement_after_a_broadcast_abort_is_its_success_and_review_status_finishes_it() {
    let (_dir, wallet) = wallet().await;
    let (a_txid, d_txid, coin_a, change_d) = broadcast_chain(&wallet, "abort-retire").await;
    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(LogCapture(logs.clone()));

    wallet.storage().busy_next_descendant_retire();
    let result = wallet
        .abort_action(
            AbortActionArgs {
                reference: "abort-retire".to_string(),
            },
            ORIGINATOR,
        )
        .await;
    assert!(
        matches!(result, Ok(AbortActionResult { aborted: true })),
        "the abort is committed: its success, never WalletBusy; got {result:?}"
    );
    let marked = logs
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains("post_commit_retire_failed"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(marked.len(), 1, "{marked:?}");
    assert!(
        marked[0].contains(&a_txid) && marked[0].contains("Store busy: sqlite code 5"),
        "{}",
        marked[0]
    );

    let spendable = |output_id: i64| {
        let pool = wallet.storage().pool().clone();
        async move {
            sqlx::query_scalar::<_, bool>("SELECT spendable FROM outputs WHERE output_id = ?")
                .bind(output_id)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(
        tx_and_req(&wallet, &a_txid).await,
        ("failed".to_string(), "invalid".to_string()),
        "the root's abort committed"
    );
    assert!(spendable(coin_a).await, "the root's input released");
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("unproven".to_string(), "unmined".to_string()),
        "the descendant not yet retired"
    );
    assert!(spendable(change_d).await);

    let review = crate::storage::MonitorStorage::review_status(wallet.storage())
        .await
        .unwrap();
    assert!(
        review
            .log
            .contains("Retired the unproven descendants of 1 failed transactions"),
        "{}",
        review.log
    );
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("failed".to_string(), "invalid".to_string()),
        "the review-status pass retired the descendant"
    );
    assert!(
        !spendable(change_d).await,
        "the descendant's change is dead"
    );

    let again = crate::storage::MonitorStorage::review_status(wallet.storage())
        .await
        .unwrap();
    assert_eq!(again.log, "No status mismatches found\n", "converged");
}

/// The review-status pass leaves the descendants of a failed root the chain
/// knows (a `mined` row): the abort's own rule.
#[tokio::test]
async fn review_status_leaves_the_descendants_of_a_failed_root_with_chain_evidence() {
    let (_dir, wallet) = wallet().await;
    let (a_txid, d_txid, _, _) = broadcast_chain(&wallet, "abort-evidence").await;
    for retired in [
        "UPDATE transactions SET status = 'failed' WHERE txid = ?",
        "UPDATE proven_tx_reqs SET status = 'invalid' WHERE txid = ?",
    ] {
        sqlx::query(retired)
            .bind(&a_txid)
            .execute(wallet.storage().writer())
            .await
            .unwrap();
    }
    wallet
        .storage()
        .record_broadcast_status(
            &a_txid,
            crate::services::broadcast_memory::PROVIDER_TAAL_ARC,
            crate::services::broadcast_memory::BROADCAST_STATUS_MINED,
        )
        .await
        .unwrap();

    crate::storage::MonitorStorage::review_status(wallet.storage())
        .await
        .unwrap();
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("unproven".to_string(), "unmined".to_string())
    );
}

/// Review 3 F3 of bsv-wallet-cli #44: internalizeAction's post-broadcast
/// site. The payment is committed and posted; the req's advance to
/// `unmined` after the post answers busy. The call answers its success, the
/// marker `post_broadcast_status_write_failed` is logged with the txid, the
/// req stays `unsent`, and the monitor's send-waiting pass re-posts it and
/// writes `unmined`.
#[tokio::test]
async fn a_busy_status_write_after_an_internalize_broadcast_is_its_success_and_send_waiting_heals_it(
) {
    let (_dir, wallet) = wallet().await;
    wallet
        .storage()
        .set_services(std::sync::Arc::new(MockWalletServices::new()));
    let mut parent = Transaction::new();
    let mut parent_input = TransactionInput::new("33".repeat(32), 0);
    parent_input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(parent_input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(20_000),
        locking_script: LockingScript::from_hex("7551").unwrap(),
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
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_002));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_transaction(tx);
    let atomic = beef.to_binary_atomic(&txid).unwrap();

    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(LogCapture(logs.clone()));
    wallet.storage().busy_next_status_write();
    let result = wallet
        .internalize_action(
            InternalizeActionArgs {
                tx: atomic,
                outputs: vec![InternalizeOutput {
                    output_index: 0,
                    protocol: "basket insertion".to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(BasketInsertion {
                        basket: "post-broadcast".to_string(),
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
        .await
        .expect("the payment is committed and posted: the call succeeds");
    assert!(result.accepted);
    assert!(
        !wallet.storage().take_busy_next_status_write(),
        "the post-broadcast write was reached"
    );
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
    let req = || {
        let pool = wallet.storage().pool().clone();
        let txid = txid.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(req().await, "unsent", "the req's advance did not land");

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
        req().await,
        "unmined",
        "the monitor's send-waiting pass healed the req"
    );
}

/// One store per file per process (bsv-wallet-cli #44, the CLI review of
/// 2026-10-21): the host daemon gave its `Wallet` and its `Monitor` two
/// `StorageSqlx` over the one file, two writer pools meeting in SQLite.
/// Built over two handles of one store (a clone), they hold one writer:
/// the writer taken through the monitor's handle is the wallet's too. Then
/// 300 rounds of the monitor's pass beside the wallet's `createAction`:
/// never `WalletBusy`, every action done.
#[tokio::test]
async fn a_wallet_and_a_monitor_over_one_store_share_the_writer() {
    const ROUNDS: u32 = 300;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.db").to_string_lossy().to_string();
    let store = StorageSqlx::open(&path).await.unwrap();
    let root = PrivateKey::random();
    store
        .migrate("shared-store-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    let monitor_store = std::sync::Arc::new(store.clone());
    let wallet = Wallet::new(Some(root), store, MockWalletServices::new())
        .await
        .unwrap();
    let monitor = crate::Monitor::new(
        monitor_store.clone(),
        std::sync::Arc::new(MockWalletServices::new()),
    );

    {
        let held = monitor_store.writer().acquire().await.unwrap();
        assert_eq!(wallet.storage().writer().size(), 1);
        assert_eq!(
            wallet.storage().writer().num_idle(),
            0,
            "the writer the monitor's handle holds is the wallet's writer"
        );
        drop(held);
    }

    let lock = own_lock(&wallet);
    for tag in 1..=ROUNDS {
        fund_tagged(&wallet, &lock, tag).await;
    }
    for round in 0..ROUNDS {
        let (created, pass) = tokio::join!(
            wallet.create_action(labelled_payment(true), ORIGINATOR),
            monitor.run_once()
        );
        match created {
            Ok(r) => assert!(r.txid.is_some(), "round {round}"),
            Err(e) => panic!("round {round}: createAction beside the monitor: {e:?}"),
        }
        if let Err(e) = pass {
            panic!("round {round}: the monitor's pass beside createAction: {e:?}");
        }
    }
    let (done,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM transactions WHERE is_outgoing = 1 AND status = 'nosend'",
    )
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap();
    assert_eq!(done, i64::from(ROUNDS));
    assert_eq!(wallet.storage().writer().size(), 1, "one writer connection");
}

/// The arguments of a direct `acquireCertificate` with one field.
fn direct_certificate_args() -> bsv_rs::wallet::AcquireCertificateArgs {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), "v".to_string());
    bsv_rs::wallet::AcquireCertificateArgs {
        certificate_type: "dGVzdC1jZXJ0LXR5cGU=".to_string(),
        certifier: "02".to_string() + &"ab".repeat(32),
        acquisition_protocol: bsv_rs::wallet::AcquisitionProtocol::Direct,
        fields,
        serial_number: Some("c2VyaWFsLW51bWJlcg==".to_string()),
        revocation_outpoint: Some("abc123.0".to_string()),
        signature: Some("deadbeef".to_string()),
        certifier_url: None,
        keyring_revealer: None,
        keyring_for_subject: None,
        privileged: None,
        privileged_reason: None,
    }
}

/// `(certificates, certificate_fields)` rows.
async fn certificate_rows(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> (i64, i64) {
    let pool = wallet.storage().pool();
    let certificates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM certificates")
        .fetch_one(pool)
        .await
        .unwrap();
    let fields: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM certificate_fields")
        .fetch_one(pool)
        .await
        .unwrap();
    (certificates, fields)
}

/// Review 4 G1 of bsv-wallet-cli #44: a direct `acquireCertificate` wrote
/// the certificate row, then each field as its own write; a busy at a field
/// answered `WalletBusy` with the row committed and no fields, and the repeat
/// refused `UNIQUE constraint failed: certificates`. The certificate and its
/// fields are one write transaction now: a busy at the field writes nothing,
/// the answer is `WalletBusy` (honest), and the same call repeated stores
/// the certificate with its field.
#[tokio::test]
async fn a_busy_certificate_field_insert_writes_nothing_and_is_wallet_busy() {
    let (_dir, wallet) = wallet().await;

    wallet.storage().busy_next_certificate_field();
    let first = wallet
        .acquire_certificate(direct_certificate_args(), ORIGINATOR)
        .await;
    assert!(
        matches!(first, Err(bsv_rs::Error::WalletBusy { .. })),
        "a busy before anything committed is WalletBusy: {:?}",
        first
    );
    assert_eq!(
        certificate_rows(&wallet).await,
        (0, 0),
        "nothing stored: the certificate and its fields are one write"
    );

    let repeat = wallet
        .acquire_certificate(direct_certificate_args(), ORIGINATOR)
        .await;
    assert!(repeat.is_ok(), "the repeat stores it: {:?}", repeat.err());
    assert_eq!(certificate_rows(&wallet).await, (1, 1));
}

/// The issued certificate `store_certificate` is handed after the exchange
/// with the certifier (serial number `[9; 32]`, one field), and the
/// certifier.
fn issued_certificate(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
) -> (
    super::super::certificate_issuance::ParseCertificateResponseResult,
    PublicKey,
) {
    use bsv_rs::auth::certificates::Certificate;

    let certifier = PrivateKey::random().public_key();
    let subject = PublicKey::from_hex(&wallet.identity_key).unwrap();
    let mut cert_fields = HashMap::new();
    cert_fields.insert("name".to_string(), "encrypted".to_string());
    let parsed = super::super::certificate_issuance::ParseCertificateResponseResult {
        certificate: Certificate::new([7u8; 32], [9u8; 32], subject, certifier.clone()),
        server_nonce: String::new(),
        cert_fields,
        serial_number: [9u8; 32],
    };
    (parsed, certifier)
}

/// `store_certificate` of the issued certificate.
async fn store_issued(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    parsed: &super::super::certificate_issuance::ParseCertificateResponseResult,
    certifier: &PublicKey,
) -> crate::Result<bsv_rs::wallet::WalletCertificate> {
    let mut keyring = HashMap::new();
    keyring.insert("name".to_string(), "key".to_string());
    super::super::certificate_issuance::store_certificate(
        wallet.storage(),
        &wallet.auth(),
        parsed,
        &keyring,
        certifier,
        &wallet.identity_key,
        "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=",
    )
    .await
}

/// Review 4 G1: on the issuance path the store is written after the
/// exchange with the certifier, a post to a third party that has issued. A
/// busy there is never `WalletBusy` (a host would repeat the exchange).
/// Review 5 H2: the store is tried again, `CERTIFICATE_STORE_TRIES` times in
/// all; a busy that never clears is a storage error, `Storage error: Store
/// busy: ...`, naming the certificate's serial number and certifier so an
/// operator can recover it, with nothing stored.
#[tokio::test]
async fn a_busy_store_after_the_certifier_exchange_is_never_wallet_busy() {
    use super::super::certificate_issuance::{
        CERTIFICATE_STORE_BACKOFF_MS, CERTIFICATE_STORE_TRIES,
    };

    let backoff_total: u64 = (0..CERTIFICATE_STORE_TRIES - 1)
        .map(|n| CERTIFICATE_STORE_BACKOFF_MS << n)
        .sum();
    assert!(
        backoff_total <= crate::storage::sqlx::STORE_BUSY_CEILING_MS,
        "the sleeps between tries are {} ms in all, at most the store's ceiling (each try can also wait up to that ceiling: about 16 s in one process, 31 s across processes)",
        backoff_total
    );

    let (_dir, wallet) = wallet().await;
    let (parsed, certifier) = issued_certificate(&wallet);

    // One busy more than the 3 tries: the call never sees the store clear.
    // The count is a literal, so fewer tries leaves more than one busy.
    wallet.storage().busy_next_certificate_fields(4);
    let error = store_issued(&wallet, &parsed, &certifier)
        .await
        .expect_err("the busy store is the caller's error");
    assert!(
        wallet.storage().take_busy_next_certificate_field()
            && !wallet.storage().take_busy_next_certificate_field(),
        "exactly 3 tries"
    );
    assert!(
        matches!(error, crate::Error::StorageError(_)),
        "a busy after the exchange is a storage error: {:?}",
        error
    );
    let message = error.to_string();
    assert!(
        message.starts_with("Storage error: Store busy: "),
        "the busy is wrapped once, a storage error: {}",
        message
    );
    assert!(
        message.contains("type BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="),
        "the error names the certificate type: {}",
        message
    );
    assert!(
        message.contains("tried 3 times"),
        "the error says the store was tried 3 times: {}",
        message
    );
    assert!(
        message.contains(&parsed.certificate.serial_number_base64()),
        "the error names the issued certificate's serial number: {}",
        message
    );
    assert!(
        message.contains(&certifier.to_hex()),
        "the error names the certifier: {}",
        message
    );
    let boundary: bsv_rs::Error = error.into();
    assert!(
        !matches!(boundary, bsv_rs::Error::WalletBusy { .. }),
        "never WalletBusy after the certifier issued: {:?}",
        boundary
    );
    assert!(
        boundary.to_string().contains("Storage error: Store busy"),
        "{}",
        boundary
    );
    assert_eq!(certificate_rows(&wallet).await, (0, 0), "nothing stored");
}

/// Review 5 H2: a busy that clears on the second try stores the issued
/// certificate with its field, and the call succeeds.
#[tokio::test]
async fn a_busy_store_that_clears_on_the_second_try_stores_the_issued_certificate() {
    let (_dir, wallet) = wallet().await;
    let (parsed, certifier) = issued_certificate(&wallet);

    wallet.storage().busy_next_certificate_fields(1);
    let stored = store_issued(&wallet, &parsed, &certifier)
        .await
        .expect("the second try stores it");
    assert_eq!(
        stored.serial_number,
        parsed.certificate.serial_number_base64()
    );
    assert!(
        !wallet.storage().take_busy_next_certificate_field(),
        "the busy was taken by the first try"
    );
    assert_eq!(certificate_rows(&wallet).await, (1, 1), "stored, once");
}

/// A root `failed` with its req at `req`, written directly (the shape an
/// abort or a rejection leaves).
async fn fail_root(wallet: &Wallet<StorageSqlx, MockWalletServices>, txid: &str, req: &str) {
    sqlx::query("UPDATE transactions SET status = 'failed' WHERE txid = ?")
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    sqlx::query("UPDATE proven_tx_reqs SET status = ? WHERE txid = ?")
        .bind(req)
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

/// `outputs.spendable` of `output_id`.
async fn output_spendable(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    output_id: i64,
) -> bool {
    sqlx::query_scalar("SELECT spendable FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

/// Review 4 G2 of bsv-wallet-cli #44: the review-status pass's descendants
/// check read chain evidence for the root only, so a spender a broadcaster
/// reports mined was retired (`failed` / `invalid`, its change dead). A
/// descendant with chain evidence now leaves the root's descendants alone.
#[tokio::test]
async fn review_status_leaves_the_descendants_when_a_descendant_has_chain_evidence() {
    let (_dir, wallet) = wallet().await;
    let (a_txid, d_txid, _, change_d) = broadcast_chain(&wallet, "g2").await;
    fail_root(&wallet, &a_txid, "invalid").await;
    wallet
        .storage()
        .record_broadcast_status(
            &d_txid,
            crate::services::broadcast_memory::PROVIDER_TAAL_ARC,
            crate::services::broadcast_memory::BROADCAST_STATUS_MINED,
        )
        .await
        .unwrap();

    let review = crate::storage::MonitorStorage::review_status(wallet.storage())
        .await
        .unwrap();
    assert_eq!(review.log, "No status mismatches found\n");
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("unproven".to_string(), "unmined".to_string()),
        "the spender the chain knows is not retired"
    );
    assert!(
        output_spendable(&wallet, change_d).await,
        "its change lives"
    );
}

/// Review 4 G3: a completed descendant made the descendants check count the
/// root as retired and log a line (and the retirement's refusal) on every
/// pass, forever. The root is skipped, uncounted: the passes say nothing.
#[tokio::test]
async fn review_status_with_a_completed_descendant_is_quiet_every_pass() {
    let (_dir, wallet) = wallet().await;
    let (a_txid, d_txid, _, change_d) = broadcast_chain(&wallet, "g3").await;
    let now = Utc::now();
    let grandchild = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, created_at, updated_at) \
         VALUES (?, 'completed', 'g3-grandchild', 1, 1000, 'a busy door test', ?, 1, 0, ?, ?)",
    )
    .bind(wallet.user_id)
    .bind("3".repeat(64))
    .bind(now)
    .bind(now)
    .execute(wallet.storage().writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query("UPDATE outputs SET spent_by = ?, spendable = 0 WHERE output_id = ?")
        .bind(grandchild)
        .bind(change_d)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    fail_root(&wallet, &a_txid, "invalid").await;
    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(LogCapture(logs.clone()));

    for pass in 0..2 {
        let review = crate::storage::MonitorStorage::review_status(wallet.storage())
            .await
            .unwrap();
        assert_eq!(review.log, "No status mismatches found\n", "pass {pass}");
    }
    let refusals = logs
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains("PROVEN") || line.contains("retired"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(refusals.is_empty(), "{refusals:?}");
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("unproven".to_string(), "unmined".to_string())
    );
}

/// Review 4 G4: `retire_one_poisoned` writes the req, then the transaction's
/// status. A retirement cut between the two (a busy, a process dying) leaves
/// an `invalid` req on a live row, and the next pass finishes it: its first
/// check fails the row. In the other order the cut would leave a `failed`
/// row with its req `unmined`, which no check revisits.
#[tokio::test]
async fn a_retirement_cut_between_its_two_writes_is_finished_by_the_next_pass() {
    let (_dir, wallet) = wallet().await;
    let (a_txid, d_txid, _, change_d) = broadcast_chain(&wallet, "g4").await;
    fail_root(&wallet, &a_txid, "invalid").await;

    wallet.storage().cut_next_retirement();
    let cut = crate::storage::MonitorStorage::review_status(wallet.storage()).await;
    assert!(
        matches!(cut, Err(crate::Error::StoreBusy { .. })),
        "the pass was cut: {cut:?}"
    );
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("unproven".to_string(), "invalid".to_string()),
        "cut after the req, before the status"
    );

    crate::storage::MonitorStorage::review_status(wallet.storage())
        .await
        .unwrap();
    assert_eq!(
        tx_and_req(&wallet, &d_txid).await,
        ("failed".to_string(), "invalid".to_string()),
        "the next pass finished the retirement"
    );
    assert!(
        !output_spendable(&wallet, change_d).await,
        "its change is dead"
    );

    let again = crate::storage::MonitorStorage::review_status(wallet.storage())
        .await
        .unwrap();
    assert_eq!(again.log, "No status mismatches found\n", "converged");
}
