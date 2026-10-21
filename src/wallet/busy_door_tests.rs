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
    .unwrap();
}

fn assert_wallet_busy<T: std::fmt::Debug>(result: bsv_rs::Result<T>) {
    match result {
        Err(bsv_rs::Error::WalletBusy {
            detail,
            retry_after_ms,
        }) => {
            assert_eq!(
                detail,
                "Store busy: the writer was not acquired within the store's ceiling"
            );
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
