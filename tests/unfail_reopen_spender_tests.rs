//! The unfail task's reopen of a recovered transaction's outputs reads what
//! spent each one. A provider's unspent answer is a word about the chain it
//! sees; a spend the wallet made that is not mined, or not yet seen there,
//! is not in it. An output is reopened only when a provider answers it
//! unspent, its row records no spender, and no transaction the store holds
//! that is not failed spends it by its bytes. In-memory stores, a mock
//! broadcaster and mock chain answers; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    GetStatusForTxidsResult, MonitorStorage, StorageSqlx, TxStatusDetail, Wallet,
    WalletStorageManager, WalletStorageProvider, WalletStorageWriter,
};

use common::{other_lock, paying_derived, payment, wallet_on_manager};

const PREFIX: &str = "dW5mYWlsLXByZWZpeA==";
const SUFFIX: &str = "dW5mYWlsLXN1ZmZpeA==";
const RULE: &str = "underReceivedRule";
const A: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const B: &str = "2222222222222222222222222222222222222222222222222222222222222222";

async fn store(sik: &str, identity: &str) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("unfail-reopen", sik).await.unwrap();
    s.make_available().await.unwrap();
    let (u, _) = s.find_or_insert_user(identity).await.unwrap();
    s.update_user_active_storage(u.user_id, A).await.unwrap();
    s
}

fn pay(satoshis: u64, labels: &[&str]) -> CreateActionArgs {
    CreateActionArgs {
        description: "a payment from the wallet".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: other_lock().to_binary(),
            satoshis,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: (!labels.is_empty()).then(|| labels.iter().map(|l| l.to_string()).collect()),
        options: Some(CreateActionOptions {
            accept_delayed_broadcast: Some(false),
            randomize_outputs: Some(false),
            ..Default::default()
        }),
    }
}

/// `args` created: the outpoints its transaction spends, or the refusal.
async fn create<S, V>(wallet: &Wallet<S, V>, args: CreateActionArgs) -> Result<Vec<String>, String>
where
    S: WalletStorageProvider + Send + Sync + 'static,
    V: bsv_wallet_toolbox_rs::WalletServices + Send + Sync + 'static,
{
    let created = wallet
        .create_action(args, "test.local")
        .await
        .map_err(|e| e.to_string())?;
    let tx = Transaction::from_binary(&created.tx.expect("the signed transaction")).unwrap();
    Ok(tx
        .inputs
        .iter()
        .map(|i| format!("{}.{}", i.get_source_txid().unwrap(), i.source_output_index))
        .collect())
}

/// A payment of 9500 to the wallet, internalized.
async fn receive<S, V>(wallet: &Wallet<S, V>, root: &PrivateKey)
where
    S: WalletStorageProvider + Send + Sync + 'static,
    V: bsv_wallet_toolbox_rs::WalletServices + Send + Sync + 'static,
{
    let sender = PrivateKey::random().public_key();
    let (tx, _) = paying_derived(root, &sender, PREFIX, SUFFIX);
    wallet
        .internalize_action(
            bsv_rs::wallet::InternalizeActionArgs {
                tx,
                outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
                description: "a payment to the wallet".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .expect("the payment");
}

/// The output row at `outpoint` (`txid.vout`): `spendable spent_by`.
async fn row(s: &StorageSqlx, outpoint: &str) -> String {
    let (txid, vout) = outpoint.split_once('.').unwrap();
    let (spendable, spent_by): (i64, Option<i64>) =
        ::sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?")
            .bind(txid)
            .bind(vout.parse::<i64>().unwrap())
            .fetch_one(s.pool())
            .await
            .unwrap();
    format!("spendable={spendable} spent_by={spent_by:?}")
}

/// `txid` failed on `s` as a false fail is: the transaction `failed`, its
/// request `invalid` and aged past the canary's backoff.
async fn fail(s: &StorageSqlx, txid: &str) {
    let aged = chrono::Utc::now() - chrono::Duration::hours(2);
    ::sqlx::query("UPDATE transactions SET status = 'failed' WHERE txid = ?")
        .bind(txid)
        .execute(s.writer())
        .await
        .unwrap();
    let raw: Vec<u8> = ::sqlx::query_scalar("SELECT raw_tx FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    let moved = ::sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'invalid', attempts = 0, updated_at = ? WHERE txid = ?",
    )
    .bind(aged)
    .bind(txid)
    .execute(s.writer())
    .await
    .unwrap()
    .rows_affected();
    if moved == 0 {
        ::sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notify, notified, raw_tx, created_at, updated_at) \
             VALUES (?, 'invalid', 0, '{}', '{}', 0, ?, ?, ?)",
        )
        .bind(txid)
        .bind(raw)
        .bind(aged)
        .bind(aged)
        .execute(s.writer())
        .await
        .unwrap();
    }
}

/// The unfail task on `s` where the chain answers `txid` mined and every
/// output unspent.
async fn unfail(s: &StorageSqlx, txid: &str) {
    let services = MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "mock".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: txid.to_string(),
                status: "mined".to_string(),
                depth: Some(1),
                ..Default::default()
            }],
        }))
        .is_utxo_response(MockResponse::Success(true))
        .post_beef_success()
        .build();
    WalletStorageProvider::set_services(s, Arc::new(services));
    s.un_fail().await.expect("the unfail task");
    let status: String = ::sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert_eq!(status, "unproven", "the transaction is recovered");
}

async fn wallet_on(root: &PrivateKey, s: &StorageSqlx) -> Wallet<StorageSqlx, MockWalletServices> {
    let services = MockWalletServices::builder().post_beef_success().build();
    Wallet::new(Some(root.clone()), s.clone(), services)
        .await
        .unwrap()
}

/// One store: action one's change spent by action two, posted and taken;
/// action one falsely failed, then recovered by the task. The change records
/// its spender and stays not spendable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_output_a_later_action_spent_is_not_reopened() {
    let root = PrivateKey::random();
    let s = store(A, &root.public_key().to_hex()).await;
    let wallet = wallet_on(&root, &s).await;
    receive(&wallet, &root).await;
    create(&wallet, pay(1_000, &[RULE])).await.unwrap();
    let two = create(&wallet, pay(1_000, &[])).await.unwrap();
    assert_eq!(two.len(), 1);
    let change = two[0].clone();
    let one_txid = change.split_once('.').unwrap().0.to_string();
    let before = row(&s, &change).await;
    assert!(before.starts_with("spendable=0 spent_by=Some("), "{before}");

    fail(&s, &one_txid).await;
    unfail(&s, &one_txid).await;
    let after = row(&s, &change).await;
    println!("WITNESS one store: action one's change {change}: before {before}, after the unfail {after}");
    assert_eq!(after, before);
}

/// Two stores under a manager: on A action one's change is spent by action
/// two; the push and the switch to B, where the change is not spendable with
/// no spender and action two's row and bytes are held; action one falsely
/// failed on B, then recovered by the task. The change stays not spendable
/// on B, and an action needing its value is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_output_the_other_store_spent_is_not_reopened_after_a_switch() {
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    let a = store(A, &identity).await;
    let b = store(B, &identity).await;
    let manager = WalletStorageManager::new(
        identity.clone(),
        Some(a.clone() as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    manager.make_available().await.unwrap();
    let services = MockWalletServices::builder().post_beef_success().build();
    let wallet = wallet_on_manager(&root, manager, services);
    receive(&wallet, &root).await;
    create(&wallet, pay(1_000, &[RULE])).await.unwrap();
    let two = create(&wallet, pay(1_000, &[])).await.unwrap();
    let change = two[0].clone();
    let one_txid = change.split_once('.').unwrap().0.to_string();

    wallet.storage().update_backups().await.expect("the push");
    wallet.storage().set_active(B).await.expect("the switch");
    let on_b = row(&b, &change).await;
    assert_eq!(
        on_b, "spendable=0 spent_by=None",
        "the sync carries no spender"
    );

    fail(&b, &one_txid).await;
    unfail(&b, &one_txid).await;
    let after = row(&b, &change).await;
    let taken = create(&wallet, pay(8_000, &[])).await;
    println!(
        "WITNESS two stores: action one's change {change} on B: after the switch {on_b}, \
         after the unfail {after}; an action of 8000 on B: {taken:?}"
    );
    assert_eq!(after, "spendable=0 spent_by=None");
    assert!(
        taken
            .as_ref()
            .is_err_and(|e| e.contains("Insufficient funds")),
        "{taken:?}"
    );
}

/// One store: action one's change, which nothing spends, not spendable as a
/// fail leaves it; action one recovered by the task: the change is reopened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_output_nothing_spends_is_reopened() {
    let root = PrivateKey::random();
    let s = store(A, &root.public_key().to_hex()).await;
    let wallet = wallet_on(&root, &s).await;
    receive(&wallet, &root).await;
    let one = create(&wallet, pay(1_000, &[RULE])).await.unwrap();
    let received_txid = one[0].split_once('.').unwrap().0.to_string();
    let one_txid: String = ::sqlx::query_scalar(
        "SELECT txid FROM transactions WHERE txid IS NOT NULL AND txid != ? ORDER BY transaction_id DESC LIMIT 1",
    )
    .bind(&received_txid)
    .fetch_one(s.pool())
    .await
    .unwrap();
    let change = format!("{one_txid}.1");
    assert_eq!(row(&s, &change).await, "spendable=1 spent_by=None");
    ::sqlx::query("UPDATE outputs SET spendable = 0 WHERE txid = ?")
        .bind(&one_txid)
        .execute(s.writer())
        .await
        .unwrap();

    fail(&s, &one_txid).await;
    unfail(&s, &one_txid).await;
    let after = row(&s, &change).await;
    println!("WITNESS nothing spends {change}: after the unfail {after}");
    assert_eq!(after, "spendable=1 spent_by=None");
}
