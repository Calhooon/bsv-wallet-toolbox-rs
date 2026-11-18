//! The selection of change on a store made active by a switch: each coin the
//! other store spent arrives here not spendable, with no record of what
//! spent it, and no action takes it, whether it accepts delayed broadcast
//! or not. Two in-memory stores under a `WalletStorageManager`, a wallet on
//! it, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{
    MonitorStorage, StorageSqlx, Wallet, WalletStorageManager, WalletStorageWriter,
};

use common::{derived_key, other_lock, p2pkh_bytes, paying_derived, payment, wallet_on_manager};

const PREFIX: &str = "c3dpdGNoZWQtcHJlZml4";
const SUFFIX: &str = "c3dpdGNoZWQtc3VmZml4";
const RULE: &str = "underReceivedRule";
const A: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const B: &str = "2222222222222222222222222222222222222222222222222222222222222222";

type ManagedWallet = Wallet<WalletStorageManager, MockWalletServices>;

/// A store of `sik` holding the wallet's user, whose row names A active.
async fn store(sik: &str, identity: &str) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("switched-store-selection", sik).await.unwrap();
    s.make_available().await.unwrap();
    let (u, _) = s.find_or_insert_user(identity).await.unwrap();
    s.update_user_active_storage(u.user_id, A).await.unwrap();
    s
}

/// An action paying `satoshis` to another key, funded by the wallet's own
/// selection, posted at once unless `delayed`.
fn pay(satoshis: u64, delayed: bool, labels: &[&str]) -> CreateActionArgs {
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
            accept_delayed_broadcast: Some(delayed),
            randomize_outputs: Some(false),
            ..Default::default()
        }),
    }
}

/// `args` created: the outpoints its transaction spends, or the refusal.
async fn create(wallet: &ManagedWallet, args: CreateActionArgs) -> Result<Vec<String>, String> {
    create_on(wallet, args).await
}

/// `args` created on any wallet: the outpoints its transaction spends, or
/// the refusal.
async fn create_on<S, V>(
    wallet: &Wallet<S, V>,
    args: CreateActionArgs,
) -> Result<Vec<String>, String>
where
    S: bsv_wallet_toolbox_rs::WalletStorageProvider + Send + Sync + 'static,
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

/// Every change row a store holds: `txid.vout sats spendable spent_by status`.
async fn change_rows(s: &StorageSqlx) -> Vec<String> {
    ::sqlx::query_as::<_, (String, i64, i64, i64, Option<i64>, String)>(
        "SELECT o.txid, o.vout, o.satoshis, o.spendable, o.spent_by, t.status FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.change = 1 ORDER BY o.output_id",
    )
    .fetch_all(s.pool())
    .await
    .unwrap()
    .into_iter()
    .map(|(txid, vout, sats, spendable, spent_by, status)| {
        format!("{txid}.{vout} {sats} spendable={spendable} spent_by={spent_by:?} {status}")
    })
    .collect()
}

/// A wallet's coin spent on A by an action posted and taken, the push, the
/// switch to B: on B the coin is not spendable with no spender, and an
/// action that needs its value is refused, accepting delayed broadcast or
/// not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_coin_the_other_store_spent_is_offered_to_no_action() {
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

    let sender = PrivateKey::random().public_key();
    let (tx, _) = paying_derived(&root, &sender, PREFIX, SUFFIX);
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
    // The received coin spent by action one, its change spent by action
    // two: both posted and taken on A.
    let one = create(&wallet, pay(1_000, false, &[RULE])).await.unwrap();
    let two = create(&wallet, pay(1_000, false, &[])).await.unwrap();
    let spent = two[0].clone();
    let rows_a = change_rows(&a).await;
    println!("action one spends {one:?}, action two spends {two:?}; A: {rows_a:#?}");
    assert_eq!(two.len(), 1);
    assert!(
        rows_a
            .iter()
            .any(|r| r.starts_with(&format!("{spent} 8477 spendable=0 spent_by=Some("))),
        "{rows_a:?}"
    );

    wallet.storage().update_backups().await.expect("the push");
    wallet.storage().set_active(B).await.expect("the switch");
    let rows_b = change_rows(&b).await;
    println!("B after the switch: {rows_b:#?}");
    let spent_row = format!("{spent} 8477 spendable=0 spent_by=None unproven");
    assert!(rows_b.contains(&spent_row), "{rows_b:?}");
    let free: Vec<&String> = rows_b
        .iter()
        .filter(|r| r.contains("spendable=1"))
        .collect();
    assert_eq!(free.len(), 1, "{rows_b:?}");

    // An action that needs the spent coin's value: refused, accepting
    // delayed broadcast or not.
    let not_delayed = create(&wallet, pay(8_000, false, &[])).await;
    let delayed = create(&wallet, pay(8_000, true, &[])).await;
    println!("WITNESS on B, not delayed: {not_delayed:?}; delayed: {delayed:?}");
    let refusal = "wallet error: Insufficient funds: need 8020, have 7454".to_string();
    assert_eq!(not_delayed, Err(refusal.clone()));
    assert_eq!(delayed, Err(refusal));
    assert!(change_rows(&b).await.contains(&spent_row));

    // A spendable coin is still taken by an action accepting delayed
    // broadcast, and only that coin.
    let taken = create(&wallet, pay(5_000, true, &[])).await.unwrap();
    println!("a delayed action of 5000 on B spends {taken:?}");
    assert_eq!(taken.len(), 1);
    assert!(free[0].starts_with(&format!("{} ", taken[0])), "{free:?}");
    assert!(change_rows(&b).await.contains(&spent_row));
}

/// A wallet of `root` on a clone of `store`, posting accepted.
async fn wallet_on(
    root: &PrivateKey,
    store: &StorageSqlx,
) -> Wallet<StorageSqlx, MockWalletServices> {
    let services = MockWalletServices::builder().post_beef_success().build();
    Wallet::new(Some(root.clone()), store.clone(), services)
        .await
        .unwrap()
}

/// One store, two users: Y's `noSend` action pays X, Y aborts it, X
/// internalizes its transaction. The internalize marks Y's row of the coin
/// the transaction spends not spendable and records no spender (the row is
/// another user's). An action of Y's that needs that coin's value is
/// refused, accepting delayed broadcast or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_coin_another_users_payment_spent_is_offered_to_no_action() {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("one-store-two-users", A).await.unwrap();
    store.make_available().await.unwrap();
    let y_root = PrivateKey::random();
    let x_root = PrivateKey::random();
    let y = wallet_on(&y_root, &store).await;
    let x = wallet_on(&x_root, &store).await;

    let sender = PrivateKey::random().public_key();
    let (tx, _) = paying_derived(&y_root, &sender, PREFIX, SUFFIX);
    y.internalize_action(
        bsv_rs::wallet::InternalizeActionArgs {
            tx,
            outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
            description: "a payment to Y".to_string(),
            labels: None,
            seek_permission: None,
        },
        "test.local",
    )
    .await
    .expect("the payment");
    // Y's own change, posted and taken.
    let one = create_on(&y, pay(1_000, false, &[RULE])).await.unwrap();
    println!("Y's action one spends {one:?}");

    // Y's noSend action paying X, signed, then aborted.
    let y_key = y_root.public_key();
    let x_lock = p2pkh_bytes(&derived_key(&x_root, &y_key, PREFIX, SUFFIX));
    let mut to_x = pay(1_000, false, &[]);
    to_x.outputs.as_mut().unwrap()[0].locking_script = x_lock;
    let options = to_x.options.as_mut().unwrap();
    options.no_send = Some(true);
    options.sign_and_process = Some(false);
    let created = y.create_action(to_x, "test.local").await.unwrap();
    let signable = created
        .signable_transaction
        .expect("a signable transaction");
    let reference = String::from_utf8(signable.reference).unwrap();
    let signed = y
        .sign_action(
            bsv_rs::wallet::SignActionArgs {
                spends: std::collections::HashMap::new(),
                reference: reference.clone(),
                options: None,
            },
            "test.local",
        )
        .await
        .unwrap();
    let beef = signed.tx.expect("the signed AtomicBEEF");
    let aborted = y
        .abort_action(bsv_rs::wallet::AbortActionArgs { reference }, "test.local")
        .await;
    println!("Y's abort: {aborted:?}");
    let before = change_rows(&store).await;
    println!("after the abort: {before:#?}");
    let taken = x
        .internalize_action(
            bsv_rs::wallet::InternalizeActionArgs {
                tx: beef,
                outputs: vec![payment(0, PREFIX, SUFFIX, &y_key.to_hex())],
                description: "Y pays X".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await;
    println!("X's internalize: {taken:?}");
    let rows = change_rows(&store).await;
    println!("after X's internalize: {rows:#?}");

    let not_delayed = create_on(&y, pay(8_000, false, &[])).await;
    let delayed = create_on(&y, pay(8_000, true, &[])).await;
    println!("WITNESS one store, Y not delayed: {not_delayed:?}; delayed: {delayed:?}");
    assert_eq!(aborted.map(|r| r.aborted), Ok(true));
    assert_eq!(taken.map(|r| r.accepted), Ok(true));
    let held = rows
        .iter()
        .find(|r| r.contains(" 8477 "))
        .expect("Y's change")
        .clone();
    assert!(
        held.ends_with(" 8477 spendable=0 spent_by=None unproven"),
        "{held}"
    );
    let refusal = "wallet error: Insufficient funds: need 8005, have 0".to_string();
    assert_eq!(not_delayed, Err(refusal.clone()));
    assert_eq!(delayed, Err(refusal));
    assert!(change_rows(&store).await.contains(&held));
}
