//! Helpers shared by the basket insertion integration tests
//! (`internalize_insertion_tests.rs`, `internalize_insertion_host_tests.rs`):
//! a wallet over in-memory SQLite, the two doors' answers, a funded wallet,
//! a transaction it makes from its own change, and the stored rows read
//! back. The crate root declares `common` (`internalize_payment.rs`) beside
//! this module.
//!
//! Each test file uses part of this module, hence `dead_code` allowed.

#![allow(dead_code)]

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, WalletInterface,
};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{AuthId, Error, StorageSqlx, Wallet, WalletStorageWriter};

use super::common::{self, args, beef_paying, other_lock, p2pkh_bytes, refusal};

pub const PREFIX: &str = "aW5zZXJ0LXByZWZpeA==";
pub const SUFFIX: &str = "aW5zZXJ0LXN1ZmZpeA==";

/// The reference's sentence for `default` as the insertion basket.
pub const NON_DEFAULT: &str =
    "The insertionRemittance.basket parameter must be a non-default basket";

/// Every spelling the reference's validateBasket maps to `default`.
pub const DEFAULT_SPELLINGS: [&str; 5] = [
    "default",
    "DEFAULT",
    " Default ",
    "\u{FEFF}default\t",
    "\ndeFAULT\u{00A0}",
];

pub type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

pub async fn wallet(root: &PrivateKey, txid: &str) -> TestWallet {
    common::wallet("internalize-insertion-tests", root, txid).await
}

pub async fn storage_auth(wallet: &TestWallet) -> AuthId {
    let identity = wallet.identity_key().to_string();
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&identity)
        .await
        .unwrap();
    AuthId::with_user_id(&identity, user.user_id)
}

pub fn insertion(vout: u32, basket: &str) -> InternalizeOutput {
    InternalizeOutput {
        output_index: vout,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: basket.to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }
}

/// The store's output and basket counts.
pub async fn counts(wallet: &TestWallet) -> (i64, i64) {
    let writer = wallet.storage().writer();
    let outputs = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(writer)
        .await
        .unwrap();
    let baskets = sqlx::query_scalar("SELECT COUNT(*) FROM output_baskets")
        .fetch_one(writer)
        .await
        .unwrap();
    (outputs, baskets)
}

/// The stored row of `txid.vout`: basket name, type, change, derivation
/// prefix, suffix and sender.
pub type Row = (
    Option<String>,
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub async fn row(wallet: &TestWallet, txid: &str, vout: u32) -> Row {
    sqlx::query_as(
        "SELECT b.name, o.type, o.change, o.derivation_prefix, o.derivation_suffix, \
         o.sender_identity_key FROM outputs o LEFT JOIN output_baskets b \
         ON b.basket_id = o.basket_id WHERE o.txid = ? AND o.vout = ?",
    )
    .bind(txid)
    .bind(vout)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

pub async fn balance(wallet: &TestWallet) -> u64 {
    wallet.balance().await.unwrap().total
}

/// The two doors, each answering the class and message of a refusal.
pub async fn at_wallet(
    wallet: &TestWallet,
    args: InternalizeActionArgs,
) -> Result<bool, (WalletRefusalClass, String)> {
    match wallet.internalize_action(args, Some("test.local")).await {
        Ok(r) => Ok(r.accepted),
        Err(e) => Err(refusal(Err(e), "wallet door")),
    }
}

pub async fn at_storage(
    wallet: &TestWallet,
    args: InternalizeActionArgs,
) -> Result<bool, (WalletRefusalClass, String)> {
    let auth = storage_auth(wallet).await;
    match wallet.storage().internalize_action(&auth, args).await {
        Ok(r) => Ok(r.base.accepted),
        // The storage door's own variant: `ValidationError` is the one the
        // wallet boundary carries as `InvalidParameter`.
        Err(e @ Error::ValidationError(_)) => {
            Err((WalletRefusalClass::InvalidParameter, e.to_string()))
        }
        Err(other) => panic!("storage door: not a validation error: {other:?}"),
    }
}

/// Funds a wallet of `root` with a received payment of 4500 satoshis and
/// answers it with the payment's atomic BEEF.
pub async fn funded(root: &PrivateKey) -> (TestWallet, Vec<u8>) {
    let sender = PrivateKey::random().public_key();
    let derived = common::derived_key(root, &sender, PREFIX, SUFFIX);
    let lock = bsv_rs::script::LockingScript::from_binary(&p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = beef_paying(lock);
    let wallet = wallet(root, &txid).await;
    let paid = wallet
        .internalize_action(
            args(bytes.clone(), common::payment(&sender, PREFIX, SUFFIX)),
            Some("test.local"),
        )
        .await
        .expect("the payment is internalized");
    assert!(paid.accepted);
    assert_eq!(balance(&wallet).await, 4500);
    (wallet, bytes)
}

/// A transaction the wallet makes from its own change: one 1000-satoshi
/// output locked by `lock` (another key's when `None`), the change back to
/// the wallet, not sent. Answers its atomic BEEF, its txid, the index of
/// its change output and of the 1000-satoshi output.
pub async fn made(
    wallet: &TestWallet,
    funding: &[u8],
    lock: Option<Vec<u8>>,
) -> (Vec<u8>, String, u32, u32) {
    let made = wallet
        .create_action(
            CreateActionArgs {
                description: "a payment with change".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: lock.unwrap_or_else(|| other_lock().to_binary()),
                    satoshis: 1_000,
                    output_description: "a payment".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: Some(vec!["underReceivedRule".to_string()]),
                options: Some(CreateActionOptions {
                    no_send: Some(true),
                    ..Default::default()
                }),
            },
            Some("test.local"),
        )
        .await
        .expect("createAction");
    let txid = hex::encode(made.txid.expect("txid"));
    let vout_where = |change: i64| {
        sqlx::query_scalar::<_, i64>("SELECT vout FROM outputs WHERE txid = ? AND change = ?")
            .bind(txid.clone())
            .bind(change)
            .fetch_one(wallet.storage().writer())
    };
    let change = vout_where(1).await.expect("the change output");
    let paid = vout_where(0).await.expect("the payment output");
    // The noSend answer is the raw transaction; its atomic BEEF carries the
    // payment it spends and that payment's proven parent.
    let mut beef = Beef::from_binary(funding).expect("the payment's BEEF");
    beef.merge_raw_tx(made.tx.expect("the raw transaction"), None)
        .unwrap();
    let atomic = beef.to_binary_atomic(&txid).unwrap();
    let index = |vout: i64| u32::try_from(vout).unwrap();
    (atomic, txid, index(change), index(paid))
}

/// [`made`] with the payment to another key: its atomic BEEF, its txid and
/// its change output's index.
pub async fn own_change(wallet: &TestWallet, funding: &[u8]) -> (Vec<u8>, String, u32) {
    let (atomic, txid, change, _) = made(wallet, funding, None).await;
    (atomic, txid, change)
}

/// Every column of the stored row of `txid.vout`, quoted, in one string.
pub async fn every_column(wallet: &TestWallet, txid: &str, vout: u32) -> String {
    sqlx::query_scalar(
        "SELECT quote(output_id) || '|' || quote(user_id) || '|' || quote(transaction_id)          || '|' || quote(basket_id) || '|' || quote(spendable) || '|' || quote(change)          || '|' || quote(vout) || '|' || quote(satoshis) || '|' || quote(provided_by)          || '|' || quote(purpose) || '|' || quote(type) || '|' || quote(output_description)          || '|' || quote(txid) || '|' || quote(sender_identity_key)          || '|' || quote(derivation_prefix) || '|' || quote(derivation_suffix)          || '|' || quote(custom_instructions) || '|' || quote(spent_by)          || '|' || quote(sequence_number) || '|' || quote(spending_description)          || '|' || quote(script_length) || '|' || quote(script_offset)          || '|' || quote(locking_script) || '|' || quote(created_at)          || '|' || quote(updated_at) FROM outputs WHERE txid = ? AND vout = ?",
    )
    .bind(txid)
    .bind(vout)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

/// The stored `provided_by` and `purpose` of `txid.vout`.
pub async fn origin(wallet: &TestWallet, txid: &str, vout: u32) -> (String, String) {
    sqlx::query_as("SELECT provided_by, purpose FROM outputs WHERE txid = ? AND vout = ?")
        .bind(txid)
        .bind(vout)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// The refusal of a basket insertion naming wallet-managed change.
pub fn managed_change_refusal(vout: u32) -> String {
    format!(
        "Validation error: The outputs parameter must be output {vout} is wallet-managed \
         change and cannot be reclassified as a basket insertion"
    )
}
