//! The basket insertion protocol of `internalizeAction`, at the wallet door
//! (`Wallet::internalize_action`) and at the storage door
//! (`StorageSqlx::internalize_action`), as the reference has it
//! (ts-stack@4d0ea63c8 `signer/methods/internalizeAction.ts:93-99`,
//! `storage/methods/internalizeAction.ts:282-307, 360-406`):
//!
//! - `default`, in every spelling the reference's basket validator maps to
//!   it (trimmed, lowercased), is refused as the insertion basket, before
//!   any row is written and any basket created;
//! - an existing output that is wallet-managed change is not reclassified as
//!   a basket insertion: the row keeps its type, its change flag and its
//!   derivation, and the balance does not move;
//! - an existing output in another application basket is not moved into the
//!   requested basket, and the requested basket is not created; the same
//!   basket named again is admitted;
//! - an output in the default basket that is not wallet-managed change (the
//!   legacy recovery state) is swept into a real basket.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing touches the
//! network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, WalletInterface,
};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, Wallet, WalletStorageWriter};

use common::{args, beef_paying, other_lock, p2pkh_bytes, refusal};

const PREFIX: &str = "aW5zZXJ0LXByZWZpeA==";
const SUFFIX: &str = "aW5zZXJ0LXN1ZmZpeA==";

/// The reference's sentence for `default` as the insertion basket.
const NON_DEFAULT: &str = "The insertionRemittance.basket parameter must be a non-default basket";

/// Every spelling the reference's validateBasket maps to `default`.
const DEFAULT_SPELLINGS: [&str; 5] = [
    "default",
    "DEFAULT",
    " Default ",
    "\u{FEFF}default\t",
    "\ndeFAULT\u{00A0}",
];

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

async fn wallet(root: &PrivateKey, txid: &str) -> TestWallet {
    common::wallet("internalize-insertion-tests", root, txid).await
}

async fn storage_auth(wallet: &TestWallet) -> AuthId {
    let identity = wallet.identity_key().to_string();
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&identity)
        .await
        .unwrap();
    AuthId::with_user_id(&identity, user.user_id)
}

fn insertion(vout: u32, basket: &str) -> InternalizeOutput {
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
async fn counts(wallet: &TestWallet) -> (i64, i64) {
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
type Row = (
    Option<String>,
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn row(wallet: &TestWallet, txid: &str, vout: u32) -> Row {
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

async fn balance(wallet: &TestWallet) -> u64 {
    wallet.balance().await.unwrap().total
}

/// The two doors, each answering the class and message of a refusal.
async fn at_wallet(
    wallet: &TestWallet,
    args: InternalizeActionArgs,
) -> Result<bool, (WalletRefusalClass, String)> {
    match wallet.internalize_action(args, Some("test.local")).await {
        Ok(r) => Ok(r.accepted),
        Err(e) => Err(refusal(Err(e), "wallet door")),
    }
}

async fn at_storage(
    wallet: &TestWallet,
    args: InternalizeActionArgs,
) -> Result<bool, (WalletRefusalClass, String)> {
    let auth = storage_auth(wallet).await;
    match wallet.storage().internalize_action(&auth, args).await {
        Ok(r) => Ok(r.base.accepted),
        Err(e) => Err((WalletRefusalClass::InvalidParameter, e.to_string())),
    }
}

/// Funds a wallet of `root` with a received payment of 4500 satoshis and
/// answers it with the payment's atomic BEEF.
async fn funded(root: &PrivateKey) -> (TestWallet, Vec<u8>) {
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
/// output to another key, the change back to the wallet, not sent. Answers
/// its atomic BEEF, its txid and its change output's index.
async fn own_change(wallet: &TestWallet, funding: &[u8]) -> (Vec<u8>, String, u32) {
    let made = wallet
        .create_action(
            CreateActionArgs {
                description: "a payment with change".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: other_lock().to_binary(),
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
    let vout: i64 = sqlx::query_scalar("SELECT vout FROM outputs WHERE txid = ? AND change = 1")
        .bind(&txid)
        .fetch_one(wallet.storage().writer())
        .await
        .expect("the change output");
    // The noSend answer is the raw transaction; its atomic BEEF carries the
    // payment it spends and that payment's proven parent.
    let mut beef = Beef::from_binary(funding).expect("the payment's BEEF");
    beef.merge_raw_tx(made.tx.expect("the raw transaction"), None)
        .unwrap();
    let atomic = beef.to_binary_atomic(&txid).unwrap();
    (atomic, txid, u32::try_from(vout).unwrap())
}

/// A basket insertion naming `default`, in each spelling the reference's
/// validator maps to it, at each door: refused with the reference's
/// sentence, no output stored, no basket created.
#[tokio::test]
async fn a_basket_insertion_into_the_default_basket_is_refused_in_every_spelling() {
    for spelling in DEFAULT_SPELLINGS {
        let (bytes, txid) = beef_paying(other_lock());
        let wallet = wallet(&PrivateKey::random(), &txid).await;
        let before = counts(&wallet).await;
        let answer = at_wallet(&wallet, args(bytes.clone(), insertion(0, spelling))).await;
        assert_eq!(
            answer,
            Err((
                WalletRefusalClass::InvalidParameter,
                format!("Validation error: {NON_DEFAULT}")
            )),
            "wallet door, {spelling:?}"
        );
        assert_eq!(counts(&wallet).await, before, "wallet door, {spelling:?}");

        let answer = at_storage(&wallet, args(bytes, insertion(0, spelling))).await;
        assert_eq!(
            answer.map_err(|(_, text)| text),
            Err(format!("Validation error: {NON_DEFAULT}")),
            "storage door, {spelling:?}"
        );
        assert_eq!(counts(&wallet).await, before, "storage door, {spelling:?}");
    }
}

/// A basket insertion into a named basket, at each door: stored in that
/// basket as a custom output, no balance moved.
#[tokio::test]
async fn a_basket_insertion_into_a_named_basket_is_stored() {
    for door in ["wallet", "storage"] {
        let (bytes, txid) = beef_paying(other_lock());
        let wallet = wallet(&PrivateKey::random(), &txid).await;
        let args = args(bytes, insertion(0, "tokens"));
        let answer = match door {
            "wallet" => at_wallet(&wallet, args).await,
            _ => at_storage(&wallet, args).await,
        };
        assert_eq!(answer, Ok(true), "{door}");
        let (basket, kind, change, ..) = row(&wallet, &txid, 0).await;
        assert_eq!(
            (basket.as_deref(), kind.as_str(), change),
            (Some("tokens"), "custom", 0),
            "{door}"
        );
    }
}

/// The wallet's own change of a transaction it made, named by a basket
/// insertion on the merge path, at each door: refused with the reference's
/// sentence; the row keeps its basket, type, change flag and derivation,
/// the balance does not move and the requested basket is not created.
#[tokio::test]
async fn the_wallets_own_change_is_not_reclassified_as_a_basket_insertion() {
    for door in ["wallet", "storage"] {
        let root = PrivateKey::random();
        let (wallet, funding) = funded(&root).await;
        let (bytes, txid, vout) = own_change(&wallet, &funding).await;
        let kept = row(&wallet, &txid, vout).await;
        assert_eq!((kept.1.as_str(), kept.2), ("P2PKH", 1), "{door}: change");
        assert!(kept.3.is_some() && kept.4.is_some(), "{door}: derived");
        let before = (balance(&wallet).await, counts(&wallet).await);

        let args = args(bytes, insertion(vout, "tokens"));
        let answer = match door {
            "wallet" => at_wallet(&wallet, args).await,
            _ => at_storage(&wallet, args).await,
        };
        let expected = format!(
            "Validation error: The outputs parameter must be output {vout} is wallet-managed \
             change and cannot be reclassified as a basket insertion"
        );
        assert_eq!(
            answer,
            Err((WalletRefusalClass::InvalidParameter, expected)),
            "{door}"
        );
        assert_eq!(row(&wallet, &txid, vout).await, kept, "{door}: the row");
        assert_eq!(
            (balance(&wallet).await, counts(&wallet).await),
            before,
            "{door}: balance, outputs and baskets"
        );
    }
}

/// A received wallet payment, wallet-managed change, named by a basket
/// insertion: refused as the wallet's own change is, the row unchanged.
#[tokio::test]
async fn a_received_payment_is_not_reclassified_as_a_basket_insertion() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let lock = bsv_rs::script::LockingScript::from_binary(&p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = beef_paying(lock);
    let wallet = wallet(&root, &txid).await;
    let paid = at_storage(
        &wallet,
        args(bytes.clone(), common::payment(&sender, PREFIX, SUFFIX)),
    )
    .await;
    assert_eq!(paid, Ok(true));
    let kept = row(&wallet, &txid, 0).await;
    let before = (balance(&wallet).await, counts(&wallet).await);
    let answer = at_storage(&wallet, args(bytes, insertion(0, "tokens"))).await;
    assert_eq!(
        answer.map_err(|(_, text)| text),
        Err(
            "Validation error: The outputs parameter must be output 0 is wallet-managed change \
             and cannot be reclassified as a basket insertion"
                .to_string()
        )
    );
    assert_eq!(row(&wallet, &txid, 0).await, kept);
    assert_eq!((balance(&wallet).await, counts(&wallet).await), before);
}

/// An output already in one application basket, named by a basket
/// insertion into another, at each door: refused as the reference refuses
/// it, the output stays and the other basket is not created. The same
/// basket named again is admitted, the output unmoved.
#[tokio::test]
async fn an_output_in_one_basket_is_not_moved_into_another() {
    for door in ["wallet", "storage"] {
        let (bytes, txid) = beef_paying(other_lock());
        let wallet = wallet(&PrivateKey::random(), &txid).await;
        assert_eq!(
            at_storage(&wallet, args(bytes.clone(), insertion(0, "tokens"))).await,
            Ok(true)
        );
        let kept = row(&wallet, &txid, 0).await;
        let before = counts(&wallet).await;
        let into = |basket: &str| args(bytes.clone(), insertion(0, basket));
        let answer = match door {
            "wallet" => at_wallet(&wallet, into("other")).await,
            _ => at_storage(&wallet, into("other")).await,
        };
        assert_eq!(
            answer,
            Err((
                WalletRefusalClass::InvalidParameter,
                "Validation error: The outputs parameter must be output 0 is already assigned \
                 to a different basket and cannot be reclassified as a basket insertion into \
                 other"
                    .to_string()
            )),
            "{door}"
        );
        assert_eq!(row(&wallet, &txid, 0).await, kept, "{door}: the row");
        assert_eq!(counts(&wallet).await, before, "{door}: no basket created");

        let again = match door {
            "wallet" => at_wallet(&wallet, into("tokens")).await,
            _ => at_storage(&wallet, into("tokens")).await,
        };
        assert_eq!(again, Ok(true), "{door}: the same basket");
        assert_eq!(row(&wallet, &txid, 0).await.0.as_deref(), Some("tokens"));
    }
}

/// The reference's legacy recovery state: an output in the default basket
/// that is not wallet-managed change, swept into a real basket by a basket
/// insertion, at each door: admitted, no satoshis counted, the row custom
/// in the real basket.
#[tokio::test]
async fn an_output_in_the_default_basket_that_is_not_change_is_swept_into_a_basket() {
    for door in ["wallet", "storage"] {
        let (bytes, txid) = beef_paying(other_lock());
        let wallet = wallet(&PrivateKey::random(), &txid).await;
        assert_eq!(
            at_storage(&wallet, args(bytes.clone(), insertion(0, "staging"))).await,
            Ok(true)
        );
        // The state rows written before the default basket was refused.
        sqlx::query(
            "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets \
             WHERE name = 'default') WHERE txid = ?",
        )
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
        assert_eq!(row(&wallet, &txid, 0).await.0.as_deref(), Some("default"));

        let sweep = args(bytes, insertion(0, "recovered"));
        if door == "wallet" {
            assert_eq!(at_wallet(&wallet, sweep).await, Ok(true));
        } else {
            let auth = storage_auth(&wallet).await;
            let swept = wallet.storage().internalize_action(&auth, sweep).await;
            let swept = swept.expect("the sweep is admitted");
            assert_eq!((swept.is_merge, swept.satoshis), (true, 0));
        }
        let (basket, kind, change, ..) = row(&wallet, &txid, 0).await;
        assert_eq!(
            (basket.as_deref(), kind.as_str(), change),
            (Some("recovered"), "custom", 0),
            "{door}"
        );
    }
}
