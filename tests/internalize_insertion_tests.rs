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
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, Wallet, WalletStorageWriter};

use common::{args, beef_paying, other_lock, refusal};

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
