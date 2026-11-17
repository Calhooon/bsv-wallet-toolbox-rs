//! `internalizeAction`'s basket insertion, at the wallet door
//! (`Wallet::internalize_action`) and at the storage door
//! (`WalletStorageWriter::internalize_action`), as the reference's
//! `internalizeAction` has it (wallet-toolbox `setupBasketInsertionForOutput`,
//! storage `classifyRequestedOutput`, `getBasket` and `validateBasketMerges`,
//! ts-stack 4d0ea63c8):
//!
//! - the default basket, in every spelling the reference's validator trims
//!   and lowercases to `default`, is never the insertion basket;
//!
//! A refusal writes no output row and creates no basket. Real SQLite
//! storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, Wallet, WalletStorageWriter};

use common::{beef_paying, OTHER_HASH};

const NON_DEFAULT: &str = "The insertionRemittance.basket parameter must be a non-default basket";

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

/// Every spelling the reference's validator trims and lowercases to
/// `default`.
const DEFAULT_SPELLINGS: [&str; 6] = [
    "default",
    "Default",
    "DEFAULT",
    " default ",
    "\tdefault\n",
    "\u{FEFF}default",
];

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

fn args(tx: Vec<u8>, output: InternalizeOutput) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![output],
        description: "an output to insert".to_string(),
        labels: None,
        seek_permission: None,
    }
}

async fn auth(wallet: &TestWallet, root: &PrivateKey) -> AuthId {
    let identity = root.public_key().to_hex();
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&identity)
        .await
        .unwrap();
    AuthId::with_user_id(&identity, user.user_id)
}

/// The store's output and basket counts.
async fn counts(wallet: &TestWallet) -> (i64, i64) {
    let pool = wallet.storage().writer();
    let outputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(pool)
        .await
        .unwrap();
    let baskets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM output_baskets")
        .fetch_one(pool)
        .await
        .unwrap();
    (outputs, baskets)
}

/// A fresh wallet and an AtomicBEEF of one output locked by another key.
async fn fresh(name: &str) -> (TestWallet, PrivateKey, Vec<u8>) {
    let root = PrivateKey::random();
    let lock = LockingScript::from_hex(&format!("76a914{OTHER_HASH}88ac")).unwrap();
    let (bytes, txid) = beef_paying(lock);
    let wallet = common::wallet(name, &root, &txid).await;
    (wallet, root, bytes)
}

/// A basket insertion naming the default basket in any spelling: refused at
/// the wallet door with the reference's sentence; no output row, no basket.
#[tokio::test]
async fn a_basket_insertion_into_the_default_basket_is_refused_at_the_wallet() {
    for spelling in DEFAULT_SPELLINGS {
        let (wallet, _root, bytes) = fresh("insertion-default-wallet").await;
        let before = counts(&wallet).await;
        let answer = wallet
            .internalize_action(args(bytes, insertion(0, spelling)), "test.local")
            .await;
        match answer {
            Err(e @ bsv_rs::Error::WalletError(_)) => {
                assert_eq!(
                    e.to_string(),
                    format!("wallet error: {NON_DEFAULT}"),
                    "{spelling:?}"
                )
            }
            other => panic!("{spelling:?}: not refused: {other:?}"),
        }
        assert_eq!(
            counts(&wallet).await,
            before,
            "{spelling:?}: nothing written"
        );
    }
}

/// The same at the storage door, which a host reaches without the wallet.
#[tokio::test]
async fn a_basket_insertion_into_the_default_basket_is_refused_at_the_storage_door() {
    for spelling in DEFAULT_SPELLINGS {
        let (wallet, root, bytes) = fresh("insertion-default-storage").await;
        let before = counts(&wallet).await;
        let answer = wallet
            .storage()
            .internalize_action(
                &auth(&wallet, &root).await,
                args(bytes, insertion(0, spelling)),
            )
            .await;
        match answer {
            Err(e @ bsv_wallet_toolbox_rs::Error::ValidationError(_)) => assert_eq!(
                e.to_string(),
                format!("Validation error: {NON_DEFAULT}"),
                "{spelling:?}"
            ),
            other => panic!("{spelling:?}: not refused: {:?}", other.map(|r| r.txid)),
        }
        assert_eq!(
            counts(&wallet).await,
            before,
            "{spelling:?}: nothing written"
        );
    }
}

/// A basket insertion into a named basket: stored, at both doors, in a
/// basket of that name.
#[tokio::test]
async fn a_basket_insertion_into_a_named_basket_is_stored() {
    for door in ["wallet", "storage"] {
        let (wallet, root, bytes) = fresh("insertion-named").await;
        let (outputs, _) = counts(&wallet).await;
        let accepted = if door == "wallet" {
            wallet
                .internalize_action(args(bytes, insertion(0, "tokens")), "test.local")
                .await
                .expect("the wallet stores it")
                .accepted
        } else {
            wallet
                .storage()
                .internalize_action(
                    &auth(&wallet, &root).await,
                    args(bytes, insertion(0, "tokens")),
                )
                .await
                .expect("the storage door stores it")
                .base
                .accepted
        };
        assert!(accepted, "{door}");
        assert_eq!(counts(&wallet).await.0, outputs + 1, "{door}: stored");
        let basket: String = sqlx::query_scalar(
            "SELECT b.name FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id",
        )
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
        assert_eq!(basket, "tokens", "{door}");
    }
}
