//! A wallet payment whose derivation prefix or suffix is empty is refused
//! at the storage door (`WalletStorageWriter::internalize_action`, reached
//! without the wallet) before anything is written, as the wallet door
//! already refuses it; the wallet door's answers are unchanged.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::InternalizeActionArgs;

use common::{internalize_at, paying_derived, payment, stored_outputs};

const EMPTY_DERIVATION: &str =
    "Wallet payment outputs require non-empty derivation_prefix and derivation_suffix";

async fn answer(door: &str, prefix: &str, suffix: &str) -> (Result<bool, String>, i64) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    // The script pays the key derived under the remittance as given, empty
    // part and all.
    let (tx, txid) = paying_derived(&root, &sender, prefix, suffix);
    let wallet = common::wallet("internalize-payment-derivation-tests", &root, &txid).await;
    let args = InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, prefix, suffix, &sender.to_hex())],
        description: "a payment to check".to_string(),
        labels: None,
        seek_permission: None,
    };
    let answer = internalize_at(door, &wallet, &root, args).await;
    (answer, stored_outputs(&wallet).await)
}

#[tokio::test]
async fn an_empty_prefix_or_suffix_is_refused_at_the_storage_door() {
    for (prefix, suffix) in [("", "c3VmZml4"), ("cHJlZml4", "")] {
        let (storage, stored) = answer("storage", prefix, suffix).await;
        println!("storage door, prefix {prefix:?} suffix {suffix:?}: {storage:?}");
        assert_eq!(
            storage,
            Err(format!("Validation error: {EMPTY_DERIVATION}"))
        );
        assert_eq!(stored, 0, "no row is written");

        let (wallet, stored) = answer("wallet", prefix, suffix).await;
        assert_eq!(wallet, Err(format!("wallet error: {EMPTY_DERIVATION}")));
        assert_eq!(stored, 0);
    }
}

#[tokio::test]
async fn a_full_derivation_is_admitted_at_both_doors() {
    for door in ["wallet", "storage"] {
        let (answer, stored) = answer(door, "cHJlZml4", "c3VmZml4").await;
        assert_eq!((answer, stored), (Ok(true), 1));
    }
}

/// A row an earlier release wrote through the storage door: a change row
/// with an empty derivation suffix. The host finds it by an empty prefix or
/// suffix on a change row, and a relinquish of it from the default basket
/// takes it out of the balance and out of coin selection.
#[tokio::test]
async fn a_change_row_with_an_empty_derivation_is_found_and_relinquished() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, "cHJlZml4", "c3VmZml4");
    let wallet = common::wallet("internalize-payment-derivation-tests", &root, &txid).await;
    let args = InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, "cHJlZml4", "c3VmZml4", &sender.to_hex())],
        description: "a payment to check".to_string(),
        labels: None,
        seek_permission: None,
    };
    assert_eq!(
        internalize_at("storage", &wallet, &root, args).await,
        Ok(true)
    );
    let pool = wallet.storage().writer();
    sqlx::query("UPDATE outputs SET derivation_suffix = '' WHERE txid = ?")
        .bind(&txid)
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(wallet.balance().await.unwrap().total, 9_500, "counted");

    let found: Vec<(String, i64)> = sqlx::query_as(
        "SELECT txid, vout FROM outputs WHERE change = 1 \
         AND (derivation_prefix = '' OR derivation_suffix = '')",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(found, vec![(txid.clone(), 0)]);

    let relinquished = bsv_rs::wallet::WalletInterface::relinquish_output(
        &wallet,
        bsv_rs::wallet::RelinquishOutputArgs {
            basket: "default".to_string(),
            output: bsv_rs::wallet::Outpoint::from_string(&format!("{txid}.0")).unwrap(),
        },
        "test.local",
    )
    .await
    .unwrap();
    assert!(relinquished.relinquished);
    assert_eq!(
        wallet.balance().await.unwrap().total,
        0,
        "out of the balance"
    );
}
