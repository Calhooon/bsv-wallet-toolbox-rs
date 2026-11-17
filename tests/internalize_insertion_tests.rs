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
#[path = "common/insertion.rs"]
mod insertion;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::WalletStorageWriter;

use common::{args, beef_paying, other_lock, p2pkh_bytes};
use insertion::*;

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
        let before = balance(&wallet).await;
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
        assert_eq!(balance(&wallet).await, before, "{door}: no balance moved");
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

/// An output of the wallet's own `createAction` (`provided_by` `you`,
/// purpose empty) converted to change by a wallet payment merge, then named
/// by a basket insertion, at each door. The merge writes the managed-change
/// fields a new wallet payment row gets (`provided_by` `storage`, purpose
/// `receive`, the port's spelling of the reference's `change`). The same
/// row as a merge before 0.14.0-beta.1 left it (the two fields as they
/// were) holds the derivation the wallet signs with too. Each is refused
/// with the wallet-managed change sentence, the row byte for byte as it
/// was, the balance, outputs and baskets unmoved.
#[tokio::test]
async fn an_output_a_wallet_payment_merge_made_change_is_not_reclassified() {
    for (door, as_before) in [
        ("wallet", false),
        ("storage", false),
        ("wallet", true),
        ("storage", true),
    ] {
        let case = format!("{door} door, as before 0.14.0-beta.1: {as_before}");
        let root = PrivateKey::random();
        let (wallet, funding) = funded(&root).await;
        let me = root.public_key();
        let lock = p2pkh_bytes(&common::derived_key(&root, &me, PREFIX, SUFFIX));
        let (bytes, txid, _, vout) = made(&wallet, &funding, Some(lock)).await;
        let created = row(&wallet, &txid, vout).await;
        assert_eq!((created.2, created.3.as_deref()), (0, None), "{case}");
        assert_eq!(
            origin(&wallet, &txid, vout).await,
            ("you".to_string(), String::new()),
            "{case}: createAction's output"
        );

        let mut merge = common::payment(&me, PREFIX, SUFFIX);
        merge.output_index = vout;
        assert_eq!(
            at_wallet(&wallet, args(bytes.clone(), merge)).await,
            Ok(true)
        );
        let merged = row(&wallet, &txid, vout).await;
        assert_eq!(
            (merged.0.as_deref(), merged.1.as_str(), merged.2),
            (Some("default"), "P2PKH", 1),
            "{case}: merged to change"
        );
        assert_eq!(
            (merged.3.as_deref(), merged.4.as_deref()),
            (Some(PREFIX), Some(SUFFIX)),
            "{case}: the derivation"
        );
        assert_eq!(
            origin(&wallet, &txid, vout).await,
            ("storage".to_string(), "receive".to_string()),
            "{case}: the managed-change fields"
        );
        if as_before {
            sqlx::query(
                "UPDATE outputs SET provided_by = 'you', purpose = '' WHERE txid = ? AND vout = ?",
            )
            .bind(&txid)
            .bind(vout)
            .execute(wallet.storage().writer())
            .await
            .unwrap();
        }
        let kept = every_column(&wallet, &txid, vout).await;
        let before = (balance(&wallet).await, counts(&wallet).await);

        let args = args(bytes, insertion(vout, "tokens"));
        let answer = match door {
            "wallet" => at_wallet(&wallet, args).await,
            _ => at_storage(&wallet, args).await,
        };
        assert_eq!(
            answer.map_err(|(_, text)| text),
            Err(managed_change_refusal(vout)),
            "{case}"
        );
        assert_eq!(
            every_column(&wallet, &txid, vout).await,
            kept,
            "{case}: the row"
        );
        assert_eq!(
            (balance(&wallet).await, counts(&wallet).await),
            before,
            "{case}: balance, outputs and baskets"
        );
    }
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
