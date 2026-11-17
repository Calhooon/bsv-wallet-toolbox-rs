//! `internalizeAction`'s basket insertion, at the wallet door
//! (`Wallet::internalize_action`) and at the storage door
//! (`WalletStorageWriter::internalize_action`), as the reference's
//! `internalizeAction` has it (wallet-toolbox `setupBasketInsertionForOutput`,
//! storage `classifyRequestedOutput`, `getBasket` and `validateBasketMerges`,
//! ts-stack 4d0ea63c8):
//!
//! - the default basket, in every spelling the reference's validator trims
//!   and lowercases to `default`, is never the insertion basket;
//! - an output the wallet already holds as its own managed change is never
//!   reclassified as a basket insertion;
//! - an output in an application basket is never moved into another one;
//!   naming its own basket again is admitted;
//! - an output in the default basket that is not managed change may be swept
//!   into a named basket (the reference's legacy-recovery state).
//!
//! The last two tests run what a host does about rows an earlier release
//! wrote: an output inserted into the default basket, and a change row whose
//! derivation a basket insertion cleared.
//!
//! A refusal writes no output row and creates no basket. Real SQLite
//! storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, ListOutputsArgs, Outpoint, RelinquishOutputArgs,
    WalletInterface, WalletPayment,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, Wallet, WalletStorageWriter};

use common::{beef_paying, derived_key, p2pkh_bytes, OTHER_HASH};

const PREFIX: &str = "aW5zZXJ0LXByZWZpeA==";
const SUFFIX: &str = "aW5zZXJ0LXN1ZmZpeA==";
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

/// A wallet payment carrying a stray insertion remittance that names the
/// default basket: the default-basket refusal reads basket insertions alone,
/// so the payment is stored as change in the default basket at both doors,
/// as 0.13.2 stored it.
#[tokio::test]
async fn a_wallet_payment_with_a_stray_insertion_remittance_naming_default_is_stored() {
    for door in ["wallet", "storage"] {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let lock =
            LockingScript::from_binary(&p2pkh_bytes(&derived_key(&root, &sender, PREFIX, SUFFIX)))
                .unwrap();
        let (bytes, txid) = beef_paying(lock);
        let wallet = common::wallet("insertion-stray", &root, &txid).await;
        let output = InternalizeOutput {
            output_index: 0,
            protocol: "wallet payment".to_string(),
            payment_remittance: Some(WalletPayment {
                derivation_prefix: PREFIX.to_string(),
                derivation_suffix: SUFFIX.to_string(),
                sender_identity_key: sender.to_hex(),
            }),
            insertion_remittance: Some(BasketInsertion {
                basket: "default".to_string(),
                custom_instructions: None,
                tags: None,
            }),
        };
        let answer = if door == "wallet" {
            wallet
                .internalize_action(args(bytes, output), "test.local")
                .await
                .map(|r| r.accepted)
                .map_err(|e| e.to_string())
        } else {
            wallet
                .storage()
                .internalize_action(&auth(&wallet, &root).await, args(bytes, output))
                .await
                .map(|r| r.base.accepted)
                .map_err(|e| e.to_string())
        };
        assert_eq!(answer, Ok(true), "{door}");
        let (change, kind, prefix, ..) = row(&wallet, &txid, 0).await;
        let basket: String = sqlx::query_scalar(
            "SELECT b.name FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id \
             WHERE o.txid = ? AND o.vout = 0",
        )
        .bind(&txid)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
        assert_eq!(
            (change, kind.as_str(), prefix.as_deref(), basket.as_str()),
            (1, "P2PKH", Some(PREFIX), "default"),
            "{door}"
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

/// A wallet funded by a payment of 9500 satoshis, then a `createAction`
/// paying 1000 to another key into `basket` (none: no basket), so the action
/// holds the wallet's own change; sent with no broadcast, then marked as a
/// broadcast action is (`unproven`, its change spendable). Returns the
/// wallet, its root, an AtomicBEEF of the action and the action's txid.
async fn spent_wallet(
    name: &str,
    basket: Option<&str>,
    script: Vec<u8>,
) -> (TestWallet, PrivateKey, Vec<u8>, String) {
    spent_wallet_of(PrivateKey::random(), name, basket, script).await
}

/// `spent_wallet` for a given root key.
async fn spent_wallet_of(
    root: PrivateKey,
    name: &str,
    basket: Option<&str>,
    script: Vec<u8>,
) -> (TestWallet, PrivateKey, Vec<u8>, String) {
    let sender = PrivateKey::random().public_key();
    let derived = derived_key(&root, &sender, PREFIX, SUFFIX);
    let lock = LockingScript::from_binary(&p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = beef_paying(lock);
    let wallet = common::wallet(name, &root, &txid).await;
    wallet
        .internalize_action(
            args(
                bytes.clone(),
                InternalizeOutput {
                    output_index: 0,
                    protocol: "wallet payment".to_string(),
                    payment_remittance: Some(WalletPayment {
                        derivation_prefix: PREFIX.to_string(),
                        derivation_suffix: SUFFIX.to_string(),
                        sender_identity_key: sender.to_hex(),
                    }),
                    insertion_remittance: None,
                },
            ),
            "test.local",
        )
        .await
        .expect("the payment funds the wallet");
    let created = wallet
        .create_action(
            CreateActionArgs {
                description: "a payment with change".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: script,
                    satoshis: 1_000,
                    output_description: "a payment".to_string(),
                    basket: basket.map(str::to_string),
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: None,
                options: Some(CreateActionOptions {
                    no_send: Some(true),
                    randomize_outputs: Some(false),
                    ..Default::default()
                }),
            },
            "test.local",
        )
        .await
        .expect("createAction");
    // The action's raw transaction, carried with the payment that funds it
    // as an AtomicBEEF.
    let raw = created.tx.expect("the action's transaction");
    let txid = bsv_rs::transaction::Transaction::from_binary(&raw)
        .expect("the action's transaction")
        .id();
    let mut beef = bsv_rs::transaction::Beef::from_binary(&bytes).expect("the payment's BEEF");
    beef.merge_raw_tx(raw, None);
    let tx = beef.to_binary_atomic(&txid).unwrap();
    // As if the action had been sent: the transaction `unproven` and its
    // change spendable, so the change counts in the balance.
    for sql in [
        "UPDATE transactions SET status = 'unproven' WHERE txid = ?",
        "UPDATE outputs SET spendable = 1 WHERE txid = ? AND change = 1",
    ] {
        sqlx::query(sql)
            .bind(&txid)
            .execute(wallet.storage().writer())
            .await
            .unwrap();
    }
    (wallet, root, tx, txid)
}

type Row = (
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

async fn row(wallet: &TestWallet, txid: &str, vout: u32) -> Row {
    sqlx::query_as(
        "SELECT change, type, derivation_prefix, derivation_suffix, sender_identity_key, basket_id \
         FROM outputs WHERE txid = ? AND vout = ?",
    )
    .bind(txid)
    .bind(vout)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

async fn change_vout(wallet: &TestWallet, txid: &str) -> u32 {
    sqlx::query_scalar("SELECT vout FROM outputs WHERE txid = ? AND change = 1")
        .bind(txid)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// The wallet's own change of an action it made, named by a basket insertion
/// on the merge path: refused with the reference's sentence at both doors;
/// the row keeps `change`, its type and its derivation byte for byte, and the
/// balance does not move.
#[tokio::test]
async fn the_wallets_own_change_is_never_reclassified_as_a_basket_insertion() {
    for door in ["wallet", "storage"] {
        let (wallet, root, tx, txid) = spent_wallet(
            "insertion-change",
            None,
            hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
        )
        .await;
        let vout = change_vout(&wallet, &txid).await;
        let before_row = row(&wallet, &txid, vout).await;
        let before_counts = counts(&wallet).await;
        let before_balance = wallet.balance().await.unwrap().total;
        let sentence = format!(
            "Validation error: The outputs parameter must be output {vout} is wallet-managed change and cannot be reclassified as a basket insertion"
        );
        let refused = if door == "wallet" {
            wallet
                .internalize_action(args(tx, insertion(vout, "tokens")), "test.local")
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        } else {
            wallet
                .storage()
                .internalize_action(
                    &auth(&wallet, &root).await,
                    args(tx, insertion(vout, "tokens")),
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        let expected = if door == "wallet" {
            format!("wallet error: {sentence}")
        } else {
            sentence
        };
        assert_eq!(refused, Err(expected), "{door}");
        let after_row = row(&wallet, &txid, vout).await;
        assert_eq!(after_row, before_row, "{door}: the change row is untouched");
        assert_eq!(after_row.0, 1, "{door}: still change");
        assert_eq!(
            counts(&wallet).await,
            before_counts,
            "{door}: no row, no basket"
        );
        assert_eq!(
            wallet.balance().await.unwrap().total,
            before_balance,
            "{door}: balance"
        );
    }
}

/// An output of the wallet's own `createAction`, then converted to change
/// by a wallet payment merge: the row carries the managed-change columns a
/// new wallet payment row gets (`provided_by` `storage`, `purpose`
/// `receive`), and a basket insertion naming it is refused with the
/// reference's sentence at both doors, the row byte for byte unchanged. The
/// same for a row such a merge wrote before 0.13.3 (its `provided_by` and
/// `purpose` those of the `createAction` output): change with a full
/// derivation is managed change whatever those two columns hold.
#[tokio::test]
async fn an_output_a_wallet_payment_merge_made_change_is_never_reclassified() {
    const MERGE_PREFIX: &str = "bWVyZ2UtcHJlZml4";
    const MERGE_SUFFIX: &str = "bWVyZ2Utc3VmZml4";
    for (door, written_before) in [
        ("wallet", false),
        ("storage", false),
        ("wallet", true),
        ("storage", true),
    ] {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let script = p2pkh_bytes(&derived_key(&root, &sender, MERGE_PREFIX, MERGE_SUFFIX));
        let (wallet, root, tx, txid) =
            spent_wallet_of(root, "insertion-merged", None, script).await;
        let auth = auth(&wallet, &root).await;
        let payment = InternalizeOutput {
            output_index: 0,
            protocol: "wallet payment".to_string(),
            payment_remittance: Some(WalletPayment {
                derivation_prefix: MERGE_PREFIX.to_string(),
                derivation_suffix: MERGE_SUFFIX.to_string(),
                sender_identity_key: sender.to_hex(),
            }),
            insertion_remittance: None,
        };
        let internalize = |output: InternalizeOutput| {
            let tx = tx.clone();
            let wallet = &wallet;
            let auth = auth.clone();
            async move {
                if door == "wallet" {
                    wallet
                        .internalize_action(args(tx, output), "test.local")
                        .await
                        .map(|r| r.accepted)
                        .map_err(|e| e.to_string())
                } else {
                    wallet
                        .storage()
                        .internalize_action(&auth, args(tx, output))
                        .await
                        .map(|r| r.base.accepted)
                        .map_err(|e| e.to_string())
                }
            }
        };
        assert_eq!(internalize(payment).await, Ok(true), "{door}: the merge");
        let managed: (String, String) =
            sqlx::query_as("SELECT provided_by, purpose FROM outputs WHERE txid = ? AND vout = 0")
                .bind(&txid)
                .fetch_one(wallet.storage().writer())
                .await
                .unwrap();
        if written_before {
            // The row as a merge left it before 0.13.3: the `createAction`
            // output's own `provided_by` and `purpose`.
            sqlx::query(
                "UPDATE outputs SET provided_by = 'you', purpose = '' WHERE txid = ? AND vout = 0",
            )
            .bind(&txid)
            .execute(wallet.storage().writer())
            .await
            .unwrap();
        }
        let door = format!("{door}, written before: {written_before}");
        let before_row = row(&wallet, &txid, 0).await;
        let before_counts = counts(&wallet).await;
        let refused = internalize(insertion(0, "tokens")).await;
        let after_row = row(&wallet, &txid, 0).await;
        let sentence = "Validation error: The outputs parameter must be output 0 is wallet-managed change and cannot be reclassified as a basket insertion";
        let expected = if door.starts_with("wallet") {
            format!("wallet error: {sentence}")
        } else {
            sentence.to_string()
        };
        assert_eq!(
            (refused, after_row),
            (Err(expected), before_row.clone()),
            "{door}: refused, the row untouched"
        );
        assert_eq!(before_row.0, 1, "{door}: change");
        assert_eq!(
            (before_row.2.as_deref(), before_row.3.as_deref()),
            (Some(MERGE_PREFIX), Some(MERGE_SUFFIX)),
            "{door}: the derivation"
        );
        assert_eq!(
            counts(&wallet).await,
            before_counts,
            "{door}: no row, no basket"
        );
        assert_eq!(
            (managed.0.as_str(), managed.1.as_str()),
            ("storage", "receive"),
            "{door}: the merge writes the managed-change columns"
        );
    }
}

/// A wallet payment the wallet internalized, named again by a basket
/// insertion: it is managed change, refused the same way.
#[tokio::test]
async fn a_received_payment_is_never_reclassified_as_a_basket_insertion() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = derived_key(&root, &sender, PREFIX, SUFFIX);
    let lock = LockingScript::from_binary(&p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = beef_paying(lock);
    let wallet = common::wallet("insertion-payment", &root, &txid).await;
    let payment = InternalizeOutput {
        output_index: 0,
        protocol: "wallet payment".to_string(),
        payment_remittance: Some(WalletPayment {
            derivation_prefix: PREFIX.to_string(),
            derivation_suffix: SUFFIX.to_string(),
            sender_identity_key: sender.to_hex(),
        }),
        insertion_remittance: None,
    };
    wallet
        .internalize_action(args(bytes.clone(), payment), "test.local")
        .await
        .expect("the payment");
    let before_row = row(&wallet, &txid, 0).await;
    let before_balance = wallet.balance().await.unwrap().total;
    let answer = wallet
        .internalize_action(args(bytes, insertion(0, "tokens")), "test.local")
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    assert_eq!(
        answer,
        Err("wallet error: Validation error: The outputs parameter must be output 0 is wallet-managed change and cannot be reclassified as a basket insertion".to_string())
    );
    assert_eq!(row(&wallet, &txid, 0).await, before_row);
    assert_eq!(wallet.balance().await.unwrap().total, before_balance);
}

/// The reference's legacy-recovery state: an application output sitting in
/// the default basket without being managed change. A basket insertion
/// sweeps it into a named basket, typed `custom`.
#[tokio::test]
async fn a_custom_output_in_the_default_basket_is_swept_into_a_named_basket() {
    for door in ["wallet", "storage"] {
        let (wallet, root, tx, txid) = spent_wallet(
            "insertion-recovery",
            Some("recovery staging"),
            hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
        )
        .await;
        let pool = wallet.storage().writer();
        // Reproduce the legacy state: the custom row moved into the default
        // basket.
        sqlx::query(
            "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE name = 'default') \
             WHERE txid = ? AND vout = 0",
        )
        .bind(&txid)
        .execute(pool)
        .await
        .unwrap();
        let answer = if door == "wallet" {
            wallet
                .internalize_action(args(tx, insertion(0, "recovered")), "test.local")
                .await
                .map(|r| r.accepted)
                .map_err(|e| e.to_string())
        } else {
            wallet
                .storage()
                .internalize_action(
                    &auth(&wallet, &root).await,
                    args(tx, insertion(0, "recovered")),
                )
                .await
                .map(|r| r.base.accepted)
                .map_err(|e| e.to_string())
        };
        assert_eq!(answer, Ok(true), "{door}");
        let (change, kind, ..) = row(&wallet, &txid, 0).await;
        let basket: String = sqlx::query_scalar(
            "SELECT b.name FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id \
             WHERE o.txid = ? AND o.vout = 0",
        )
        .bind(&txid)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(
            (change, kind.as_str(), basket.as_str()),
            (0, "custom", "recovered"),
            "{door}"
        );
    }
}

/// An output in an application basket, named by a basket insertion into
/// another basket: refused, and the other basket is not created. Naming its
/// own basket again is admitted and leaves it there.
#[tokio::test]
async fn an_output_in_an_application_basket_is_never_moved_into_another() {
    for door in ["wallet", "storage"] {
        let (wallet, root, tx, txid) = spent_wallet(
            "insertion-move",
            Some("staging"),
            hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
        )
        .await;
        let before_row = row(&wallet, &txid, 0).await;
        let before_counts = counts(&wallet).await;
        let auth = auth(&wallet, &root).await;
        let internalize = |basket: &'static str| {
            let tx = tx.clone();
            let wallet = &wallet;
            let auth = auth.clone();
            async move {
                if door == "wallet" {
                    wallet
                        .internalize_action(args(tx, insertion(0, basket)), "test.local")
                        .await
                        .map(|r| r.accepted)
                        .map_err(|e| e.to_string())
                } else {
                    wallet
                        .storage()
                        .internalize_action(&auth, args(tx, insertion(0, basket)))
                        .await
                        .map(|r| r.base.accepted)
                        .map_err(|e| e.to_string())
                }
            }
        };
        let sentence = "Validation error: The outputs parameter must be output 0 is already assigned to a different basket and cannot be reclassified as a basket insertion into other";
        let expected = if door == "wallet" {
            format!("wallet error: {sentence}")
        } else {
            sentence.to_string()
        };
        assert_eq!(internalize("other").await, Err(expected), "{door}");
        assert_eq!(
            row(&wallet, &txid, 0).await,
            before_row,
            "{door}: untouched"
        );
        assert_eq!(
            counts(&wallet).await,
            before_counts,
            "{door}: no basket created"
        );

        assert_eq!(
            internalize("staging").await,
            Ok(true),
            "{door}: same basket"
        );
        assert_eq!(
            row(&wallet, &txid, 0).await.5,
            before_row.5,
            "{door}: still in staging"
        );
    }
}

fn default_basket() -> ListOutputsArgs {
    ListOutputsArgs {
        basket: "default".to_string(),
        tags: None,
        tag_query_mode: None,
        include: None,
        include_custom_instructions: None,
        include_tags: None,
        include_labels: None,
        limit: None,
        offset: None,
        seek_permission: None,
    }
}

async fn listed_in_default(wallet: &TestWallet) -> Vec<String> {
    wallet
        .list_outputs(default_basket(), "test.local")
        .await
        .expect("listOutputs")
        .outputs
        .iter()
        .map(|o| o.outpoint.to_string())
        .collect()
}

/// A wallet holding the row an earlier release wrote for a basket insertion
/// naming `default`: `custom`, not change, in the default basket. Returns
/// the wallet, its root, the insertion's AtomicBEEF and the outpoint.
async fn inserted_into_default(name: &str) -> (TestWallet, PrivateKey, Vec<u8>, String) {
    let (wallet, root, bytes) = fresh(name).await;
    wallet
        .internalize_action(args(bytes.clone(), insertion(0, "tokens")), "test.local")
        .await
        .expect("the insertion");
    sqlx::query(
        "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE name = 'default')",
    )
    .execute(wallet.storage().writer())
    .await
    .unwrap();
    let txid: String = sqlx::query_scalar("SELECT txid FROM outputs")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
    (wallet, root, bytes, format!("{txid}.0"))
}

/// The remedy for an output an earlier release inserted into the default
/// basket: `listOutputs` on "default" lists it and the balance counts it,
/// though change selection does not take it;
/// a basket insertion of the same output into a named basket (the
/// reference's legacy-recovery sweep) takes it out, as `relinquishOutput`
/// on "default" does.
#[tokio::test]
async fn an_output_inserted_into_the_default_basket_leaves_it_by_a_sweep_or_a_relinquish() {
    let (wallet, root, bytes, outpoint) = inserted_into_default("remedy-sweep").await;
    assert_eq!(listed_in_default(&wallet).await, vec![outpoint.clone()]);
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
    assert_eq!(
        needs_change(&wallet, &root).await,
        Err("Insufficient funds: need 1005, have 0".to_string()),
        "not selected as change"
    );
    let swept = wallet
        .internalize_action(args(bytes, insertion(0, "tokens")), "test.local")
        .await
        .expect("the sweep");
    assert!(swept.accepted);
    assert!(listed_in_default(&wallet).await.is_empty());
    assert_eq!(wallet.balance().await.unwrap().total, 0);

    let (wallet, _, _, outpoint) = inserted_into_default("remedy-relinquish").await;
    let relinquished = wallet
        .relinquish_output(
            RelinquishOutputArgs {
                basket: "default".to_string(),
                output: Outpoint::from_string(&outpoint).unwrap(),
            },
            "test.local",
        )
        .await
        .expect("relinquishOutput");
    assert!(relinquished.relinquished);
    assert!(listed_in_default(&wallet).await.is_empty());
    assert_eq!(wallet.balance().await.unwrap().total, 0);
}

/// A change row an earlier release reclassified as a basket insertion holds
/// no derivation and is not change: the balance no longer counts it and
/// change selection does not take it.
/// Internalizing the same output again as a wallet payment
/// restores it only with the derivation the row held; any other derivation
/// is refused with the derivation mismatch text.
#[tokio::test]
async fn a_change_row_whose_derivation_was_cleared_returns_only_with_its_derivation() {
    let (wallet, root, tx, txid) = spent_wallet(
        "remedy-change",
        None,
        hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
    )
    .await;
    let vout = change_vout(&wallet, &txid).await;
    let held = row(&wallet, &txid, vout).await;
    let balance = wallet.balance().await.unwrap().total;
    let pool = wallet.storage().writer();
    // The row an earlier release wrote for a basket insertion into "tokens".
    sqlx::query(
        "INSERT INTO output_baskets (user_id, name, created_at, updated_at) \
         SELECT user_id, 'tokens', created_at, updated_at FROM output_baskets WHERE name = 'default'",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE name = 'tokens'), \
         type = 'custom', change = 0, custom_instructions = NULL, derivation_prefix = NULL, \
         derivation_suffix = NULL, sender_identity_key = NULL WHERE txid = ? AND vout = ?",
    )
    .bind(&txid)
    .bind(vout)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(
        wallet.balance().await.unwrap().total,
        balance - held_satoshis(&wallet, &txid, vout).await
    );
    assert_eq!(
        needs_change(&wallet, &root).await,
        Err("Insufficient funds: need 1005, have 0".to_string()),
        "not selected as change"
    );
    let payment = |prefix: &str, suffix: &str| InternalizeOutput {
        output_index: vout,
        protocol: "wallet payment".to_string(),
        payment_remittance: Some(WalletPayment {
            derivation_prefix: prefix.to_string(),
            derivation_suffix: suffix.to_string(),
            sender_identity_key: String::new(),
        }),
        insertion_remittance: None,
    };
    let guessed = wallet
        .internalize_action(args(tx.clone(), payment(PREFIX, SUFFIX)), "test.local")
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    assert!(
        guessed
            .as_ref()
            .is_err_and(|e| e.starts_with("wallet error: Derivation mismatch for output")),
        "{guessed:?}"
    );
    let (prefix, suffix) = (held.2.clone().unwrap(), held.3.clone().unwrap());
    let restored = wallet
        .internalize_action(args(tx, payment(&prefix, &suffix)), "test.local")
        .await
        .expect("the held derivation restores it");
    assert!(restored.accepted);
    let (change, kind, p, s, _, _) = row(&wallet, &txid, vout).await;
    assert_eq!((change, kind.as_str()), (1, "P2PKH"));
    assert_eq!((p, s), (held.2, held.3));
    assert_eq!(wallet.balance().await.unwrap().total, balance);
}

/// A storage `createAction` for 1000 satoshis to another key, which needs
/// change to fund it: `Ok` when change covered it, else the refusal's text.
async fn needs_change(wallet: &TestWallet, root: &PrivateKey) -> Result<(), String> {
    let args = CreateActionArgs {
        description: "a payment funded by change".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(false),
            no_send: Some(true),
            ..Default::default()
        }),
    };
    wallet
        .storage()
        .create_action(&auth(wallet, root).await, args)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn held_satoshis(wallet: &TestWallet, txid: &str, vout: u32) -> u64 {
    let sats: i64 = sqlx::query_scalar("SELECT satoshis FROM outputs WHERE txid = ? AND vout = ?")
        .bind(txid)
        .bind(vout)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
    sats as u64
}
