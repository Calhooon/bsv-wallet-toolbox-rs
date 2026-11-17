//! What a host holds from before the basket insertion rules, at the wallet
//! door and the storage reader: a custom output an earlier basket insertion
//! put in the default basket, a change output or a received payment an
//! earlier basket insertion reclassified, a spent output an earlier wallet
//! payment merge made spendable, and the custom output `createAction` puts
//! in the default basket. Real SQLite storage, a mock broadcaster and
//! tracker; nothing touches the network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/insertion.rs"]
mod insertion;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, ListOutputsArgs,
    RelinquishOutputArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::storage::FindOutputsArgs;
use bsv_wallet_toolbox_rs::WalletStorageReader;
use serde_json::{json, Value};

use common::{args, beef_paying, other_lock};
use insertion::*;

/// `listOutputs` on `basket`, every include on, each entry as JSON.
async fn listed(wallet: &TestWallet, basket: &str) -> Vec<Value> {
    let args: ListOutputsArgs = serde_json::from_value(json!({
        "basket": basket, "include": "locking scripts", "includeCustomInstructions": true,
        "includeTags": true, "includeLabels": true,
    }))
    .unwrap();
    let listed = wallet.list_outputs(args, Some("test.local")).await.unwrap();
    serde_json::to_value(&listed.outputs)
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

/// A `createAction` of one 1000-satoshi output to another key that needs
/// change to fund it, not sent: its answer's text.
async fn needs_change(wallet: &TestWallet) -> String {
    let answer = wallet
        .create_action(
            CreateActionArgs {
                description: "a payment that needs change".to_string(),
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
                labels: None,
                options: Some(CreateActionOptions {
                    no_send: Some(true),
                    ..Default::default()
                }),
            },
            Some("test.local"),
        )
        .await;
    match answer {
        Ok(made) => format!("made {:?}", made.txid.map(hex::encode)),
        Err(e) => e.to_string(),
    }
}

/// What a host does about an output an earlier basket insertion put in the
/// default basket: the balance sums it, `createAction` never takes it as
/// change, `listOutputs` on "default" lists it with the fields of the
/// wallet's own change; the storage reader finds it there as `custom`, not
/// change; `relinquishOutput` on "default" takes it out of the basket the
/// balance sums.
#[tokio::test]
async fn a_host_finds_and_relinquishes_a_custom_output_in_the_default_basket() {
    let (bytes, txid) = beef_paying(other_lock());
    let wallet = wallet(&PrivateKey::random(), &txid).await;
    assert_eq!(
        at_storage(&wallet, args(bytes, insertion(0, "staging"))).await,
        Ok(true)
    );
    let default_id: i64 =
        sqlx::query_scalar("SELECT basket_id FROM output_baskets WHERE name = 'default'")
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap();
    sqlx::query("UPDATE outputs SET basket_id = ? WHERE txid = ?")
        .bind(default_id)
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    assert_eq!(balance(&wallet).await, 4500, "the default basket sums it");
    // createAction never takes it as change.
    assert_eq!(
        needs_change(&wallet).await,
        "Insufficient funds: need 1005, have 0"
    );
    // listOutputs on "default" lists it with the fields it lists for the
    // wallet's own change: nothing names its type or change flag.
    let custom = listed(&wallet, "default").await;
    assert_eq!(
        custom,
        vec![json!({
            "labels": [], "lockingScript": hex::encode(other_lock().to_binary()),
            "outpoint": format!("{txid}.0"), "satoshis": 4500, "spendable": true, "tags": [],
        })]
    );
    let (change_wallet, _) = funded(&PrivateKey::random()).await;
    let change = listed(&change_wallet, "default").await;
    let keys = |entry: &Value| {
        entry
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(change.len(), 1);
    assert_eq!(keys(&custom[0]), keys(&change[0]), "the same fields");

    let auth = storage_auth(&wallet).await;
    let found: Vec<String> = wallet
        .storage()
        .find_outputs(
            &auth,
            FindOutputsArgs {
                basket_id: Some(default_id),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|o| o.output_type == "custom" && !o.change)
        .map(|o| format!("{}.{}", o.txid, o.vout))
        .collect();
    assert_eq!(found, vec![format!("{txid}.0")]);

    let relinquished = wallet
        .relinquish_output(
            RelinquishOutputArgs {
                basket: "default".to_string(),
                output: bsv_rs::wallet::Outpoint::from_string(&found[0]).unwrap(),
            },
            Some("test.local"),
        )
        .await
        .unwrap();
    assert!(relinquished.relinquished);
    assert_eq!(
        balance(&wallet).await,
        0,
        "the default basket no longer sums it"
    );
}

/// What a host holds of the wallet's own change an earlier basket insertion
/// reclassified (the row as the base wrote it: in the requested basket,
/// `custom`, `change = 0`, its derivation prefix, suffix and sender NULL,
/// out of the balance): a wallet payment of that output with any other
/// derivation is refused (`Derivation mismatch`), and with the prefix and
/// suffix the change was derived with it restores the row, managed change
/// in the default basket, the balance back to its value.
#[tokio::test]
async fn a_reclassified_change_row_is_restored_only_with_its_own_derivation() {
    let root = PrivateKey::random();
    let (wallet, funding) = funded(&root).await;
    let (bytes, txid, vout) = own_change(&wallet, &funding).await;
    let (_, kind, change, prefix, suffix, _) = row(&wallet, &txid, vout).await;
    assert_eq!((kind.as_str(), change), ("P2PKH", 1));
    let (prefix, suffix) = (prefix.unwrap(), suffix.unwrap());
    let with_change = balance(&wallet).await;

    reclassify(&wallet, &txid, vout).await;
    let reclassified = row(&wallet, &txid, vout).await;
    assert_eq!(
        reclassified,
        (
            Some("tokens".to_string()),
            "custom".to_string(),
            0,
            None,
            None,
            None
        )
    );
    let without = balance(&wallet).await;
    assert!(without < with_change, "{without} < {with_change}");

    let me = root.public_key();
    let paying = |prefix: &str, suffix: &str| {
        let mut payment = common::payment(&me, prefix, suffix);
        payment.output_index = vout;
        args(bytes.clone(), payment)
    };
    let refused = at_wallet(&wallet, paying(PREFIX, SUFFIX))
        .await
        .unwrap_err();
    assert_eq!(refused.0, bsv_rs::WalletRefusalClass::InvalidParameter);
    assert!(
        refused
            .1
            .starts_with(&format!("Derivation mismatch for output {vout}: derived ")),
        "{}",
        refused.1
    );
    assert_eq!(row(&wallet, &txid, vout).await, reclassified);
    assert_eq!(balance(&wallet).await, without);

    assert_eq!(at_wallet(&wallet, paying(&prefix, &suffix)).await, Ok(true));
    assert_eq!(
        row(&wallet, &txid, vout).await,
        (
            Some("default".to_string()),
            "P2PKH".to_string(),
            1,
            Some(prefix),
            Some(suffix),
            Some(me.to_hex())
        )
    );
    assert_eq!(
        origin(&wallet, &txid, vout).await,
        ("storage".to_string(), "receive".to_string())
    );
    assert_eq!(balance(&wallet).await, with_change);
}

/// What a host holds of a spent output an earlier wallet payment merge made
/// spendable (the row as the base wrote it: managed change, `spendable = 1`,
/// `spent_by` naming the transaction that spent it): the storage reader
/// finds it by its `spent_by`, and `relinquishOutput` on "default" takes it
/// out of the balance.
#[tokio::test]
async fn a_host_finds_and_relinquishes_a_spendable_row_whose_spender_is_set() {
    let root = PrivateKey::random();
    let (wallet, _funding) = funded(&root).await;
    let auth = storage_auth(&wallet).await;
    let listed_default = wallet
        .storage()
        .find_outputs(&auth, FindOutputsArgs::default())
        .await
        .unwrap();
    let held = listed_default[0].clone();
    let txid = held.txid.clone();
    assert_eq!(balance(&wallet).await, 4500);

    // A spender row, and the output marked spent by it yet spendable.
    let spender = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, \
         description, txid) VALUES (?, 'completed', 'a-spender', 1, 0, 'a spender', ?)",
    )
    .bind(auth.user_id.unwrap())
    .bind("cd".repeat(32))
    .execute(wallet.storage().writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query("UPDATE outputs SET spendable = 1, spent_by = ? WHERE output_id = ?")
        .bind(spender)
        .bind(held.output_id)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    let counted = balance(&wallet).await;

    let found: Vec<(String, i64)> = sqlx::query_as(
        "SELECT txid, vout FROM outputs WHERE spendable = 1 AND spent_by IS NOT NULL",
    )
    .fetch_all(wallet.storage().writer())
    .await
    .unwrap();
    assert_eq!(found, vec![(txid.clone(), i64::from(held.vout))]);

    let relinquish = RelinquishOutputArgs {
        basket: "default".to_string(),
        output: bsv_rs::wallet::Outpoint::from_string(&format!("{txid}.{}", held.vout)).unwrap(),
    };
    wallet
        .relinquish_output(relinquish, Some("test.local"))
        .await
        .unwrap();
    assert_eq!(counted, 4500);
    assert_eq!(balance(&wallet).await, 0);
}

/// A custom output also reaches the default basket through `createAction`
/// with an output whose basket is `default`: this line admits it and stores
/// the output in the default basket as `custom`, `change = 0`.
#[tokio::test]
async fn create_action_with_an_output_into_the_default_basket() {
    let root = PrivateKey::random();
    let (wallet, _funding) = funded(&root).await;
    let answer = wallet
        .create_action(
            CreateActionArgs {
                description: "an output into default".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: other_lock().to_binary(),
                    satoshis: 1_000,
                    output_description: "a kept output".to_string(),
                    basket: Some("default".to_string()),
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
        .await;
    let rows: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT o.type, o.change, b.name FROM outputs o LEFT JOIN output_baskets b \
         ON b.basket_id = o.basket_id WHERE o.satoshis = 1000",
    )
    .fetch_all(wallet.storage().writer())
    .await
    .unwrap();
    assert!(answer.unwrap().txid.is_some());
    assert_eq!(
        rows,
        vec![("custom".to_string(), 0, Some("default".to_string()))]
    );
}
