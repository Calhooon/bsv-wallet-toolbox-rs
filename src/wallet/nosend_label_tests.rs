//! The BRC-177 `nosend` module at the `WalletInterface` boundary (#312,
//! ruling 19.1 (a')). The reference routes any `p nosend ` label to
//! `createNoSendExpiryAction` (ts-stack@edf6e03 `Wallet.ts:1162-1165`), which
//! refuses on a store without the capability (`createNoSendExpiryAction.ts:201-203`).
//! In the port the storage door's validator refuses it
//! (`create_action/validation.rs`), ahead of any storage write; the wallet
//! hands it to the caller as `WalletError`, the action unwritten.

use super::busy_door_tests::{fund_tagged, own_lock, store_snapshot, wallet};
use super::*;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput};

fn expiry_payment(label: &str) -> CreateActionArgs {
    CreateActionArgs {
        description: "a BRC-177 label test".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac")
                .unwrap(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec![label.to_string()]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn nosend_expiry_label_is_refused_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    fund_tagged(&wallet, &own_lock(&wallet), 0).await;
    let before = store_snapshot(&wallet).await;

    let result = wallet
        .create_action(
            expiry_payment("p nosend expiry seconds 300"),
            "nosend.example.com",
        )
        .await;
    match result {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(
            message,
            "Invalid operation: Active storage does not support BRC-177 noSend expiry"
        ),
        Err(other) => panic!("refused with another error: {other:?}"),
        Ok(_) => panic!("the action was created"),
    }
    assert_eq!(store_snapshot(&wallet).await, before);
}

#[tokio::test]
async fn label_outside_the_prefix_still_creates_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    fund_tagged(&wallet, &own_lock(&wallet), 0).await;

    wallet
        .create_action(expiry_payment("p nosendx"), "nosend.example.com")
        .await
        .unwrap();
}
