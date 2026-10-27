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

/// The originator of the internalize fixture.
const ORIGIN: &str = "nosend.example.com";

/// A one-input, one-output transaction spending `outpoint`'s output 0 to
/// `7551` (`OP_DROP OP_1`).
fn spend_to_drop_one(outpoint: String, satoshis: u64) -> bsv_rs::transaction::Transaction {
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(outpoint, 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(satoshis),
        locking_script: LockingScript::from_hex("7551").unwrap(),
        change: false,
    });
    tx
}

/// A basket insertion of an AtomicBEEF whose parent is proven by a
/// coinbase-shaped BUMP, carrying `label`.
fn insertion(label: &str) -> InternalizeActionArgs {
    use bsv_rs::transaction::{Beef, MerklePath};
    use bsv_rs::wallet::{BasketInsertion, InternalizeOutput};

    let parent = spend_to_drop_one("11".repeat(32), 20_000);
    let parent_txid = parent.id();
    let child = spend_to_drop_one(parent_txid.clone(), 10_000);
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_transaction(child);
    InternalizeActionArgs {
        tx: beef.to_binary_atomic(&txid).unwrap(),
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "nosend".to_string(),
                custom_instructions: None,
                tags: None,
            }),
        }],
        description: "a BRC-177 label test".to_string(),
        labels: Some(vec![label.to_string()]),
        seek_permission: None,
    }
}

/// The internalize door (#357): the reference refuses the label on
/// internalizeAction (ts-stack@edf6e03 `Wallet.ts:1248-1250`); the port's
/// storage door refuses it ahead of any write, and the wallet hands the
/// reference's text to the caller as `WalletError`.
#[tokio::test]
async fn nosend_label_is_refused_on_internalize_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    let before = store_snapshot(&wallet).await;

    let result = wallet
        .internalize_action(insertion("p nosend expiry seconds 300"), ORIGIN)
        .await;
    match result {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(
            message,
            "Validation error: The labels parameter must be BRC-177 noSend expiry labels only on outgoing createAction requests"
        ),
        Err(other) => panic!("refused with another error: {other:?}"),
        Ok(_) => panic!("the action was internalized"),
    }
    assert_eq!(store_snapshot(&wallet).await, before);
}
