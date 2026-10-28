//! The BRC-177 `nosend` module at the `WalletInterface` boundary (#312,
//! ruling 19.1 (a')). The reference routes any `p nosend ` label to
//! `createNoSendExpiryAction` (ts-stack@edf6e03 `Wallet.ts:1162-1165`), which
//! refuses on a store without the capability (`createNoSendExpiryAction.ts:201-203`).
//! In the port the storage door's validator refuses it
//! (`create_action/validation.rs`), ahead of any storage write; the wallet
//! hands it to the caller as `WalletError`, the action unwritten.

use super::busy_door_tests::{fund_tagged, own_lock, store_snapshot, wallet};
use super::*;
use crate::services::mock::MockWalletServices;
use crate::StorageSqlx;
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

/// `create_action` refuses `label` with `text` and the store is unchanged.
async fn assert_refused(label: &str, text: &str) {
    let (_dir, wallet) = wallet().await;
    fund_tagged(&wallet, &own_lock(&wallet), 0).await;
    let before = store_snapshot(&wallet).await;

    let result = wallet
        .create_action(expiry_payment(label), "nosend.example.com")
        .await;
    match result {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(message, text, "{label}"),
        Err(other) => panic!("{label}: refused with another error: {other:?}"),
        Ok(_) => panic!("{label}: the action was created"),
    }
    assert_eq!(store_snapshot(&wallet).await, before, "{label}");
}

#[tokio::test]
async fn nosend_expiry_label_is_refused_through_wallet_interface() {
    assert_refused(
        "p nosend expiry seconds 300",
        "Invalid operation: Active storage does not support BRC-177 noSend expiry",
    )
    .await;
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
/// `lock` (hex).
fn spend_to(outpoint: String, satoshis: u64, lock: &str) -> bsv_rs::transaction::Transaction {
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(outpoint, 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(satoshis),
        locking_script: LockingScript::from_hex(lock).unwrap(),
        change: false,
    });
    tx
}

/// An `AtomicBEEF` whose subject pays `lock` (hex) and whose parent is
/// proven by a coinbase-shaped BUMP.
fn atomic_beef_paying(lock: &str) -> Vec<u8> {
    use bsv_rs::transaction::{Beef, MerklePath};

    let parent = spend_to("11".repeat(32), 20_000, "7551");
    let parent_txid = parent.id();
    let child = spend_to(parent_txid.clone(), 10_000, lock);
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_transaction(child);
    beef.to_binary_atomic(&txid).unwrap()
}

/// A basket insertion of an output locked by `7551` (`OP_DROP OP_1`),
/// carrying `label`.
fn insertion(label: &str) -> InternalizeActionArgs {
    insertion_labelled(&[label])
}

fn insertion_labelled(labels: &[&str]) -> InternalizeActionArgs {
    use bsv_rs::wallet::{BasketInsertion, InternalizeOutput};

    InternalizeActionArgs {
        tx: atomic_beef_paying("7551"),
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
        labels: Some(labels.iter().map(ToString::to_string).collect()),
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

/// The label grammar answers ahead of the capability refusal (#359,
/// `Wallet.ts:1163`; `brc177NoSendExpiry.ts:96-98`).
#[tokio::test]
async fn nosend_batch_label_is_refused_with_the_grammar_text_through_wallet_interface() {
    assert_refused(
        "p nosend batch",
        "Validation error: The labels parameter must be exactly one BRC-177 noSend expiry label",
    )
    .await;
}

/// `internalize_action` refuses `args` with `text` and the store is unchanged.
async fn assert_internalize_refused(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    args: InternalizeActionArgs,
    text: &str,
) {
    let before = store_snapshot(wallet).await;
    match wallet.internalize_action(args, ORIGIN).await {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(message, text),
        Err(other) => panic!("refused with another error: {other:?}"),
        Ok(_) => panic!("the action was internalized"),
    }
    assert_eq!(store_snapshot(wallet).await, before);
}

/// #365 pair (a): validateLabel (ts-stack@edf6e03 validationHelpers.ts:1054,
/// 385-387, 409-417) answers ahead of the BRC-177 refusal (Wallet.ts:1248-1250).
#[tokio::test]
async fn over_long_label_answers_ahead_of_the_nosend_refusal_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    let long = "a".repeat(301);
    assert_internalize_refused(
        &wallet,
        insertion_labelled(&["p nosend expiry seconds 300", &long]),
        "Validation error: The label parameter must be no more than 300 length.",
    )
    .await;
}

/// #365 pair (b), `docs/ARCHITECTURE.md` D13: the reference refuses the
/// BRC-177 label (Wallet.ts:1248-1250) before its signer finds the
/// derivation locks nothing (`signer/methods/internalizeAction.ts:84-89`,
/// "The paymentRemittance parameter must be locked by script conforming to
/// BRC-29"). The port's wallet checks the derivation ahead of the storage
/// door, where the label checks live, so the mismatch answers ahead of them.
#[tokio::test]
async fn derivation_mismatch_answers_ahead_of_the_nosend_refusal_through_wallet_interface() {
    use bsv_rs::wallet::{
        Counterparty, InternalizeOutput, KeyDeriverApi, Protocol, SecurityLevel, WalletPayment,
    };
    const SENDER: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

    let (_dir, wallet) = wallet().await;
    // The output pays the wallet's key for counterparty self; the payment
    // names another sender, so the derived key locks nothing of the output.
    let lock = own_lock(&wallet);
    let derived = wallet
        .proto_wallet
        .key_deriver()
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            "cHJlZml4 c3VmZml4",
            &Counterparty::Other(PublicKey::from_hex(SENDER).unwrap()),
            true,
        )
        .unwrap();
    let args = InternalizeActionArgs {
        tx: atomic_beef_paying(&hex::encode(&lock)),
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "wallet payment".to_string(),
            payment_remittance: Some(WalletPayment {
                derivation_prefix: "cHJlZml4".to_string(),
                derivation_suffix: "c3VmZml4".to_string(),
                sender_identity_key: SENDER.to_string(),
            }),
            insertion_remittance: None,
        }],
        description: "a BRC-177 label test".to_string(),
        labels: Some(vec!["p nosend expiry seconds 300".to_string()]),
        seek_permission: None,
    };
    let text = format!(
        "Derivation mismatch for output 0: derived {} != script {}",
        hex::encode(derived.hash160()),
        hex::encode(&lock[3..23])
    );
    assert_internalize_refused(&wallet, args, &text).await;
}

/// #380: validateStringLength(args.description, 'description', 5, 2000)
/// (ts-stack@edf6e03 validationHelpers.ts:1053) answers ahead of the labels
/// (1054); the wallet serves the store's text.
#[tokio::test]
async fn short_description_answers_ahead_of_an_over_long_label_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    let long = "a".repeat(301);
    let mut args = insertion_labelled(&[&long]);
    args.description = "abcd".to_string();
    assert_internalize_refused(
        &wallet,
        args,
        "Validation error: The description parameter must be at least 5 length.",
    )
    .await;
}

/// The insertion fixture made a wallet payment, its remittance `prefix`,
/// `suffix` and `sender` as given.
fn payment_of(prefix: &str, suffix: &str, sender: &str) -> InternalizeActionArgs {
    let mut args = insertion_labelled(&[]);
    let output = &mut args.outputs[0];
    output.protocol = "wallet payment".to_string();
    output.insertion_remittance = None;
    output.payment_remittance = Some(bsv_rs::wallet::WalletPayment {
        derivation_prefix: prefix.to_string(),
        derivation_suffix: suffix.to_string(),
        sender_identity_key: sender.to_string(),
    });
    args
}

const PAYMENT_SENDER: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

/// #377: the wallet refuses an empty derivationPrefix or derivationSuffix
/// ahead of the store (D13) in the text of the reference's
/// validateBase64String (ts-stack@edf6e03 validationHelpers.ts:439-441,
/// 490-491, called at 918-919), the prefix ahead of the suffix.
#[tokio::test]
async fn empty_derivation_answers_the_base64_text_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    assert_internalize_refused(
        &wallet,
        payment_of("", "", PAYMENT_SENDER),
        "The derivationPrefix parameter must be valid base64 string",
    )
    .await;
    assert_internalize_refused(
        &wallet,
        payment_of("cHJlZml4", "", PAYMENT_SENDER),
        "The derivationSuffix parameter must be valid base64 string",
    )
    .await;
}

/// #377: on the fixture's `7551` output, which the wallet's derivation check
/// skips (not P2PKH), the door's validateWalletPayment texts are served
/// (validationHelpers.ts:914-923).
#[tokio::test]
async fn door_payment_texts_are_served_through_wallet_interface() {
    let (_dir, wallet) = wallet().await;
    assert_internalize_refused(
        &wallet,
        payment_of("test_prefix", "c3VmZml4", PAYMENT_SENDER),
        "Validation error: The derivationPrefix parameter must be valid base64 string",
    )
    .await;
    assert_internalize_refused(
        &wallet,
        payment_of("cHJlZml4", "sender_key", "sender_key"),
        "Validation error: The derivationSuffix parameter must be valid base64 string",
    )
    .await;
    assert_internalize_refused(
        &wallet,
        payment_of("cHJlZml4", "c3VmZml4", "sender_key"),
        "Validation error: The senderIdentityKey parameter must be hexadecimal string.",
    )
    .await;
}
