//! #200: input ownership does not authorize the wallet to sign a named input.
//!
//! BRCs@5119a9b wallet/0100.md:643-649, 724-729 assigns named unlocking
//! scripts to the caller. At ts-stack@edf6e03 (wallet-toolbox/src/signer/methods/),
//! signAction.ts:65-90 calls completeSignedTransaction and verifyUnlockScripts;
//! createAction.ts:181-189 retains the plan from buildSignableTransaction.ts:
//! 176-189 excludes requested inputs from the wallet signing list, and
//! completeSignedTransaction.ts:15-32, 50-79 applies caller scripts separately.
//! verifyUnlockScripts.ts:51-64 refuses an invalid unlock with
//! WERR_INVALID_PARAMETER. Rust maps that argument failure to ValidationError,
//! not InvalidTransactionStatus: the unsigned action remains available to sign.

use super::busy_door_tests::{fund_tagged, own_lock, wallet};
use super::{SignerInput, Wallet};
use crate::services::mock::MockWalletServices;
use crate::StorageSqlx;
use bsv_rs::script::template::{SignOutputs, SigningContext};
use bsv_rs::script::{templates::P2PKH, Script};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput};
use bsv_rs::wallet::{
    Counterparty, CreateActionArgs, CreateActionInput, CreateActionOptions, CreateActionOutput,
    KeyDeriverApi, Outpoint, Protocol, SecurityLevel, SignActionArgs, SignActionSpend,
    SignableTransaction, WalletInterface,
};
use std::collections::HashMap;

const ORIGINATOR: &str = "named-input.test";

struct NamedInputScene {
    _dir: tempfile::TempDir,
    wallet: Wallet<StorageSqlx, MockWalletServices>,
    input: SignerInput,
}

async fn fund_wallet_change(wallet: &Wallet<StorageSqlx, MockWalletServices>, tag: u32) {
    let output_id = fund_tagged(wallet, &own_lock(wallet), tag).await;
    let txid: String = sqlx::query_scalar("SELECT txid FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    // Independent coinbase proofs belong at different heights in one BEEF.
    let height = 100_000 + tag;
    sqlx::query("UPDATE proven_txs SET height = ?, merkle_path = ? WHERE txid = ?")
        .bind(height)
        .bind(MerklePath::from_coinbase_txid(&txid, height).to_binary())
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

fn payment(satoshis: u64) -> CreateActionArgs {
    CreateActionArgs {
        description: "named input signing regression".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x51],
            satoshis,
            output_description: "local test payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            randomize_outputs: Some(false),
            ..Default::default()
        }),
        lock_time: None,
        version: None,
        labels: None,
    }
}

async fn named_input_scene() -> NamedInputScene {
    let (dir, wallet) = wallet().await;
    fund_wallet_change(&wallet, 0).await;
    let made = wallet
        .create_action(payment(1_000), ORIGINATOR)
        .await
        .unwrap();
    let txid = hex::encode(made.txid.unwrap());
    let (output_id, vout, satoshis, lock, prefix, suffix): (
        i64,
        u32,
        i64,
        Vec<u8>,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT output_id, vout, satoshis, locking_script, derivation_prefix, derivation_suffix \
         FROM outputs WHERE txid = ? AND change = 1 ORDER BY satoshis DESC LIMIT 1",
    )
    .bind(&txid)
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap();
    assert!(!prefix.is_empty() && !suffix.is_empty());
    // Storage currently refuses named change. Expose this wallet-made output as
    // a named coin while retaining its real script and derivation metadata.
    sqlx::query("UPDATE outputs SET change = 0 WHERE output_id = ?")
        .bind(output_id)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    NamedInputScene {
        _dir: dir,
        wallet,
        input: SignerInput {
            vin: 0,
            source_txid: txid,
            source_vout: vout,
            satoshis: u64::try_from(satoshis).unwrap(),
            source_locking_script: Some(lock),
            unlocking_script: None,
            derivation_prefix: Some(prefix),
            derivation_suffix: Some(suffix),
            sender_identity_key: None,
        },
    }
}

async fn defer_named(
    scene: &NamedInputScene,
    satoshis: u64,
    script: Option<Vec<u8>>,
) -> SignableTransaction {
    let mut args = payment(satoshis);
    args.options.as_mut().unwrap().sign_and_process = Some(false);
    let unlocking_script_length = script.is_none().then_some(110);
    args.inputs = Some(vec![CreateActionInput {
        outpoint: Outpoint {
            txid: hex::decode(&scene.input.source_txid)
                .unwrap()
                .try_into()
                .unwrap(),
            vout: scene.input.source_vout,
        },
        unlocking_script: script,
        unlocking_script_length,
        input_description: "caller unlocks an owned output".to_string(),
        sequence_number: None,
    }]);
    scene
        .wallet
        .create_action(args, ORIGINATOR)
        .await
        .unwrap()
        .signable_transaction
        .unwrap()
}

fn subject(beef: &[u8]) -> Transaction {
    let beef = Beef::from_binary(beef).unwrap();
    beef.find_txid(beef.atomic_txid.as_deref().unwrap())
        .unwrap()
        .tx()
        .unwrap()
        .clone()
}

fn input_script(tx: &Transaction, vin: usize) -> Vec<u8> {
    tx.inputs[vin]
        .unlocking_script
        .as_ref()
        .unwrap()
        .to_binary()
}

fn caller_script(scene: &NamedInputScene, signable: &SignableTransaction) -> Vec<u8> {
    let raw = subject(&signable.tx).to_binary();
    let mut script = scene
        .wallet
        .signer
        .sign_input(&raw, 0, &scene.input, &scene.wallet.proto_wallet)
        .unwrap();
    // A valid OP_NOP makes the caller's bytes distinguishable from wallet signing.
    script.push(0x61);
    script
}

async fn complete(
    scene: &NamedInputScene,
    signable: SignableTransaction,
    script: Option<Vec<u8>>,
) -> Transaction {
    let spends = script
        .map(|unlocking_script| {
            (
                0,
                SignActionSpend {
                    unlocking_script,
                    sequence_number: None,
                },
            )
        })
        .into_iter()
        .collect();
    let result = scene
        .wallet
        .sign_action(
            SignActionArgs {
                reference: String::from_utf8(signable.reference).unwrap(),
                spends,
                options: None,
            },
            ORIGINATOR,
        )
        .await
        .unwrap();
    assert_eq!(scene.wallet.services().total_calls(), 0);
    subject(&result.tx.unwrap())
}

#[tokio::test]
async fn sign_action_requires_caller_script_for_named_input_with_derivation() {
    let scene = named_input_scene().await;
    let signable = defer_named(&scene, 1_000, None).await;
    let reference = String::from_utf8(signable.reference.clone()).unwrap();
    let result = scene
        .wallet
        .sign_action(
            SignActionArgs {
                reference: reference.clone(),
                spends: HashMap::new(),
                options: None,
            },
            ORIGINATOR,
        )
        .await;
    match result {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(
            message,
            "Validation error: The inputs[0].unlockScript parameter must be valid."
        ),
        Ok(_) => panic!("wallet signed a caller-named input with no caller script"),
        Err(error) => panic!("unexpected refusal: {error}"),
    }
    let status: String = sqlx::query_scalar("SELECT status FROM transactions WHERE reference = ?")
        .bind(&reference)
        .fetch_one(scene.wallet.storage().pool())
        .await
        .unwrap();
    assert_eq!(
        status, "unsigned",
        "the caller can still complete the action"
    );
    let reserved: bool = sqlx::query_scalar(
        "SELECT o.spendable = 0 AND o.spent_by = t.transaction_id \
         FROM outputs o JOIN transactions t ON t.reference = ? \
         WHERE o.txid = ? AND o.vout = ?",
    )
    .bind(&reference)
    .bind(&scene.input.source_txid)
    .bind(scene.input.source_vout)
    .fetch_one(scene.wallet.storage().pool())
    .await
    .unwrap();
    assert!(
        reserved,
        "the named input stays reserved to the same action"
    );
    let script = caller_script(&scene, &signable);
    let signed = complete(&scene, signable, Some(script.clone())).await;
    assert_eq!(input_script(&signed, 0), script);
}

#[tokio::test]
async fn sign_action_preserves_named_script_and_signs_only_allocated_inputs() {
    let scene = named_input_scene().await;
    fund_wallet_change(&scene.wallet, 1).await;
    let signable = defer_named(&scene, 100_000, None).await;
    assert_eq!(subject(&signable.tx).inputs.len(), 2);
    let script = caller_script(&scene, &signable);
    let signed = complete(&scene, signable, Some(script.clone())).await;
    assert_eq!(input_script(&signed, 0), script);
    assert!(!input_script(&signed, 1).is_empty());
}

#[tokio::test]
async fn sign_action_preserves_named_script_supplied_at_create_action() {
    let scene = named_input_scene().await;
    let key_id = format!(
        "{} {}",
        scene.input.derivation_prefix.as_ref().unwrap(),
        scene.input.derivation_suffix.as_ref().unwrap()
    );
    let key = scene
        .wallet
        .proto_wallet
        .key_deriver()
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            &key_id,
            &Counterparty::Self_,
        )
        .unwrap();
    let mut draft = Transaction::new();
    draft.inputs.push(TransactionInput::new(
        scene.input.source_txid.clone(),
        scene.input.source_vout,
    ));
    let raw = draft.to_binary();
    let lock = Script::from_binary(scene.input.source_locking_script.as_ref().unwrap()).unwrap();
    // NONE|ANYONECANPAY signs this input before the wallet chooses outputs.
    let script = P2PKH::unlock(&key, SignOutputs::None, true)
        .sign(&SigningContext::new(&raw, 0, scene.input.satoshis, &lock))
        .unwrap()
        .to_binary();
    let signable = defer_named(&scene, 1_000, Some(script.clone())).await;
    let signed = complete(&scene, signable, None).await;
    assert_eq!(input_script(&signed, 0), script);
}
