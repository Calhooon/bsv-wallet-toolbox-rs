//! Wallet validation order over the remote storage boundary (#376).

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionInput, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, Outpoint, ProtoWallet, WalletInterface,
    WalletPayment,
};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageClient, Wallet};
use serde_json::{json, Value};

const SENDER: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

async fn wallet(
    server: &mut mockito::ServerGuard,
) -> Wallet<StorageClient<ProtoWallet>, MockWalletServices> {
    let user = server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(
            json!({"method": "findOrInsertUser"}),
        ))
        .with_body_from_request(|request| {
            let body: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0", "id": body["id"],
                "result": {"isNew": false, "user": {
                    "userId": 1, "identityKey": body["params"][0], "activeStorage": null,
                    "createdAt": "2026-01-02T03:04:05Z", "updatedAt": "2026-01-02T03:04:05Z"
                }}
            }))
            .unwrap()
        })
        .expect(1)
        .create_async()
        .await;
    let root = PrivateKey::from_bytes(&[7; 32]).unwrap();
    let storage =
        StorageClient::new_unauthenticated(ProtoWallet::new(Some(root.clone())), server.url());
    let wallet = Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap();
    user.assert_async().await;
    wallet
}

fn payment() -> InternalizeActionArgs {
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("11".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(20_000),
        locking_script: LockingScript::from_hex("7551").unwrap(),
        change: false,
    });
    let mut child = Transaction::new();
    let mut input = TransactionInput::new(parent.id(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_hex(&format!("76a914{}88ac", "00".repeat(20))).unwrap(),
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent.id(), 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_transaction(child);
    InternalizeActionArgs {
        tx: beef.to_binary_atomic(&txid).unwrap(),
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "wallet payment".into(),
            insertion_remittance: None,
            payment_remittance: Some(WalletPayment {
                derivation_prefix: "cHJlZml4".into(),
                derivation_suffix: "c3VmZml4".into(),
                sender_identity_key: SENDER.into(),
            }),
        }],
        description: "validation order witness".into(),
        labels: None,
        seek_permission: None,
    }
}

#[tokio::test]
async fn internalize_refusal_order_wire_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "data/storage-wire/internalizeAction.validationOrder.json"
    ))
    .unwrap();
    let mut server = mockito::Server::new_async().await;
    let wallet = wallet(&mut server).await;
    let internalize = server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(
            json!({"method": "internalizeAction"}),
        ))
        .expect(0)
        .create_async()
        .await;
    for case in fixture["cases"].as_array().unwrap() {
        let mut args = payment();
        args.labels = Some(vec![
            case["label"].as_str().unwrap().to_string();
            usize::try_from(case["count"].as_u64().unwrap())
                .unwrap()
        ]);
        let error = wallet.internalize_action(args, None).await.unwrap_err();
        match error {
            bsv_rs::Error::WalletRefusal { class, message } => {
                assert_eq!(class, WalletRefusalClass::InvalidParameter);
                let expected = case["message"].as_str().unwrap();
                assert_eq!(message, expected, "{}", case["name"]);
            }
            other => panic!("unexpected refusal: {other:?}"),
        }
    }
    internalize.assert_async().await;
}

fn create_args(labels: Option<Vec<String>>) -> CreateActionArgs {
    CreateActionArgs {
        description: "create validation order witness".into(),
        labels,
        options: Some(CreateActionOptions {
            no_send: Some(true),
            ..Default::default()
        }),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
    }
}

/// A create that storage never sees (#416): the wallet's own validator
/// refuses ahead of any storage call, as ts-stack@edf6e03 Wallet.ts:1154
/// runs validateCreateActionArgs before createAction reaches storage.
async fn refused_before_storage(args: CreateActionArgs) -> (WalletRefusalClass, String) {
    let mut server = mockito::Server::new_async().await;
    let wallet = wallet(&mut server).await;
    let create = server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(
            json!({"method": "createAction"}),
        ))
        .expect(0)
        .create_async()
        .await;
    let error = wallet.create_action(args, None).await.unwrap_err();
    create.assert_async().await;
    match error {
        bsv_rs::Error::WalletRefusal { class, message } => (class, message),
        other => panic!("unexpected refusal: {other:?}"),
    }
}

/// BRC-177 labels are refused by the wallet: the grammar's text for a
/// malformed one (Wallet.ts:1163), the capability's for a well formed one.
#[tokio::test]
async fn create_nosend_labels_refused_before_remote_storage() {
    for (label, class, message) in [
        (
            "p nosend batch",
            WalletRefusalClass::InvalidParameter,
            "Validation error: The labels parameter must be exactly one BRC-177 noSend expiry label",
        ),
        (
            "p nosend expiry seconds 300",
            WalletRefusalClass::InvalidOperation,
            "Invalid operation: Active storage does not support BRC-177 noSend expiry",
        ),
    ] {
        let refusal = refused_before_storage(create_args(Some(vec![label.into()]))).await;
        assert_eq!(refusal, (class, message.to_string()), "{label}");
    }
}

/// The description (validationHelpers.ts:755) answers ahead of an input's
/// unlocking script (758, validateCreateActionInput at 595).
#[tokio::test]
async fn create_description_refused_before_input_script() {
    let mut args = create_args(None);
    args.description = "x".into();
    args.inputs = Some(vec![CreateActionInput {
        outpoint: Outpoint {
            txid: [0x11; 32],
            vout: 0,
        },
        input_description: "create input witness".into(),
        unlocking_script: Some(Vec::new()),
        unlocking_script_length: None,
        sequence_number: None,
    }]);
    let refusal = refused_before_storage(args).await;
    assert_eq!(
        refusal,
        (
            WalletRefusalClass::InvalidParameter,
            "Validation error: The description parameter must be at least 5 length.".to_string()
        )
    );
}

/// An input's unlocking script (validationHelpers.ts:758, 595) answers ahead
/// of an output's locking script (759).
#[tokio::test]
async fn create_input_script_refused_before_output() {
    let mut args = create_args(None);
    args.inputs = Some(vec![CreateActionInput {
        outpoint: Outpoint {
            txid: [0x11; 32],
            vout: 0,
        },
        input_description: "create input witness".into(),
        unlocking_script: Some(Vec::new()),
        unlocking_script_length: None,
        sequence_number: None,
    }]);
    args.outputs = Some(vec![CreateActionOutput {
        locking_script: Vec::new(),
        satoshis: 1_000,
        output_description: "create output witness".into(),
        basket: None,
        custom_instructions: None,
        tags: None,
    }]);
    let refusal = refused_before_storage(args).await;
    assert_eq!(
        refusal,
        (
            WalletRefusalClass::InvalidParameter,
            "Validation error: The unlockingScript parameter must be hexadecimal string."
                .to_string()
        )
    );
}

fn create_input(unlocking_script: Vec<u8>) -> CreateActionInput {
    CreateActionInput {
        outpoint: Outpoint {
            txid: [0x11; 32],
            vout: 0,
        },
        input_description: "create input witness".into(),
        unlocking_script: Some(unlocking_script),
        unlocking_script_length: None,
        sequence_number: None,
    }
}

/// Every input's own checks (validationHelpers.ts:758,
/// `.map(i => validateCreateActionInput(i))`) answer ahead of the duplicate
/// outpoint loop (784): an input 1 that repeats input 0's outpoint does not
/// hide input 2's empty unlocking script.
#[tokio::test]
async fn review_duplicate_cannot_hide_later_empty_script() {
    let mut args = create_args(None);
    let mut later = create_input(Vec::new());
    later.outpoint.vout = 1;
    args.inputs = Some(vec![
        create_input(vec![0x00]),
        create_input(vec![0x00]),
        later,
    ]);
    let refusal = refused_before_storage(args).await;
    assert_eq!(
        refusal,
        (
            WalletRefusalClass::InvalidParameter,
            "Validation error: The unlockingScript parameter must be hexadecimal string."
                .to_string()
        )
    );
}

/// An output's locking script (validationHelpers.ts:759) answers ahead of
/// the duplicate outpoint loop (784).
#[tokio::test]
async fn create_output_refused_before_duplicate_input() {
    let mut args = create_args(None);
    args.inputs = Some(vec![create_input(vec![0x00]), create_input(vec![0x00])]);
    args.outputs = Some(vec![CreateActionOutput {
        locking_script: Vec::new(),
        satoshis: 1_000,
        output_description: "create output witness".into(),
        basket: None,
        custom_instructions: None,
        tags: None,
    }]);
    let refusal = refused_before_storage(args).await;
    assert_eq!(
        refusal,
        (
            WalletRefusalClass::InvalidParameter,
            "Validation error: outputs[0]: locking script cannot be empty".to_string()
        )
    );
}

/// The inputBEEF envelope (validationHelpers.ts:775, parseWalletResultBEEF)
/// answers ahead of the duplicate outpoint loop (784): BEEF V2's version
/// bytes with nothing after them beside an input that repeats another's
/// outpoint answer the envelope's refusal.
#[tokio::test]
async fn review_input_beef_precedes_duplicate() {
    let mut args = create_args(None);
    args.input_beef = Some(vec![0x02, 0x00, 0xbe, 0xef]);
    args.inputs = Some(vec![create_input(vec![0x00]), create_input(vec![0x00])]);
    let refusal = refused_before_storage(args).await;
    assert_eq!(
        refusal,
        (
            WalletRefusalClass::InvalidParameter,
            "Invalid BEEF at byte 4: BadVarint".to_string()
        )
    );
}
