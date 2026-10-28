//! Wallet validation order over the remote storage boundary (#376).

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, InternalizeActionArgs, InternalizeOutput, ProtoWallet,
    WalletInterface, WalletPayment,
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

/// The create validator remains inside sqlx: these requests reach a remote store.
#[tokio::test]
async fn create_nosend_labels_reach_remote_storage() {
    let mut server = mockito::Server::new_async().await;
    let wallet = wallet(&mut server).await;
    let create = server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(
            json!({"method": "createAction"}),
        ))
        .with_body_from_request(|request| {
            let body: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            serde_json::to_vec(&json!({"jsonrpc": "2.0", "id": body["id"],
                "error": {"code": -32602, "message": "create storage tripwire"}}))
            .unwrap()
        })
        .expect(2)
        .create_async()
        .await;
    for label in ["p nosend batch", "p nosend expiry seconds 300"] {
        let error = wallet
            .create_action(
                CreateActionArgs {
                    description: "create validation order witness".into(),
                    labels: Some(vec![label.into()]),
                    options: Some(CreateActionOptions {
                        no_send: Some(true),
                        ..Default::default()
                    }),
                    input_beef: None,
                    inputs: None,
                    outputs: None,
                    lock_time: None,
                    version: None,
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("create storage tripwire"),
            "{error}"
        );
    }
    create.assert_async().await;
}
