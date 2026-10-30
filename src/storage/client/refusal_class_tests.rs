//! #436: a server refusal reaches a host behind `StorageClient` with the
//! class the local door answers. The reference's server puts
//! `WalletError.unknownToJson`'s object as the JSON-RPC `error` itself
//! (`StorageServer.ts:857-867` at ts-stack edf6e03) and its client rebuilds
//! the class by `name` (`StorageClient.ts:178-181`, `WalletErrorFromJson.ts`).
//! Each test answers that shape from a loopback mockito server.

use super::StorageClient;
use crate::storage::traits::{AuthId, WalletStorageWriter};
use crate::Error;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    ProtoWallet, RelinquishCertificateArgs, ReviewActionResult, ReviewActionResultStatus,
    ReviewActionsRefusal, SendWithResult, SendWithResultStatus,
};
use bsv_rs::WalletRefusalClass;
use serde_json::{json, Value};

const IDENTITY: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn args() -> RelinquishCertificateArgs {
    RelinquishCertificateArgs {
        certificate_type: "dHlwZQ==".to_string(),
        serial_number: "c2VyaWFs".to_string(),
        certifier: IDENTITY.to_string(),
    }
}

/// The reference's JSON-RPC answer: the error object is the WalletError JSON.
fn answer(error: Value) -> String {
    json!({"jsonrpc": "2.0", "error": error, "id": 1}).to_string()
}

/// `relinquishCertificate` against a server answering `error`.
async fn refused_with(error: Value) -> Error {
    let mut server = mockito::Server::new_async().await;
    let _refusal = server
        .mock("POST", "/")
        .with_body(answer(error))
        .create_async()
        .await;
    let key = PrivateKey::from_bytes(&[7u8; 32]).unwrap();
    let client = StorageClient::new_unauthenticated(ProtoWallet::new(Some(key)), server.url());
    client
        .relinquish_certificate(&AuthId::with_user_id(IDENTITY, 1), args())
        .await
        .expect_err("the server refuses")
}

/// The toolbox error carries the class and the server's message byte for
/// byte, and the boundary answers the same refusal the local door would.
fn assert_class(err: Error, class: WalletRefusalClass, message: &str) {
    assert_eq!(err.to_string(), message);
    match &err {
        Error::SdkError(bsv_rs::Error::WalletRefusal {
            class: got,
            message: words,
        }) => {
            assert_eq!(*got, class);
            assert_eq!(words, message);
        }
        other => panic!("expected the {class:?} refusal, got {other:?}"),
    }
    match bsv_rs::Error::from(err) {
        bsv_rs::Error::WalletRefusal {
            class: got,
            message: words,
        } => {
            assert_eq!(got, class);
            assert_eq!(words, message);
        }
        other => panic!("expected the {class:?} refusal at the boundary, got {other:?}"),
    }
}

/// Today's generic text: the reference's fallback for a name outside its
/// whitelist, or a malformed envelope.
fn assert_generic(err: Error, code: i32, message: &str) {
    match &err {
        Error::StorageError(text) => assert_eq!(text, &format!("RPC error {code}: {message}")),
        other => panic!("expected the generic RPC text, got {other:?}"),
    }
    // A Storage fault at the boundary (bsv-rs #439), the former WalletError.
    assert!(matches!(
        bsv_rs::Error::from(err),
        bsv_rs::Error::WalletFault(fault) if fault.family == bsv_rs::WalletFaultFamily::Storage
    ));
}

/// #430: relinquishCertificate's `verifyOne` refusal, in the reference's
/// object exactly: no `code` beside the name (`WalletError.ts:145-149`).
#[tokio::test]
async fn bad_request_keeps_its_class_and_text() {
    let message = "Result must exist and be unique.";
    let err =
        refused_with(json!({"isError": true, "name": "WERR_BAD_REQUEST", "message": message}))
            .await;
    assert_class(err, WalletRefusalClass::BadRequest, message);
}

#[tokio::test]
async fn invalid_parameter_keeps_its_class_and_text() {
    let message = "The type parameter must be valid base64.";
    let err = refused_with(json!({
        "isError": true, "name": "WERR_INVALID_PARAMETER", "message": message, "parameter": "type"
    }))
    .await;
    assert_class(err, WalletRefusalClass::InvalidParameter, message);
}

#[tokio::test]
async fn invalid_operation_keeps_its_class_and_text() {
    let message = "Invalid operation: the action is not abortable";
    let err = refused_with(
        json!({"isError": true, "name": "WERR_INVALID_OPERATION", "message": message}),
    )
    .await;
    assert_class(err, WalletRefusalClass::InvalidOperation, message);
}

/// `WERR_INSUFFICIENT_FUNDS.toJson` adds `code: 7` and its two amounts.
#[tokio::test]
async fn insufficient_funds_keeps_its_class_and_text() {
    let message = "Insufficient funds in the available inputs to cover the cost of the required outputs and the transaction fee (5 more satoshis are needed, for a total of 105), plus whatever would be required in order to pay the fee to unlock and spend the outputs used to provide the additional satoshis.";
    let err = refused_with(json!({
        "isError": true, "name": "WERR_INSUFFICIENT_FUNDS", "message": message,
        "code": 7, "totalSatoshisNeeded": 105, "moreSatoshisNeeded": 5
    }))
    .await;
    assert_class(err, WalletRefusalClass::InsufficientFunds, message);
}

#[tokio::test]
async fn the_other_whitelisted_classes_keep_theirs() {
    for (name, class) in [
        ("WERR_UNAUTHORIZED", WalletRefusalClass::Unauthorized),
        ("WERR_NOT_ACTIVE", WalletRefusalClass::NotActive),
        (
            "WERR_MISSING_PARAMETER",
            WalletRefusalClass::MissingParameter,
        ),
        ("WERR_NOT_IMPLEMENTED", WalletRefusalClass::NotImplemented),
        ("WERR_NETWORK_CHAIN", WalletRefusalClass::NetworkChain),
        (
            "WERR_INVALID_PUBLIC_KEY",
            WalletRefusalClass::InvalidPublicKey,
        ),
        (
            "WERR_BROADCAST_UNAVAILABLE",
            WalletRefusalClass::BroadcastUnavailable,
        ),
        (
            "WERR_ACTION_BATCH_STATE",
            WalletRefusalClass::ActionBatchState,
        ),
        (
            "WERR_UTXO_REVIEW_INCONCLUSIVE",
            WalletRefusalClass::UtxoReviewInconclusive,
        ),
    ] {
        let message = format!("{name} words from the server.");
        let err = refused_with(json!({"isError": true, "name": name, "message": message})).await;
        assert_class(err, class, &message);
    }
}

/// `WERR_REVIEW_ACTIONS.toJson` adds `code: 5` and the results.
#[tokio::test]
async fn review_actions_keeps_its_results() {
    let message = "Undelayed createAction or signAction results require review.";
    let txid = "11".repeat(32);
    let competing = "22".repeat(32);
    let err = refused_with(json!({
        "isError": true, "name": "WERR_REVIEW_ACTIONS", "message": message, "code": 5,
        "reviewActionResults": [
            {"txid": txid, "status": "doubleSpend", "competingTxs": [competing], "competingBeef": [1, 2, 3]}
        ],
        "sendWithResults": [{"txid": txid, "status": "failed"}],
        "txid": txid,
        "noSendChange": [format!("{txid}.0")]
    }))
    .await;
    assert_eq!(err.to_string(), message);
    let expected = ReviewActionsRefusal::with_members(
        vec![ReviewActionResult {
            txid: [0x11; 32],
            status: ReviewActionResultStatus::DoubleSpend,
            competing_txs: Some(vec![[0x22; 32]]),
            competing_beef: Some(vec![1, 2, 3]),
        }],
        vec![SendWithResult {
            txid: [0x11; 32],
            status: SendWithResultStatus::Failed,
        }],
        Some([0x11; 32]),
        None,
        Some(vec![bsv_rs::wallet::Outpoint {
            txid: [0x11; 32],
            vout: 0,
        }]),
    );
    match err {
        Error::ReviewActions(refusal) => assert_eq!(*refusal, expected),
        other => panic!("expected ReviewActions, got {other:?}"),
    }
}

/// The reference's server masks every fault to `WERR_INTERNAL`
/// (`StorageServer.ts:122-127`); the local door's fault is a `WalletError`,
/// so the client keeps the generic text.
#[tokio::test]
async fn internal_keeps_the_generic_text() {
    let message = "An internal error has occurred.";
    let err =
        refused_with(json!({"isError": true, "name": "WERR_INTERNAL", "message": message})).await;
    assert_generic(err, -32000, message);
}

#[tokio::test]
async fn a_name_outside_the_whitelist_keeps_the_generic_text() {
    let message = "words of a class the client does not know";
    let err =
        refused_with(json!({"isError": true, "name": "WERR_SOMETHING_NEW", "message": message}))
            .await;
    assert_generic(err, -32000, message);
}

/// The bounds of `WalletErrorFromJson.ts:42-59`: a name outside the
/// identifier grammar, a control character, a message past 4096.
#[tokio::test]
async fn a_malformed_envelope_keeps_the_generic_text() {
    let long = "x".repeat(4097);
    for (name, message) in [
        ("WERR BAD_REQUEST", "a name with a space"),
        ("WERR_BAD_REQUEST", "a bell \u{7} in the words"),
        ("WERR_BAD_REQUEST", long.as_str()),
    ] {
        let err = refused_with(json!({"isError": true, "name": name, "message": message})).await;
        assert_generic(err, -32000, message);
    }
    // A review refusal whose results are not the reference's shape.
    let message = "Undelayed createAction or signAction results require review.";
    let err = refused_with(json!({
        "isError": true, "name": "WERR_REVIEW_ACTIONS", "message": message, "code": 5,
        "reviewActionResults": [{"txid": "11".repeat(32), "status": "lost"}],
        "sendWithResults": []
    }))
    .await;
    assert_generic(err, 5, message);
}

/// A plain JSON-RPC error, no WalletError envelope: the generic text.
#[tokio::test]
async fn a_plain_rpc_error_keeps_the_generic_text() {
    let err = refused_with(json!({"code": -32601, "message": "Method not found: x"})).await;
    assert_generic(err, -32601, "Method not found: x");
}

/// The error object's `code` beside the name: absent is
/// `bad_request_keeps_its_class_and_text` (`WalletError.toJson`,
/// `WalletError.ts:145-149` at ts-stack edf6e03), numeric here, a string
/// below; the name carries the class.
#[tokio::test]
async fn a_numeric_code_beside_the_name_keeps_its_class() {
    let message = "Invalid operation: the action is not abortable";
    let err = refused_with(json!({
        "isError": true, "name": "WERR_INVALID_OPERATION", "message": message, "code": 400
    }))
    .await;
    assert_class(err, WalletRefusalClass::InvalidOperation, message);
}

#[tokio::test]
async fn a_string_code_keeps_its_class() {
    let message = "Result must exist and be unique.";
    let err = refused_with(json!({"code": "WERR_BAD_REQUEST", "message": message})).await;
    assert_class(err, WalletRefusalClass::BadRequest, message);
    let err = refused_with(json!({
        "isError": true, "name": "WERR_INVALID_OPERATION", "code": "WERR_INVALID_OPERATION",
        "message": message
    }))
    .await;
    assert_class(err, WalletRefusalClass::InvalidOperation, message);
    // Outside the whitelist, the generic text with the default code.
    let err = refused_with(json!({"code": "WERR_SOMETHING_NEW", "message": message})).await;
    assert_generic(err, -32000, message);
}

/// A plain JSON-RPC error with a numeric code and no name keeps its code
/// in the generic text.
#[tokio::test]
async fn a_numeric_code_without_a_name_keeps_the_generic_text() {
    let err = refused_with(json!({"code": -32602, "message": "Invalid params"})).await;
    assert_generic(err, -32602, "Invalid params");
}

/// Through a `Wallet` over `StorageClient`, the host reads the refusal the
/// local door answers for relinquishCertificate (#430).
#[tokio::test]
async fn a_wallet_over_the_client_answers_the_typed_refusal() {
    use crate::services::mock::MockWalletServices;
    use crate::Wallet;
    use bsv_rs::wallet::WalletInterface;
    use mockito::Matcher;

    let key = PrivateKey::from_bytes(&[9u8; 32]).unwrap();
    let identity = key.public_key().to_hex();
    let mut server = mockito::Server::new_async().await;
    let _user = server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "findOrInsertUser"})))
        .with_body(
            json!({"jsonrpc": "2.0", "id": 1, "result": {"user": {
                "userId": 1, "identityKey": identity, "activeStorage": null,
                "createdAt": "2026-10-28T00:00:00Z", "updatedAt": "2026-10-28T00:00:00Z"
            }, "isNew": false}})
            .to_string(),
        )
        .create_async()
        .await;
    let message = "Result must exist and be unique.";
    let _refusal = server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "relinquishCertificate"}),
        ))
        .with_body(
            json!({"jsonrpc": "2.0", "id": 2, "error":
                {"isError": true, "name": "WERR_BAD_REQUEST", "message": message}})
            .to_string(),
        )
        .create_async()
        .await;
    let client =
        StorageClient::new_unauthenticated(ProtoWallet::new(Some(key.clone())), server.url());
    let wallet = Wallet::new(Some(key), client, MockWalletServices::new())
        .await
        .unwrap();
    match wallet
        .relinquish_certificate(args(), Some("example.com"))
        .await
        .unwrap_err()
    {
        bsv_rs::Error::WalletRefusal {
            class,
            message: words,
        } => {
            assert_eq!(class, WalletRefusalClass::BadRequest);
            assert_eq!(words, message);
        }
        other => panic!("expected the BadRequest refusal, got {other:?}"),
    }
}
