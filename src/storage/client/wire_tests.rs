//! Reference response names, through the client's actual deserializer.

use super::{StorageClient, ValidCreateActionArgs};
use crate::storage::traits::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOutput, ProtoWallet};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

#[test]
fn create_input_beef_has_the_reference_name_only() {
    let args = CreateActionArgs {
        description: "wire scene".into(),
        input_beef: Some(vec![0, 128, 255]),
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let value = serde_json::to_value(ValidCreateActionArgs::from(args)).unwrap();
    assert_eq!(value["inputBEEF"], json!([0, 128, 255]));
    assert!(value.get("inputBeef").is_none());
}

macro_rules! renamed_entity_field {
    ($test:ident, $entity:literal, $old:literal, $new:literal) => {
        #[test]
        fn $test() {
            let mut params = vec![json!({}), json!({$entity: [{$old: "sentinel"}]})];
            super::wire::request_params("processSyncChunk", &mut params);
            let row = &params[1][$entity][0];
            assert_eq!(row[$new], "sentinel");
            assert!(row.get($old).is_none());
        }
    };
}

renamed_entity_field!(
    created_at_uses_reference_name,
    "outputs",
    "createdAt",
    "created_at"
);
renamed_entity_field!(
    updated_at_uses_reference_name,
    "outputs",
    "updatedAt",
    "updated_at"
);
renamed_entity_field!(
    certificate_type_uses_reference_name,
    "certificates",
    "certType",
    "type"
);
renamed_entity_field!(
    transaction_beef_uses_reference_name,
    "transactions",
    "inputBeef",
    "inputBEEF"
);
renamed_entity_field!(
    proof_request_beef_uses_reference_name,
    "provenTxReqs",
    "inputBeef",
    "inputBEEF"
);
renamed_entity_field!(
    output_type_uses_reference_name,
    "outputs",
    "outputType",
    "type"
);
renamed_entity_field!(
    desired_utxos_uses_reference_name,
    "outputBaskets",
    "numberOfDesiredUtxos",
    "numberOfDesiredUTXOs"
);
renamed_entity_field!(
    desired_value_uses_reference_name,
    "outputBaskets",
    "minimumDesiredUtxoValue",
    "minimumDesiredUTXOValue"
);
renamed_entity_field!(
    label_id_uses_reference_name,
    "txLabels",
    "labelId",
    "txLabelId"
);
renamed_entity_field!(
    label_map_id_uses_reference_name,
    "txLabelMaps",
    "labelId",
    "txLabelId"
);
renamed_entity_field!(
    tag_id_uses_reference_name,
    "outputTags",
    "tagId",
    "outputTagId"
);
renamed_entity_field!(
    tag_map_id_uses_reference_name,
    "outputTagMaps",
    "tagId",
    "outputTagId"
);
renamed_entity_field!(
    commission_script_uses_reference_name,
    "commissions",
    "payerLockingScript",
    "lockingScript"
);

#[test]
fn certificate_filters_use_partial_even_without_a_filter() {
    for method in ["findCertificatesAuth", "findOutputBaskets"] {
        for fields in [json!({"userId": 7}), json!({"userId": null})] {
            let mut params = vec![json!({}), fields.clone()];
            super::wire::request_params(method, &mut params);
            assert!(params[1].get("userId").is_none());
            assert!(params[1]["partial"].is_object());
            if fields["userId"].is_null() {
                assert_eq!(params[1]["partial"], json!({}));
            } else {
                assert_eq!(params[1]["partial"]["userId"], 7);
            }
        }
    }
}

#[test]
fn basket_name_filter_is_nested() {
    let mut params = vec![json!({}), json!({"name": "basket"})];
    super::wire::request_params("findOutputBaskets", &mut params);
    assert_eq!(params[1]["partial"]["name"], "basket");
    assert!(params[1].get("name").is_none());
}

#[test]
fn transaction_status_spellings_are_the_references() {
    let mut params = vec![
        json!({}),
        json!({"txStatus": ["noSend", "nonFinal", "completed"]}),
    ];
    super::wire::request_params("findOutputsAuth", &mut params);
    assert_eq!(
        params[1]["txStatus"],
        json!(["nosend", "nonfinal", "completed"])
    );
    let mut params = vec![
        json!({}),
        json!({"transactions": [{"status": "noSend"}, {"status": "nonFinal"}]}),
    ];
    super::wire::request_params("processSyncChunk", &mut params);
    assert_eq!(params[1]["transactions"][0]["status"], "nosend");
    assert_eq!(params[1]["transactions"][1]["status"], "nonfinal");
}

#[test]
fn wire_adapters_do_not_rename_application_data_or_embedded_sdk_types() {
    for method in [
        "processAction",
        "createAction",
        "internalizeAction",
        "listCertificates",
    ] {
        let original = vec![
            json!({}),
            json!({"type": "application", "createdAt": "opaque",
            "options": {"returnTxidOnly": true, "knownTxids": [[1]], "sendWith": [[2]]},
            "tx": "0080ff", "certificateType": "unchanged", "customInstructions": "inputBeef"}),
        ];
        let mut params = original.clone();
        super::wire::request_params(method, &mut params);
        assert_eq!(params, original, "{method}");
    }
}

async fn reply(
    result: Value,
) -> (
    mockito::ServerGuard,
    mockito::Mock,
    StorageClient<ProtoWallet>,
) {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("POST", "/")
        .with_body(json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string())
        .expect(1)
        .create_async()
        .await;
    let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32]).unwrap()));
    let client = StorageClient::new_unauthenticated(wallet, server.url());
    (server, mock, client)
}

fn date() -> DateTime<Utc> {
    "2026-01-02T03:04:05Z".parse().unwrap()
}

fn sync_args() -> RequestSyncChunkArgs {
    RequestSyncChunkArgs {
        from_storage_identity_key: "source".into(),
        to_storage_identity_key: "destination".into(),
        identity_key: "user".into(),
        since: None,
        max_rough_size: 1000,
        max_items: 10,
        offsets: vec![],
    }
}

#[tokio::test]
async fn create_result_accepts_reference_type_and_keeps_legacy_name() {
    for field in ["type", "inputType"] {
        let (_server, mock, client) = reply(json!({"inputs": [{field: "P2PKH"}]})).await;
        let result = client
            .create_action(
                &AuthId::new("user"),
                CreateActionArgs {
                    description: "wire scene".into(),
                    input_beef: None,
                    inputs: None,
                    outputs: Some(vec![CreateActionOutput {
                        locking_script: vec![81],
                        satoshis: 1,
                        output_description: "wire output".into(),
                        basket: None,
                        custom_instructions: None,
                        tags: None,
                    }]),
                    lock_time: None,
                    version: None,
                    labels: None,
                    options: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.inputs[0].input_type, "P2PKH", "{field}");
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn outputs_accept_reference_type_and_keep_legacy_name() {
    for field in ["type", "outputType"] {
        let (_server, mock, client) = reply(json!([{field: "P2PKH"}])).await;
        let result = client
            .find_outputs(&AuthId::new("user"), FindOutputsArgs::default())
            .await
            .unwrap();
        assert_eq!(result[0].output_type, "P2PKH", "{field}");
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn proof_requests_accept_reference_beef_and_keep_legacy_name() {
    for field in ["inputBEEF", "inputBeef"] {
        let (_server, mock, client) = reply(json!([{field: [0, 128, 255]}])).await;
        let result = client
            .find_proven_tx_reqs(FindProvenTxReqsArgs::default())
            .await
            .unwrap();
        assert_eq!(
            result[0].input_beef.as_deref(),
            Some([0, 128, 255].as_slice()),
            "{field}"
        );
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn transactions_accept_reference_beef_and_keep_legacy_name() {
    for field in ["inputBEEF", "inputBeef"] {
        let (_server, mock, client) = reply(json!([{field: [0, 128, 255]}])).await;
        let result = client
            .find_transactions(FindTransactionsArgs::default())
            .await
            .unwrap();
        assert_eq!(
            result[0].input_beef.as_deref(),
            Some([0, 128, 255].as_slice()),
            "{field}"
        );
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn sync_accepts_reference_entity_names_and_keeps_legacy_names() {
    for (beef_field, output_field) in [("inputBEEF", "type"), ("inputBeef", "outputType")] {
        let (_server, mock, client) = reply(json!({
            "fromStorageIdentityKey": "source", "toStorageIdentityKey": "destination",
            "userIdentityKey": "user", "transactions": [{beef_field: [0, 128, 255]}],
            "provenTxReqs": [{beef_field: [1, 2, 255]}], "outputs": [{output_field: "P2PKH"}]
        }))
        .await;
        let result = client.get_sync_chunk(sync_args()).await.unwrap();
        assert_eq!(
            result.transactions.unwrap()[0].input_beef.as_deref(),
            Some([0, 128, 255].as_slice()),
            "{beef_field}"
        );
        assert_eq!(
            result.proven_tx_reqs.unwrap()[0].input_beef.as_deref(),
            Some([1, 2, 255].as_slice()),
            "{beef_field}"
        );
        assert_eq!(
            result.outputs.unwrap()[0].output_type,
            "P2PKH",
            "{output_field}"
        );
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn sync_state_accepts_reference_when_and_keeps_legacy_name() {
    for field in ["when", "whenLastSyncStarted"] {
        let (_server, mock, client) = reply(json!({"isNew": false, "syncState": {
            "syncStateId": 1, "userId": 7, "storageIdentityKey": "source", "storageName": "wire store",
            "status": "success", "init": false, "refNum": "cmVm", "syncMap": "{}",
            field: date(), "created_at": date(), "updated_at": date()
        }})).await;
        let (state, is_new) = client
            .find_or_insert_sync_state(&AuthId::new("user"), "source", "wire store")
            .await
            .unwrap();
        assert!(!is_new);
        assert_eq!(state.when_last_sync_started, Some(date()), "{field}");
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn sync_result_accepts_reference_max_updated_at_and_keeps_legacy_name() {
    for field in ["maxUpdated_at", "maxUpdatedAt"] {
        let (_server, mock, client) = reply(json!({
            "done": true, field: date(), "updates": 2, "inserts": 3
        }))
        .await;
        let result = client
            .process_sync_chunk(sync_args(), SyncChunk::default())
            .await
            .unwrap();
        assert!(result.done);
        assert_eq!(result.max_updated_at, Some(date()), "{field}");
        assert_eq!((result.updates, result.inserts), (2, 3));
        mock.assert_async().await;
    }
}
