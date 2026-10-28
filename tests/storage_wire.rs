//! Recorded JSON-RPC requests from the actual storage client, without signing.
#![cfg(feature = "remote")]

#[path = "fixtures/internalize_order.rs"]
mod internalize_order;

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::*;
use bsv_wallet_toolbox_rs::storage::client::{StorageClient, UpdateProvenTxReqWithNewProvenTxArgs};
use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

fn sample<T: DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}

fn date() -> DateTime<Utc> {
    "2026-01-02T03:04:05Z".parse().unwrap()
}

fn key() -> String {
    format!("02{}", "11".repeat(32))
}

fn txid() -> String {
    "22".repeat(32)
}

fn auth() -> AuthId {
    AuthId {
        identity_key: key(),
        user_id: Some(7),
        is_active: Some(true),
    }
}

fn base() -> FindSincePagedArgs {
    FindSincePagedArgs {
        since: Some(date()),
        paged: Some(Paged {
            offset: Some(2),
            limit: Some(3),
        }),
        order_descending: Some(true),
    }
}

fn certificate() -> TableCertificate {
    sample(json!({
        "certificateId": 4, "userId": 7, "certType": "dHlwZQ==",
        "serialNumber": "c2VyaWFs", "certifier": key(), "subject": key(),
        "verifier": key(), "revocationOutpoint": format!("{}.0", txid()),
        "signature": "3044", "createdAt": date(), "updatedAt": date()
    }))
}

fn certificate_field() -> TableCertificateField {
    sample(json!({
        "certificateFieldId": 5, "certificateId": 4, "userId": 7,
        "fieldName": "name", "fieldValue": "encrypted", "masterKey": "a2V5",
        "createdAt": date(), "updatedAt": date()
    }))
}

fn proven_tx() -> TableProvenTx {
    sample(json!({
        "provenTxId": 8, "txid": txid(), "height": 100, "index": 1,
        "blockHash": "33".repeat(32), "merkleRoot": "44".repeat(32),
        "merklePath": [0, 128, 255], "rawTx": [1, 2, 255],
        "createdAt": date(), "updatedAt": date()
    }))
}

fn sync_args() -> RequestSyncChunkArgs {
    RequestSyncChunkArgs {
        from_storage_identity_key: key(),
        to_storage_identity_key: format!("03{}", "55".repeat(32)),
        identity_key: key(),
        since: Some(date()),
        max_rough_size: 100_000,
        max_items: 25,
        offsets: vec![SyncOffset {
            name: "provenTxs".into(),
            offset: 2,
        }],
    }
}

fn sync_chunk() -> SyncChunk {
    sample(json!({
        "fromStorageIdentityKey": key(),
        "toStorageIdentityKey": format!("03{}", "55".repeat(32)),
        "userIdentityKey": key(),
        "user": {"userId": 7, "identityKey": key(), "activeStorage": key(),
                 "createdAt": date(), "updatedAt": date()},
        "provenTxs": [proven_tx()],
        "provenTxReqs": [{"provenTxReqId": 9, "txid": txid(), "status": "nosend",
            "attempts": 2, "history": "{}", "notified": false, "notify": "{}",
            "rawTx": [0, 128, 255], "inputBeef": [1, 2, 255], "provenTxId": 8,
            "batch": "batch", "createdAt": date(), "updatedAt": date()}],
        "outputBaskets": [{"basketId": 10, "userId": 7, "name": "basket",
            "numberOfDesiredUtxos": 2, "minimumDesiredUtxoValue": 100,
            "createdAt": date(), "updatedAt": date()}],
        "txLabels": [{"labelId": 11, "userId": 7, "label": "label",
            "createdAt": date(), "updatedAt": date()}],
        "outputTags": [{"tagId": 12, "userId": 7, "tag": "tag",
            "createdAt": date(), "updatedAt": date()}],
        "transactions": [{"transactionId": 13, "userId": 7, "txid": txid(),
            "status": "noSend", "reference": "cmVm", "description": "wire scene",
            "satoshis": 100, "version": 1, "lockTime": 0, "rawTx": [0, 128, 255],
            "inputBeef": [1, 2, 255], "isOutgoing": true, "proofTxid": txid(),
            "createdAt": date(), "updatedAt": date()}],
        "txLabelMaps": [{"txLabelMapId": 14, "transactionId": 13, "labelId": 11,
            "createdAt": date(), "updatedAt": date()}],
        "commissions": [{"commissionId": 15, "userId": 7, "transactionId": 13,
            "satoshis": 1, "payerLockingScript": [0, 128, 255], "keyOffset": "a2V5",
            "isRedeemed": false, "createdAt": date(), "updatedAt": date()}],
        "outputs": [{"outputId": 16, "userId": 7, "transactionId": 13,
            "basketId": 10, "txid": txid(), "vout": 0, "satoshis": 100,
            "lockingScript": [81], "scriptLength": 1, "scriptOffset": 0,
            "outputType": "P2PKH", "providedBy": "storage", "purpose": "change",
            "outputDescription": "wire output", "spentBy": 17, "sequenceNumber": 42,
            "spendingDescription": "wire spend", "spendable": true, "change": true,
            "derivationPrefix": "cHJlZml4", "derivationSuffix": "c3VmZml4",
            "senderIdentityKey": key(), "customInstructions": "keep",
            "createdAt": date(), "updatedAt": date()}],
        "outputTagMaps": [{"outputTagMapId": 18, "outputId": 16, "tagId": 12,
            "createdAt": date(), "updatedAt": date()}],
        "certificates": [certificate()], "certificateFields": [certificate_field()]
    }))
}

fn create_args() -> CreateActionArgs {
    CreateActionArgs {
        description: "storage wire scene".into(),
        input_beef: Some(vec![0, 128, 255]),
        inputs: Some(vec![sample(json!({
            "outpoint": format!("{}.1", txid()), "inputDescription": "wire input",
            "unlockingScript": "51", "unlockingScriptLength": 1, "sequenceNumber": 42
        }))]),
        outputs: Some(vec![sample(json!({
            "lockingScript": "51", "satoshis": 100, "outputDescription": "wire output",
            "basket": "basket", "customInstructions": "keep", "tags": ["tag"]
        }))]),
        lock_time: Some(42),
        version: Some(1),
        labels: Some(vec!["  LaBeL ".into()]),
        options: Some(CreateActionOptions {
            sign_and_process: Some(true),
            accept_delayed_broadcast: Some(true),
            trust_self: Some(TrustSelf::Known),
            known_txids: Some(vec![[0x22; 32]]),
            return_txid_only: Some(true),
            no_send: Some(true),
            no_send_change: Some(vec![Outpoint {
                txid: [0x22; 32],
                vout: 1,
            }]),
            send_with: Some(vec![[0x22; 32]]),
            randomize_outputs: Some(false),
        }),
    }
}

async fn witness<F, Fut>(name: &str, call: F)
where
    F: FnOnce(StorageClient<ProtoWallet>) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut server = mockito::Server::new_async().await;
    let captured = Arc::new(Mutex::new(None));
    let received = captured.clone();
    let calls = if matches!(
        name,
        "commitStorageTransaction" | "rollbackStorageTransaction"
    ) {
        2
    } else {
        1
    };
    let mock = server
        .mock("POST", "/")
        .with_body_from_request(move |request| {
            let body: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            let id = body["id"].clone();
            *received.lock().unwrap() = Some(body);
            serde_json::to_vec(&json!({"jsonrpc": "2.0", "id": id, "result": null})).unwrap()
        })
        .expect(calls)
        .create_async()
        .await;
    let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32]).unwrap()));
    call(StorageClient::new_unauthenticated(wallet, server.url())).await;
    mock.assert_async().await;
    let actual = captured.lock().unwrap().take().unwrap();
    let file = format!(
        "{}/tests/fixtures/data/storage-wire/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let expected: Value = serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    assert_eq!(actual, expected, "wire fixture: {name}");
}

macro_rules! wire {
    ($test:ident, $method:literal, $call:expr) => {
        #[tokio::test]
        async fn $test() {
            witness($method, |client| async move {
                let _ = $call(client).await;
            })
            .await;
        }
    };
}

#[test]
fn every_rpc_method_has_a_witness() {
    let source = include_str!("../src/storage/client/storage_client.rs")
        .split("\n#[cfg(test)]")
        .next()
        .unwrap();
    let methods: BTreeSet<_> = source
        .split(".rpc_call")
        .skip(1)
        .filter_map(|tail| {
            let (_, args) = tail.split_once('(')?;
            args.trim_start().strip_prefix('"')?.split('"').next()
        })
        .collect();
    let directory = format!(
        "{}/tests/fixtures/data/storage-wire",
        env!("CARGO_MANIFEST_DIR")
    );
    let witnessed: BTreeSet<String> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let value: Value =
                serde_json::from_str(&std::fs::read_to_string(entry.unwrap().path()).unwrap())
                    .unwrap();
            value["method"].as_str().unwrap().to_owned()
        })
        .collect();
    assert_eq!(methods, witnessed.iter().map(String::as_str).collect());
}

wire!(make_available, "makeAvailable", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.make_available().await
});
wire!(
    migrate,
    "migrate",
    |c: StorageClient<ProtoWallet>| async move { c.migrate("wire store", &key()).await }
);
wire!(
    destroy,
    "destroy",
    |c: StorageClient<ProtoWallet>| async move { c.destroy().await }
);
wire!(
    find_or_insert_user,
    "findOrInsertUser",
    |c: StorageClient<ProtoWallet>| async move { c.find_or_insert_user(&key()).await }
);
wire!(
    find_certificates,
    "findCertificatesAuth",
    |c: StorageClient<ProtoWallet>| async move {
        c.find_certificates(
            &auth(),
            FindCertificatesArgs {
                base: base(),
                user_id: Some(7),
                certifiers: Some(vec![key()]),
                types: Some(vec!["dHlwZQ==".into()]),
                include_fields: Some(true),
            },
        )
        .await
    }
);
wire!(
    find_output_baskets,
    "findOutputBaskets",
    |c: StorageClient<ProtoWallet>| async move {
        c.find_output_baskets(
            &auth(),
            FindOutputBasketsArgs {
                base: base(),
                user_id: Some(7),
                name: Some("basket".into()),
            },
        )
        .await
    }
);
wire!(find_outputs, "findOutputsAuth", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.find_outputs(
        &auth(),
        FindOutputsArgs {
            base: base(),
            user_id: Some(7),
            basket_id: Some(10),
            txid: Some(txid()),
            vout: Some(1),
            no_script: Some(true),
            tx_status: Some(vec![TransactionStatus::NoSend, TransactionStatus::NonFinal]),
        },
    )
    .await
});
wire!(
    find_proven_tx_reqs,
    "findProvenTxReqs",
    |c: StorageClient<ProtoWallet>| async move {
        c.find_proven_tx_reqs(FindProvenTxReqsArgs {
            base: base(),
            status: Some(vec![ProvenTxReqStatus::NoSend]),
            txids: Some(vec![txid()]),
        })
        .await
    }
);
wire!(find_transactions, "findTransactions", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.find_transactions(FindTransactionsArgs {
        base: base(),
        status: Some(vec![TransactionStatus::NonFinal]),
        no_raw_tx: Some(true),
    })
    .await
});
wire!(list_actions, "listActions", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.list_actions(&auth(), sample(json!({"labels": ["  LaBeL "], "labelQueryMode": "all",
        "includeLabels": true, "includeInputs": true, "includeInputSourceLockingScripts": true,
        "includeInputUnlockingScripts": true, "includeOutputs": true, "includeOutputLockingScripts": true,
        "limit": 3, "offset": 2, "seekPermission": false}))).await
});
wire!(list_outputs, "listOutputs", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.list_outputs(
        &auth(),
        sample(
            json!({"basket": "basket", "tags": ["tag"], "tagQueryMode": "all",
        "include": "entire transactions", "includeCustomInstructions": true, "includeTags": true,
        "includeLabels": true, "limit": 3, "offset": -2, "seekPermission": false}),
        ),
    )
    .await
});
wire!(list_certificates, "listCertificates", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.list_certificates(
        &auth(),
        sample(json!({"certifiers": [key()], "types": ["dHlwZQ=="],
        "limit": 3, "offset": 2, "privileged": true, "privilegedReason": "wire scene"})),
    )
    .await
});
wire!(abort_action, "abortAction", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.abort_action(
        &auth(),
        AbortActionArgs {
            reference: "cmVm".into(),
        },
    )
    .await
});
wire!(create_action, "createAction", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.create_action(&auth(), create_args()).await
});
wire!(process_action, "processAction", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.process_action(
        &auth(),
        StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: true,
            is_no_send: true,
            is_delayed: true,
            reference: Some("cmVm".into()),
            txid: Some(txid()),
            raw_tx: Some(vec![0, 128, 255]),
            send_with: vec![txid()],
        },
    )
    .await
});
wire!(
    internalize_action,
    "internalizeAction",
    |c: StorageClient<ProtoWallet>| async move {
        c.internalize_action(&auth(), InternalizeActionArgs { tx: vec![0, 128, 255],
        outputs: vec![sample(json!({"outputIndex": 0, "protocol": "wallet payment",
            "paymentRemittance": {"derivationPrefix": "cHJlZml4", "derivationSuffix": "c3VmZml4", "senderIdentityKey": key()}})),
            sample(json!({"outputIndex": 1, "protocol": "basket insertion",
            "insertionRemittance": {"basket": "basket", "customInstructions": "keep", "tags": ["tag"]}}))],
        description: "wire scene".into(), labels: Some(vec!["  LaBeL ".into()]), seek_permission: Some(false) }).await
    }
);
wire!(
    mark_internalized_tx_failed,
    "markInternalizedTxFailed",
    |c: StorageClient<ProtoWallet>| async move { c.mark_internalized_tx_failed(&txid()).await }
);
wire!(
    insert_certificate,
    "insertCertificateAuth",
    |c: StorageClient<ProtoWallet>| async move { c.insert_certificate(&auth(), certificate()).await }
);
wire!(
    insert_certificate_with_fields,
    "insertCertificateAuth_with_fields",
    |c: StorageClient<ProtoWallet>| async move {
        c.insert_certificate_with_fields(&auth(), certificate(), vec![certificate_field()])
            .await
    }
);
wire!(
    insert_certificate_field,
    "insertCertificateFieldAuth",
    |c: StorageClient<ProtoWallet>| async move {
        c.insert_certificate_field(&auth(), certificate_field())
            .await
    }
);
wire!(
    relinquish_certificate,
    "relinquishCertificate",
    |c: StorageClient<ProtoWallet>| async move {
        c.relinquish_certificate(
            &auth(),
            RelinquishCertificateArgs {
                certificate_type: "dHlwZQ==".into(),
                serial_number: "c2VyaWFs".into(),
                certifier: key(),
            },
        )
        .await
    }
);
wire!(relinquish_output, "relinquishOutput", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.relinquish_output(
        &auth(),
        RelinquishOutputArgs {
            basket: "basket".into(),
            output: Outpoint {
                txid: [0x22; 32],
                vout: 1,
            },
        },
    )
    .await
});
wire!(
    update_transaction_status,
    "updateTransactionStatusAfterBroadcast",
    |c: StorageClient<ProtoWallet>| async move {
        c.update_transaction_status_after_broadcast(
            &txid(),
            &bsv_wallet_toolbox_rs::storage::broadcast::BroadcastOutcome::Success,
        )
        .await
    }
);
wire!(review_status, "reviewStatus", |c: StorageClient<
    ProtoWallet,
>| async move {
    WalletStorageWriter::review_status(&c, &auth(), date()).await
});
wire!(
    purge_data,
    "purgeData",
    |c: StorageClient<ProtoWallet>| async move {
        WalletStorageWriter::purge_data(
            &c,
            &auth(),
            PurgeParams {
                max_age_days: 14,
                purge_completed: true,
                purge_failed: true,
            },
        )
        .await
    }
);
wire!(
    begin_transaction,
    "beginStorageTransaction",
    |c: StorageClient<ProtoWallet>| async move { c.begin_transaction().await }
);
wire!(
    commit_transaction,
    "commitStorageTransaction",
    |c: StorageClient<ProtoWallet>| async move {
        let token = c.begin_transaction().await.unwrap();
        c.commit_transaction(token).await
    }
);
wire!(
    rollback_transaction,
    "rollbackStorageTransaction",
    |c: StorageClient<ProtoWallet>| async move {
        let token = c.begin_transaction().await.unwrap();
        c.rollback_transaction(token).await
    }
);
wire!(
    find_or_insert_sync_state,
    "findOrInsertSyncStateAuth",
    |c: StorageClient<ProtoWallet>| async move {
        c.find_or_insert_sync_state(&auth(), &key(), "wire store")
            .await
    }
);
wire!(
    set_active,
    "setActive",
    |c: StorageClient<ProtoWallet>| async move { c.set_active(&auth(), &key()).await }
);
wire!(get_sync_chunk, "getSyncChunk", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.get_sync_chunk(sync_args()).await
});
wire!(
    process_sync_chunk,
    "processSyncChunk",
    |c: StorageClient<ProtoWallet>| async move { c.process_sync_chunk(sync_args(), sync_chunk()).await }
);
wire!(
    synchronize_transaction_statuses,
    "synchronizeTransactionStatuses",
    |c: StorageClient<ProtoWallet>| async move { c.synchronize_transaction_statuses().await }
);
wire!(
    send_waiting_transactions,
    "sendWaitingTransactions",
    |c: StorageClient<ProtoWallet>| async move {
        c.send_waiting_transactions(Duration::from_millis(1500))
            .await
    }
);
wire!(abort_abandoned, "abortAbandoned", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.abort_abandoned(Duration::from_millis(1500)).await
});
wire!(
    un_fail,
    "unFail",
    |c: StorageClient<ProtoWallet>| async move { c.un_fail().await }
);
wire!(
    monitor_review_status,
    "monitorReviewStatus",
    |c: StorageClient<ProtoWallet>| async move { MonitorStorage::review_status(&c).await }
);
wire!(
    monitor_purge_data,
    "monitorPurgeData",
    |c: StorageClient<ProtoWallet>| async move {
        MonitorStorage::purge_data(
            &c,
            PurgeParams {
                max_age_days: 14,
                purge_completed: true,
                purge_failed: true,
            },
        )
        .await
    }
);
wire!(
    try_acquire_task_lock,
    "tryAcquireTaskLock",
    |c: StorageClient<ProtoWallet>| async move {
        c.try_acquire_task_lock("review", "wire worker", Duration::from_millis(1500))
            .await
    }
);
wire!(release_task_lock, "releaseTaskLock", |c: StorageClient<
    ProtoWallet,
>| async move {
    c.release_task_lock("review", "wire worker").await
});
wire!(
    update_proven_tx_req,
    "updateProvenTxReqWithNewProvenTx",
    |c: StorageClient<ProtoWallet>| async move {
        c.update_proven_tx_req_with_new_proven_tx(UpdateProvenTxReqWithNewProvenTxArgs {
            proven_tx_req_id: 9,
            proven_tx: proven_tx(),
        })
        .await
    }
);
