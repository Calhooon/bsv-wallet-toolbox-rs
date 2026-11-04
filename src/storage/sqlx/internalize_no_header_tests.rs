//! #509, the keeper's reading of 2026-10-31 (T2): an incoming BEEF's merkle
//! proofs are checked against headers, as the reference's
//! `storage/methods/internalizeAction.ts:456` verifies them through its
//! tracker. With no tracker on the storage and services built with no
//! header service (the CLI's `CHAINTRACKS_URL=off`), or no services at all,
//! the door holds with the proofs' text, whatever the era, before any
//! write. Before, the hold read `Service error: ChainTracker verification
//! failed: Service error: ChainTracker not configured in Services (no
//! chaintracks_url)`, and with no services `Service error: ChainTracker
//! verification failed: no header service configured`.

use super::tests::{
    create_test_atomic_beef, create_test_storage, create_test_user, wallet_payment_args,
};
use super::*;
use crate::storage::traits::WalletStorageProvider;
use bsv_rs::transaction::{Beef, ChainTracker};
use std::sync::Arc;

/// The storage as the scene asks: its tracker kept or cleared, and with
/// `services` the real `Services` built with no `chaintracks_url`.
async fn internalize(
    tracker: bool,
    services: bool,
) -> (StorageSqlx, Result<StorageInternalizeActionResult>) {
    let storage = create_test_storage().await;
    let user_id = create_test_user(&storage).await;
    if !tracker {
        storage.clear_chain_tracker().await;
    }
    if services {
        let services = crate::services::Services::mainnet().unwrap();
        storage.set_services(Arc::new(services));
    }
    let (bytes, _, _) = create_test_atomic_beef();
    let result =
        internalize_action_internal(&storage, user_id, wallet_payment_args(bytes, "headers")).await;
    (storage, result)
}

#[tokio::test]
async fn internalize_holds_for_the_proofs_want_of_headers() {
    // The literal, as the create door's scene (`tests/no_header_source.rs`)
    // pins it: both doors hold with the one text, and a drift in
    // `no_header_source` is a red here too (#515).
    let held = "Service error: ChainTracker verification failed: \
        the merkle proofs of a BEEF the wallet did not sign need headers, and no header \
        service is configured (no chain tracker, no chaintracks_url)";
    for services in [true, false] {
        let (storage, result) = internalize(false, services).await;
        let error = result.unwrap_err();
        eprintln!("services={services}: {error}");
        assert!(matches!(error, Error::ServiceError(_)), "{error:?}");
        assert_eq!(error.to_string(), held);
        let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(actions, 0, "services={services}: nothing written");
    }
}

/// Control: with the storage's tracker the same payment is accepted.
#[tokio::test]
async fn internalize_with_a_tracker_accepts_the_same_payment() {
    let (_, result) = internalize(true, false).await;
    assert!(result.unwrap().base.accepted);
}

// #769, the keeper's reading of 2026-11-03: one witness per cause a
// tracker gives the door, each with the class the host reads and the
// mapping bsv-wallet-cli serves it with (`src/server/error.rs` at
// bsv-wallet-cli fa1e28f: `Kind::answer` :77 Validation 400 and :95 Service
// 502; `of_toolbox` :354-357 and :366; `of_sdk` :282; `of_fault` :295;
// `of_refusal` :130-136). An unknown height is a kept divergence pending the
// owner's word (`docs/DIVERGENCES.md` AD1).

/// A tracker whose root read answers a typed transport fault.
struct TransportFaultTracker;

#[async_trait::async_trait]
impl ChainTracker for TransportFaultTracker {
    async fn is_valid_root_for_height(
        &self,
        _: &str,
        _: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        Err(bsv_rs::transaction::ChainTrackerError::TransportFault {
            fault: bsv_rs::TransportFault::MayHaveBeenReceived,
            status: Some(503),
            detail: "header service unavailable".into(),
        })
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        panic!("the door asks the root, never the height")
    }
}

/// The payment internalized against the real HTTP header client, whose
/// `findHeaderHexForHeight` at the proof's height answers `header` (`null`
/// where the header service does not have the height).
async fn internalize_against_header(
    header: serde_json::Value,
) -> (StorageSqlx, Result<StorageInternalizeActionResult>) {
    let storage = create_test_storage().await;
    let user_id = create_test_user(&storage).await;
    let mut server = crate::services::mock_server().await;
    let (bytes, _, _) = create_test_atomic_beef();
    let height = proof_height(&bytes);
    let route = server
        .mock(
            "GET",
            format!("/findHeaderHexForHeight?height={height}").as_str(),
        )
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::json!({"status": "success", "value": header}).to_string())
        .expect(1)
        .create_async()
        .await;
    let tracker = crate::services::ChaintracksServiceClient::from_url(&server.url());
    storage.set_chain_tracker(Arc::new(tracker)).await;
    let result =
        internalize_action_internal(&storage, user_id, wallet_payment_args(bytes, "headers")).await;
    route.assert_async().await;
    (storage, result)
}

/// The proof's height and root in the fixture's BEEF.
fn proof(bytes: &[u8]) -> (u32, String) {
    let beef = Beef::from_binary(bytes).unwrap();
    let bump = &beef.bumps[0];
    (bump.block_height, bump.compute_root(None).unwrap())
}

fn proof_height(bytes: &[u8]) -> u32 {
    proof(bytes).0
}

/// A header at the proof's height carrying `root`.
fn header_with_root(bytes: &[u8], root: &str) -> serde_json::Value {
    serde_json::json!({
        "version": 1, "height": proof_height(bytes), "previousHash": "00".repeat(32),
        "merkleRoot": root, "time": 1_700_000_000, "bits": 0x1d00_ffff,
        "nonce": 0, "hash": "11".repeat(32)
    })
}

async fn transactions(storage: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

/// A transport fault from the tracker reaches the host as the SDK's typed
/// fault with its class: bsv-wallet-cli's `of_sdk` (:282) serves it as its
/// Service kind, 502 `SERVICE_ERROR`, retryable. Nothing is written.
#[tokio::test]
async fn a_tracker_transport_fault_reaches_the_host_as_its_typed_fault() {
    let storage = create_test_storage().await;
    let user_id = create_test_user(&storage).await;
    storage
        .set_chain_tracker(Arc::new(TransportFaultTracker))
        .await;
    let (bytes, _, _) = create_test_atomic_beef();
    let error = internalize_action_internal(&storage, user_id, wallet_payment_args(bytes, "fault"))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::SdkError(_)), "{error:?}");
    let sdk = bsv_rs::Error::from(error);
    assert!(
        matches!(&sdk, bsv_rs::Error::TransportFault { status: Some(503), detail, .. }
            if detail == "header service unavailable"),
        "{sdk:?}"
    );
    assert_eq!(
        sdk.transport_fault(),
        Some(bsv_rs::TransportFault::MayHaveBeenReceived)
    );
    assert_eq!(transactions(&storage).await, 0);
}

/// A header service without the proof's height (the store one block
/// behind): the reference refuses it as an invalid root
/// (`ChaintracksChainTracker.ts:189`, `MerklePath.ts:499` at ts-stack
/// edf6e03); the port holds, `ServiceError` with the tracker's text, the
/// SDK's `WalletFault` of family `Service`: bsv-wallet-cli's `of_toolbox`
/// (:366) and `of_fault` (:295) serve it as its Service kind, 502
/// `SERVICE_ERROR`, retryable. Nothing is written, and the same payment is
/// admitted on the next try once the header service has the header. The
/// kept divergence: `docs/DIVERGENCES.md` AD1.
#[tokio::test]
async fn an_unknown_proof_height_holds_as_a_service_fault_and_admits_on_retry() {
    let (storage, result) = internalize_against_header(serde_json::Value::Null).await;
    let error = result.unwrap_err();
    assert!(
        matches!(&error, Error::ServiceError(text)
            if text.starts_with("ChainTracker verification failed: ")),
        "{error:?}"
    );
    let sdk = bsv_rs::Error::from(error);
    assert!(
        matches!(&sdk, bsv_rs::Error::WalletFault(fault)
            if fault.family == bsv_rs::WalletFaultFamily::Service),
        "{sdk:?}"
    );
    assert_eq!(transactions(&storage).await, 0);

    let (bytes, _, _) = create_test_atomic_beef();
    let (_, root) = proof(&bytes);
    let (_, retried) = internalize_against_header(header_with_root(&bytes, &root)).await;
    assert!(retried.unwrap().base.accepted);
}

/// A header at the proof's height with another root: the refusal,
/// `InvalidBeef` (the BUMP's root not carried), the SDK's `WalletRefusal`
/// of class `InvalidParameter`: bsv-wallet-cli's `of_toolbox` (:354) and
/// `of_refusal` (:130) serve it as its Validation kind, 400
/// `VALIDATION_ERROR`, as the reference refuses it. Nothing is written.
#[tokio::test]
async fn a_wrong_root_at_a_known_height_is_refused() {
    let (bytes, _, _) = create_test_atomic_beef();
    let (storage, result) =
        internalize_against_header(header_with_root(&bytes, &"22".repeat(32))).await;
    let error = result.unwrap_err();
    assert!(matches!(error, Error::InvalidBeef { .. }), "{error:?}");
    let sdk = bsv_rs::Error::from(error);
    assert!(
        matches!(&sdk, bsv_rs::Error::WalletRefusal { class, .. }
            if *class == bsv_rs::WalletRefusalClass::InvalidParameter),
        "{sdk:?}"
    );
    assert_eq!(transactions(&storage).await, 0);
}
