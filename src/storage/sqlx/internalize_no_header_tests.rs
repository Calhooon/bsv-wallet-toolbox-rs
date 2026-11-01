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
