//! #509, the keeper's reading of 2026-10-31 (T2): a caller's `inputBEEF` is
//! a BEEF the wallet did not sign, and its merkle proofs are checked
//! against headers, as the reference's `storage/methods/createAction.ts:909`
//! runs `beef.verify(await storage.getServices().getChainTracker(), true)`.
//! With no tracker on the caller or the storage and services built with no
//! header service (the CLI's `CHAINTRACKS_URL=off`), or no services at all,
//! the pre-check holds with the proofs' text, whatever the era, before any
//! write. Before, the hold read `Service error: ChainTracker verification
//! failed: Service error: ChainTracker not configured in Services (no
//! chaintracks_url)`, and with no services `Service error: ChainTracker
//! verification failed: no header service configured`.

use super::*;
use crate::storage::traits::WalletStorageProvider;

const PROOFS_WANT_HEADERS: &str = "Service error: ChainTracker verification failed: \
    the merkle proofs of a BEEF the wallet did not sign need headers, and no header \
    service is configured (no chain tracker, no chaintracks_url)";

/// The header source the door has.
#[derive(Clone, Copy, Debug)]
enum Header {
    /// The caller's tracker, holding the fixture's root.
    Tracker,
    /// No tracker; the real `Services` built with no `chaintracks_url`.
    Off,
    /// No tracker and no services.
    None,
}

/// `create_action_internal` on `beef`, the caller's input `parent:0`,
/// under `header`; `tracker` the caller's when `header` is `Tracker`.
async fn create_with(
    header: Header,
    parent: &Transaction,
    beef: &[u8],
    tracker: &MockChainTracker,
) -> (StorageSqlx, crate::Result<()>) {
    let (storage, user_id) = store().await;
    if let Header::Off = header {
        let services = crate::services::Services::mainnet().unwrap();
        storage.set_services(Arc::new(services));
    }
    let tracker = match header {
        Header::Tracker => Some(tracker as &dyn ChainTracker),
        Header::Off | Header::None => None,
    };
    let args = caller_args(parent, beef, false);
    let created = create_action_internal(&storage, tracker, user_id, args)
        .await
        .map(|_| ());
    (storage, created)
}

async fn action_rows(storage: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

/// Held with the proofs' text, no action written.
async fn assert_held(header: Header, storage: &StorageSqlx, created: crate::Result<()>) {
    let error = created.unwrap_err();
    eprintln!("{header:?}: {error}");
    assert!(matches!(error, Error::ServiceError(_)), "{error:?}");
    assert_eq!(error.to_string(), PROOFS_WANT_HEADERS);
    assert_eq!(action_rows(storage).await, 0, "{header:?}: nothing written");
}

/// The caller's inputBEEF brings an unproven parent over a proven
/// grandparent: with no header source the pre-check holds with the proofs'
/// text, nothing written; with the caller's tracker it is created.
#[tokio::test]
async fn create_pre_check_holds_for_the_proofs_want_of_headers() {
    let p2pkh = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";
    for header in [Header::Off, Header::None] {
        let (parent, mut beef, tracker) = lineage(false, p2pkh);
        let (storage, created) = create_with(header, &parent, &beef.to_binary(), &tracker).await;
        assert_held(header, &storage, created).await;
    }
    let (parent, mut beef, tracker) = lineage(false, p2pkh);
    let (_, created) = create_with(Header::Tracker, &parent, &beef.to_binary(), &tracker).await;
    created.unwrap();
}
