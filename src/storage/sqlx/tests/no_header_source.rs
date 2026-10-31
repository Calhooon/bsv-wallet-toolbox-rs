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

/// #511: a caller BEEF whose only transaction is proven at `HEIGHT` (no
/// unproven transaction, so the pre-check judges none), the caller's input
/// its `OP_TRUE` coin; and a tracker that holds no root for it, so its
/// root is wrong for the chain, or with `root` the right one.
fn proven_only(root: bool) -> (Transaction, Beef, MockChainTracker) {
    let proven = spend(&"11".repeat(32), "00", 10_000, "51");
    let bump = MerklePath::from_coinbase_txid(&proven.id(), HEIGHT);
    let mut tracker = MockChainTracker::new(HEIGHT + 6);
    if root {
        tracker.add_root(HEIGHT, bump.compute_root(None).unwrap());
    }
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(proven.to_binary(), Some(index));
    (proven, beef, tracker)
}

/// #511: with no tracker the caller's root was not checked at all and the
/// action was created on it; now the call holds with the proofs' text,
/// nothing written.
#[tokio::test]
async fn create_holds_a_caller_root_no_tracker_can_check() {
    for header in [Header::Off, Header::None] {
        let (proven, mut beef, tracker) = proven_only(false);
        let (storage, created) = create_with(header, &proven, &beef.to_binary(), &tracker).await;
        assert_held(header, &storage, created).await;
    }
}

/// Control: the same BEEF with a tracker that holds no root for it is
/// refused with the invalid-root text; with the right root, created.
#[tokio::test]
async fn create_with_a_tracker_refuses_the_wrong_root_and_admits_the_right_one() {
    let (proven, mut beef, tracker) = proven_only(false);
    let (storage, created) =
        create_with(Header::Tracker, &proven, &beef.to_binary(), &tracker).await;
    let error = created.unwrap_err();
    eprintln!("wrong root: {error}");
    assert!(
        matches!(&error, Error::ValidationError(text)
            if text.starts_with("inputBEEF: invalid merkle root ")),
        "{error:?}"
    );
    assert_eq!(action_rows(&storage).await, 0);
    let (proven, mut beef, tracker) = proven_only(true);
    let (_, created) = create_with(Header::Tracker, &proven, &beef.to_binary(), &tracker).await;
    created.unwrap();
}

/// The caller's arguments on `beef`, the caller's input `proven:0`.
async fn create_on(
    storage: &StorageSqlx,
    user_id: i64,
    proven: &Transaction,
    beef: &mut Beef,
) -> crate::Result<()> {
    let args = caller_args(proven, &beef.to_binary(), false);
    create_action_internal(storage, None, user_id, args)
        .await
        .map(|_| ())
}

/// Refused with the invalid-root text, nothing written.
async fn assert_wrong_root(storage: &StorageSqlx, created: crate::Result<()>) {
    let error = created.unwrap_err();
    eprintln!("wrong root: {error}");
    assert!(
        matches!(&error, Error::ValidationError(text)
            if text.starts_with("inputBEEF: invalid merkle root ")
                && text.ends_with(&format!(" at height {HEIGHT}"))),
        "{error:?}"
    );
    assert_eq!(action_rows(storage).await, 0);
}

/// The real `Services` whose header service, on a loopback mock, carries
/// `root` at `HEIGHT`; no tracker on the caller or the storage.
async fn services_only(root: &str) -> (mockito::ServerGuard, StorageSqlx, i64) {
    use crate::services::providers::{ChaintracksServiceClient, FallbackChainTracker};
    let mut server = mockito::Server::new_async().await;
    let header = json!({
        "status": "success",
        "value": {
            "version": 1, "previousHash": "0".repeat(64), "merkleRoot": root,
            "time": 1_700_000_000u32, "bits": 486_604_799u32, "nonce": 1u32,
            "height": HEIGHT, "hash": "b".repeat(64),
        }
    });
    server
        .mock(
            "GET",
            format!("/findHeaderHexForHeight?height={HEIGHT}").as_str(),
        )
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(header.to_string())
        .create_async()
        .await;
    let mut services = crate::services::Services::mainnet().unwrap();
    let primary = ChaintracksServiceClient::from_url(&server.url());
    services.chaintracks = Some(Arc::new(FallbackChainTracker::new(primary)));
    let (storage, user_id) = store().await;
    storage.set_services(Arc::new(services));
    (server, storage, user_id)
}

/// #511: a host with the services' header service only. The caller's root
/// is read against it, as the pre-check reads its tracker: a wrong root is
/// refused with the invalid-root text and the right one created. Before,
/// refused with the proofs' text (no header service configured), and
/// before #511 admitted unchecked.
#[tokio::test]
async fn create_reads_a_caller_root_against_the_services_header_service() {
    let (proven, mut beef, _) = proven_only(false);
    let (_server, storage, user_id) = services_only(&"c".repeat(64)).await;
    let created = create_on(&storage, user_id, &proven, &mut beef).await;
    assert_wrong_root(&storage, created).await;
    let root = beef.bumps[0].compute_root(None).unwrap();
    let (_server, storage, user_id) = services_only(&root).await;
    create_on(&storage, user_id, &proven, &mut beef)
        .await
        .unwrap();
}

/// #511: no caller tracker and the storage's tracker set, no services. The
/// caller's root is read against the storage's tracker: a wrong root is
/// refused and the right one created. Before, admitted unchecked.
#[tokio::test]
async fn create_reads_a_caller_root_against_the_storage_tracker() {
    for root in [false, true] {
        let (proven, mut beef, tracker) = proven_only(root);
        let (storage, user_id) = store().await;
        storage.set_chain_tracker(Arc::new(tracker)).await;
        let created = create_on(&storage, user_id, &proven, &mut beef).await;
        if root {
            created.unwrap();
        } else {
            assert_wrong_root(&storage, created).await;
        }
    }
}
