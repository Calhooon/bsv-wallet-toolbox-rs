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
use crate::services::{
    BlockHeader, FiatCurrency, GetBeefResult, GetMerklePathResult, GetRawTxResult,
    GetStatusForTxidsResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput,
    PostBeefResult, UtxoVerdict,
};
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

/// The fault a host's own services give for their header service: another
/// text than the not-configured one (a transport-class text).
const UNREACHABLE: &str = "the header service is unreachable: connection refused";

/// The hold `header_fault` gives for `UNREACHABLE`.
const UNREACHABLE_HOLD: &str = "Service error: ChainTracker verification failed: \
    Service error: the header service is unreachable: connection refused";

/// A host's `WalletServices` whose `get_chain_tracker` faults with
/// `UNREACHABLE` and counts its asks; every other call is the mock's.
#[derive(Default)]
struct UnreachableHeaders {
    inner: MockWalletServices,
    asks: std::sync::atomic::AtomicUsize,
}

/// The double's answers.
type R<T> = crate::Result<T>;

#[async_trait::async_trait]
impl crate::services::WalletServices for UnreachableHeaders {
    async fn get_chain_tracker(&self) -> R<&dyn ChainTracker> {
        self.asks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Error::ServiceError(UNREACHABLE.to_string()))
    }

    async fn get_height(&self) -> R<u32> {
        self.inner.get_height().await
    }

    async fn get_header_for_height(&self, height: u32) -> R<Vec<u8>> {
        self.inner.get_header_for_height(height).await
    }

    async fn hash_to_header(&self, hash: &str) -> R<BlockHeader> {
        self.inner.hash_to_header(hash).await
    }

    async fn get_raw_tx(&self, txid: &str, next: bool) -> R<GetRawTxResult> {
        self.inner.get_raw_tx(txid, next).await
    }

    async fn get_merkle_path(&self, txid: &str, next: bool) -> R<GetMerklePathResult> {
        self.inner.get_merkle_path(txid, next).await
    }

    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> R<Vec<PostBeefResult>> {
        self.inner.post_beef(beef, txids).await
    }

    async fn get_utxo_status(
        &self,
        script: &str,
        form: Option<GetUtxoStatusOutputFormat>,
        at: Option<&str>,
        next: bool,
    ) -> R<GetUtxoStatusResult> {
        self.inner.get_utxo_status(script, form, at, next).await
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
        next: bool,
    ) -> R<GetStatusForTxidsResult> {
        self.inner.get_status_for_txids(txids, next).await
    }

    async fn get_bsv_exchange_rate(&self) -> R<f64> {
        self.inner.get_bsv_exchange_rate().await
    }

    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> R<f64> {
        self.inner.get_fiat_exchange_rate(currency, base).await
    }

    fn hash_output_script(&self, script: &[u8]) -> String {
        self.inner.hash_output_script(script)
    }

    async fn is_utxo(&self, txid: &str, vout: u32, script: &[u8]) -> UtxoVerdict {
        self.inner.is_utxo(txid, vout, script).await
    }

    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> R<bool> {
        self.inner.n_lock_time_is_final(n_lock_time).await
    }

    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> R<bool> {
        self.inner.n_lock_time_is_final_for_tx(input).await
    }

    async fn get_beef(&self, txid: &str, known: &[String]) -> R<GetBeefResult> {
        self.inner.get_beef(txid, known).await
    }
}

/// `create_action_internal` on `beef` (the caller's input `parent:0`), no
/// tracker on the caller or the storage, the services `UnreachableHeaders`.
/// Returns the store, the services' asks for a tracker and the outcome.
async fn create_unreachable(parent: &Transaction, beef: &[u8]) -> (StorageSqlx, usize, Error) {
    let (storage, user_id) = store().await;
    let services = Arc::new(UnreachableHeaders::default());
    storage.set_services(services.clone());
    let args = caller_args(parent, beef, false);
    let error = create_action_internal(&storage, None, user_id, args)
        .await
        .map(|_| ())
        .unwrap_err();
    let asks = services.asks.load(std::sync::atomic::Ordering::SeqCst);
    (storage, asks, error)
}

/// #515: the caller's inputBEEF carries an unproven parent alone, no BUMP,
/// so no caller root to verify: the services are not asked, and the call
/// goes on to the assembly, which refuses the parent's missing ancestry
/// with its own structure text. Before, the services were asked whenever
/// the BEEF was non-empty and the call held with `header_fault`'s text.
#[tokio::test]
async fn create_asks_no_header_service_for_a_beef_with_no_caller_root() {
    let grandparent = spend(&"11".repeat(32), "00", 10_000, "51");
    let parent = spend(&grandparent.id(), "", 9_000, "51");
    let mut beef = Beef::new();
    beef.merge_raw_tx(parent.to_binary(), None);
    let (storage, asks, error) = create_unreachable(&parent, &beef.to_binary()).await;
    eprintln!("no caller root: {error}; asks={asks}");
    // The text up to the rejected bytes' dump path, which names the hour.
    let (gp, p) = (grandparent.id(), parent.id());
    let structure = format!(
        "Validation error: inputBEEF: BEEF structure is invalid: ancestor(s) not stored, \
         and createAction fetches none: [{gp}]; roots [{}]; missing inputs [{}]; \
         transactions with missing inputs [{}]; not valid [none]; txid-only [none]; \
         1 tx(s), 0 bump(s), 68 bytes; dump ",
        &p[..16],
        &gp[..16],
        &p[..16]
    );
    assert!(matches!(error, Error::ValidationError(_)), "{error:?}");
    assert!(error.to_string().starts_with(&structure), "{error}");
    assert_eq!(asks, 0, "the services are not asked");
    assert_eq!(action_rows(&storage).await, 0);
}

/// #515, unchanged: the same services with a BEEF that carries a caller
/// root are asked, and the call holds with `header_fault`'s text, nothing
/// written: a fault is could-not-look, never an admit.
#[tokio::test]
async fn create_holds_a_caller_root_with_the_header_services_fault() {
    let (proven, mut beef, _) = proven_only(false);
    let (storage, asks, error) = create_unreachable(&proven, &beef.to_binary()).await;
    eprintln!("caller root: {error}; asks={asks}");
    assert!(matches!(error, Error::ServiceError(_)), "{error:?}");
    assert_eq!(error.to_string(), UNREACHABLE_HOLD);
    assert_eq!(asks, 1);
    assert_eq!(action_rows(&storage).await, 0);
}
