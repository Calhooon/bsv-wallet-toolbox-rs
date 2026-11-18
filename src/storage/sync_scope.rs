//! The manager's drain runs inside [`scope`]; a local writer's proof
//! preflight reads it with [`current`]. The scope says whether the chunk
//! the drain hands its writer was read from a local store (the wallet's own
//! rows, whose new proof rows the writer takes as given: the reader
//! answered local both before and after that chunk was read, set with
//! [`set_local_reader`]) and carries the chain answers already read in the
//! drain, by height, so a height is asked of the network once per sync. A
//! chunk that reaches a store's sync door outside such a scope, or inside a
//! scope whose reader is not local, has every proof row the store does not
//! hold as the same proof validated.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// One manager sync: whether the chunk in hand was read from a local
/// reader, and by height the Merkle root the tracker confirmed there and
/// the hash of the active header at that height.
#[derive(Default)]
#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
pub(crate) struct SyncScope {
    local_reader: AtomicBool,
    pub answers: Mutex<HashMap<i64, (String, String)>>,
}

#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
impl SyncScope {
    /// Whether the chunk in hand was read from a local reader.
    pub fn local_reader(&self) -> bool {
        self.local_reader.load(Ordering::SeqCst)
    }
}

tokio::task_local! {
    static SYNC_SCOPE: Arc<SyncScope>;
}

/// Run `f` (a drain) as one sync; no chunk is from a local reader until
/// the drain says so.
pub(crate) async fn scope<F: Future>(f: F) -> F::Output {
    SYNC_SCOPE.scope(Arc::new(SyncScope::default()), f).await
}

/// Inside a drain: whether the chunk it hands its writer next was read from
/// a local reader. Outside one, nothing.
pub(crate) fn set_local_reader(local: bool) {
    let _ = SYNC_SCOPE.try_with(|s| s.local_reader.store(local, Ordering::SeqCst));
}

/// The scope of the sync this call runs in, if it runs in a manager's drain.
#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
pub(crate) fn current() -> Option<Arc<SyncScope>> {
    SYNC_SCOPE.try_with(Arc::clone).ok()
}
