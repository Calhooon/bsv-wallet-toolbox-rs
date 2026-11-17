//! A sync whose reader is not a local store: the manager runs its drain
//! inside [`scope`], and a local writer's proof preflight reads the marker
//! with [`current`]. The marker carries the chain answers already read in
//! the drain, by height, so a height is asked of the network once per sync.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

/// The chain answers a sync from a reader that is not local has read: by
/// height, the Merkle root the tracker confirmed there and the hash of the
/// active header at that height.
#[derive(Default)]
#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
pub(crate) struct RemoteSync {
    pub answers: Mutex<HashMap<i64, (String, String)>>,
}

tokio::task_local! {
    static REMOTE_SYNC: Arc<RemoteSync>;
}

/// Run `f` (a drain) as a sync from a reader that is not local.
pub(crate) async fn scope<F: Future>(f: F) -> F::Output {
    REMOTE_SYNC.scope(Arc::new(RemoteSync::default()), f).await
}

/// The marker of the sync this call runs in, when its reader is not local.
#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
pub(crate) fn current() -> Option<Arc<RemoteSync>> {
    REMOTE_SYNC.try_with(Arc::clone).ok()
}
