//! The manager's drain runs inside [`scope`]; a local writer's proof
//! preflight reads it with [`current`]. The scope says whether the drain's
//! reader is a local store (the wallet's own rows, whose new proof rows the
//! writer takes as given) and carries the chain answers already read in the
//! drain, by height, so a height is asked of the network once per sync. A
//! chunk that reaches a store's sync door outside such a scope, or inside a
//! scope whose reader is not local, has every proof row the store does not
//! hold as the same proof validated.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

/// One manager sync: whether its reader is local, and by height the Merkle
/// root the tracker confirmed there and the hash of the active header at
/// that height.
#[derive(Default)]
#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
pub(crate) struct SyncScope {
    pub local_reader: bool,
    pub answers: Mutex<HashMap<i64, (String, String)>>,
}

tokio::task_local! {
    static SYNC_SCOPE: Arc<SyncScope>;
}

/// Run `f` (a drain) as one sync, from a local reader or not.
pub(crate) async fn scope<F: Future>(local_reader: bool, f: F) -> F::Output {
    let scope = SyncScope {
        local_reader,
        answers: Mutex::default(),
    };
    SYNC_SCOPE.scope(Arc::new(scope), f).await
}

/// The scope of the sync this call runs in, if it runs in a manager's drain.
#[cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]
pub(crate) fn current() -> Option<Arc<SyncScope>> {
    SYNC_SCOPE.try_with(Arc::clone).ok()
}
