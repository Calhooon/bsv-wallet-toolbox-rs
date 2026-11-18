//! The partition against a dropped call, and a call that finds the mark set:
//! these hold the manager's own locks and fields, so they live beside it.

use super::*;
use crate::storage::StorageSqlx;

const ALICE: &str = "02a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
const OTHER: &str = "03ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
const A: &str = "partition-store-a";
const B: &str = "partition-store-b";

/// Two stores whose rows of the user name A; B numbers the user apart
/// (another user takes its id 1), so the user id moves with the active.
async fn manager() -> (Vec<Arc<StorageSqlx>>, WalletStorageManager, i64, i64) {
    let mut stores = Vec::new();
    let mut ids = Vec::new();
    for (sik, other_first) in [(A, false), (B, true)] {
        let s = StorageSqlx::in_memory().await.unwrap();
        s.migrate(sik, sik).await.unwrap();
        s.make_available().await.unwrap();
        if other_first {
            s.find_or_insert_user(OTHER).await.unwrap();
        }
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        s.update_user_active_storage(u.user_id, A).await.unwrap();
        ids.push(u.user_id);
        stores.push(Arc::new(s));
    }
    assert_ne!(ids[0], ids[1]);
    let a: Arc<dyn MonitorStorage> = stores[0].clone();
    let b: Arc<dyn MonitorStorage> = stores[1].clone();
    let mgr = WalletStorageManager::new(ALICE.to_string(), Some(a), Some(vec![b]));
    mgr.make_available().await.unwrap();
    (stores, mgr, ids[0], ids[1])
}

/// The fields the partition writes: (active index, backups, conflicting,
/// user id, is_active, cached settings' key).
async fn fields(
    mgr: &WalletStorageManager,
) -> (
    Option<usize>,
    Vec<usize>,
    Vec<usize>,
    Option<i64>,
    Option<bool>,
    String,
) {
    let auth = mgr.auth_id.read().await.clone();
    let key = lock_read(&mgr.cached_settings)
        .unwrap()
        .as_ref()
        .map(|t| t.storage_identity_key.clone())
        .unwrap_or_default();
    (
        *mgr.active_index.read().await,
        mgr.backup_indices.read().await.clone(),
        mgr.conflicting_indices.read().await.clone(),
        auth.user_id,
        auth.is_active,
        key,
    )
}

async fn rows(stores: &[Arc<StorageSqlx>]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for s in stores {
        out.push(s.find_or_insert_user(ALICE).await.unwrap().0.active_storage);
    }
    out
}

#[tokio::test]
async fn a_call_dropped_inside_the_partition_leaves_every_field_as_it_was() {
    let (stores, mgr, id_a, id_b) = manager().await;
    let old = (
        Some(0),
        vec![1],
        vec![],
        Some(id_a),
        Some(true),
        A.to_string(),
    );
    assert_eq!(fields(&mgr).await, old);

    // A reader of the auth holds its lock: the partition, the transition's
    // last step, waits on it, and the host's timeout drops the call there.
    let held = mgr.auth_id.read().await;
    let answer = tokio::time::timeout(Duration::from_secs(3), mgr.set_active(B)).await;
    assert!(
        answer.is_err(),
        "set_active answered while the auth was held"
    );
    drop(held);

    // Every store's row was written: the call was inside the partition.
    let both_b = vec![Some(B.to_string()), Some(B.to_string())];
    assert_eq!(rows(&stores).await, both_b);
    let after = fields(&mgr).await;
    eprintln!("WITNESS dropped inside the partition: {after:?}");
    assert_eq!(after, old, "a dropped partition left mixed fields");
    assert!(!mgr.is_active_enabled().await);

    // A retry: every field and the answer agree with the rows (all B).
    let retry = mgr.set_active(B).await;
    let after = fields(&mgr).await;
    eprintln!("WITNESS retry: {retry:?} {after:?}");
    retry.unwrap();
    assert_eq!(
        after,
        (
            Some(1),
            vec![0],
            vec![],
            Some(id_b),
            Some(true),
            B.to_string()
        )
    );
    assert_eq!(rows(&stores).await, both_b);
    assert!(mgr.is_active_enabled().await);
    assert_eq!(mgr.get_active_store().await.unwrap(), B);
}

#[tokio::test]
async fn a_call_that_finds_the_mark_set_partitions_from_the_rows() {
    let (stores, mgr, id_a, _) = manager().await;
    let good = (
        Some(0),
        vec![1],
        vec![],
        Some(id_a),
        Some(true),
        A.to_string(),
    );

    // What an earlier call dropped with the mark set may have left: the
    // fields apart from the rows (every row still names A).
    mgr.set_rows_in_doubt(true);
    *mgr.backup_indices.write().await = Vec::new();
    mgr.auth_id.write().await.user_id = Some(id_a + 100);
    *lock_write(&mgr.cached_settings).unwrap() = Some(TableSettings {
        storage_identity_key: B.to_string(),
        ..TableSettings::default()
    });

    let answer = mgr.set_active(A).await.unwrap();
    let after = fields(&mgr).await;
    eprintln!("WITNESS no-op with the mark set: {answer:?} {after:?}");
    assert!(answer.contains("unchanged"), "{answer}");
    assert_eq!(
        after, good,
        "the no-op answered from fields it did not rebuild"
    );
    assert_eq!(
        rows(&stores).await,
        vec![Some(A.to_string()), Some(A.to_string())]
    );
    assert!(mgr.is_active_enabled().await);
}

/// Two rebuilds from different rows: one reads the rows (every row naming
/// A) and waits on the index lock (held by a reader here); the rows then
/// move to B and a second rebuild reads them. The row write waits for the
/// earlier rebuild's writes (it holds the stores read lock through them),
/// so the earlier writes from the rows as they stood and the later from
/// the new rows: the manager's fields are the newer rows'.
#[tokio::test]
async fn a_rebuild_from_older_rows_never_writes_after_one_from_newer_rows() {
    let (_stores, mgr, _, id_b) = manager().await;
    let mgr = Arc::new(mgr);
    let held = mgr.active_index.read().await;
    let older = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.partition().await })
    };
    tokio::task::yield_now().await;

    // The rows move to B (as a re-read of the stores would leave them).
    let move_rows = |mgr: Arc<WalletStorageManager>| async move {
        for store in mgr.stores.write().await.iter_mut() {
            store.user.as_mut().unwrap().active_storage = Some(B.to_string());
        }
    };
    let moved = tokio::spawn(move_rows(mgr.clone()));
    let early = tokio::time::timeout(Duration::from_millis(300), async {
        while !moved.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    drop(held);
    older.await.unwrap().unwrap();
    moved.await.unwrap();
    let newer = mgr.partition().await.unwrap();
    let after = fields(&mgr).await;
    eprintln!(
        "WITNESS two rebuilds: rows moved while the older waited: {early}; {} {after:?}",
        newer.storage_identity_key
    );
    assert!(
        !early,
        "a row moved between a rebuild's read and its writes"
    );
    assert_eq!(
        after,
        (
            Some(1),
            vec![0],
            vec![],
            Some(id_b),
            Some(true),
            B.to_string()
        )
    );
    assert!(mgr.is_available().await);
    assert!(mgr.is_active_enabled().await);
}
