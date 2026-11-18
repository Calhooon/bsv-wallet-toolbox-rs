//! What the manager answers about its active from the rows it holds, apart
//! from the `set_active` matrix (`tests/sync_set_active_tests.rs`): a
//! no-op call dropped while it reads the rows, and what `get_stores`
//! answers after a call dropped part way.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::*;

mod sync_support;
use sync_support::double::Double;
use sync_support::*;

/// Stores A and B, both rows naming A, under a manager made available on A.
async fn two() -> (Vec<Arc<Double>>, WalletStorageManager) {
    let mut doubles = Vec::new();
    for sik in [SRC, DST] {
        let s = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
            .await
            .unwrap();
        s.migrate(if sik == SRC { "A" } else { "B" }, sik)
            .await
            .unwrap();
        s.make_available().await.unwrap();
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        s.update_user_active_storage(u.user_id, SRC).await.unwrap();
        doubles.push(Arc::new(Double::new(s)));
    }
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(doubles[0].clone() as Arc<dyn MonitorStorage>),
        Some(vec![doubles[1].clone() as Arc<dyn MonitorStorage>]),
    );
    mgr.make_available().await.unwrap();
    (doubles, mgr)
}

async fn rows(doubles: &[Arc<Double>]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for d in doubles {
        out.push(active_storage_of(&d.inner, ALICE).await);
    }
    out
}

/// `set_active(A)` to the manager's own active, every row naming A, B's
/// read never answering: the call is dropped while it reads, before any
/// write. It wrote nothing, so the manager still answers A as enabled;
/// once B answers, the call answers unchanged.
#[tokio::test]
async fn a_no_op_dropped_while_it_reads_leaves_the_manager_enabled() {
    let (doubles, mgr) = two().await;
    doubles[1].hang_user_reads.store(true, Ordering::SeqCst);
    let dropped = tokio::time::timeout(Duration::from_millis(300), mgr.set_active(SRC))
        .await
        .is_err();
    let after = (
        rows(&doubles).await,
        mgr.get_active_store().await.unwrap() == SRC,
        mgr.is_active_enabled().await,
    );
    doubles[1].hang_user_reads.store(false, Ordering::SeqCst);
    let again = bounded("the no-op once B answers", mgr.set_active(SRC)).await;
    println!(
        "WITNESS a no-op dropped while B's read hangs: dropped {dropped}; rows, on A, enabled {after:?}; once B answers: {}",
        refusal(&again)
    );
    assert!(dropped);
    assert_eq!(
        after,
        (
            vec![Some(SRC.to_string()), Some(SRC.to_string())],
            true,
            true
        )
    );
    assert_eq!(again.unwrap(), format!("setActive to {SRC} unchanged\n"));
}

/// `set_active(B)` whose target writes its row and never answers, the
/// call dropped: every row names B, and `get_stores` answers the active
/// (still A) as not enabled, as `is_active_enabled` does; a retry once B
/// answers completes and both answer B as enabled.
#[tokio::test]
async fn get_stores_answers_not_enabled_after_a_dropped_switch() {
    let (doubles, mgr) = two().await;
    doubles[1].hang_after_write.store(true, Ordering::SeqCst);
    let dropped = tokio::time::timeout(Duration::from_millis(300), mgr.set_active(DST))
        .await
        .is_err();
    let enabled = |stores: Vec<WalletStorageInfo>| {
        stores
            .iter()
            .find(|s| s.is_active)
            .map(|s| (s.storage_identity_key == SRC, s.is_enabled))
    };
    let after = (
        rows(&doubles).await,
        enabled(mgr.get_stores().await),
        mgr.is_active_enabled().await,
    );
    doubles[1].hang_after_write.store(false, Ordering::SeqCst);
    let retry = refusal(&bounded("the retry", mgr.set_active(DST)).await);
    let after_retry = (
        enabled(mgr.get_stores().await),
        mgr.is_active_enabled().await,
    );
    println!(
        "WITNESS set_active(B) dropped after B wrote: dropped {dropped}; rows, (active is A, enabled) by get_stores, is_active_enabled {after:?}; retry {retry}: {after_retry:?}"
    );
    assert!(dropped);
    assert_eq!(
        after,
        (
            vec![Some(DST.to_string()), Some(DST.to_string())],
            Some((true, false)),
            false
        )
    );
    assert_eq!(retry, "ok");
    assert_eq!(after_retry, (Some((false, true)), true));
}
