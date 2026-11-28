//! Evidence is a proof this store checked: its root confirmed by this
//! store's chain tracker and recorded. A proof row merely stored, a
//! provider's word, and a `spent-proven` check whose recorded spender is
//! the transaction itself are not evidence against it.
//!
//! The store of `held_receipt_scope_tests`: X's action P spending X's coin
//! C (the coinbase's output 0), Y's received row of P held `sending`, R
//! another spend of C.

use std::sync::Arc;

use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MockChainTracker};
use chrono::Utc;

use super::competitor_proof::{apply_take_on, TakeApplied, TakeRows};
use super::held_receipt_scope_tests::{
    p_raw, r_raw, read, seed, user, write_word, z_spends_c, Cell, Writer, HEIGHT, PAYMENT, P_HEIGHT,
};
use super::reorg_tests::{single_leaf_bump, storage, txid_of};
use super::sync::{entity_names, get_sync_chunk_internal, process_sync_chunk_internal};
use super::void_lineage_tests::serving;
use crate::services::WalletServices;
use crate::storage::traits::{RequestSyncChunkArgs, SyncOffset};
use crate::storage::MonitorStorage;

/// A tracker that cannot answer.
struct ErringTracker;

#[async_trait::async_trait]
impl ChainTracker for ErringTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Err(ChainTrackerError::NetworkError("no answer".into()))
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Err(ChainTrackerError::NetworkError("no answer".into()))
    }
}

async fn checked(c: &Cell, txid: &str) -> bool {
    let sql = format!(
        "SELECT {}",
        super::held_receipts::checked_proof_condition("?1")
    );
    sqlx::query_scalar(&sql)
        .bind(txid)
        .fetch_one(c.s.pool())
        .await
        .unwrap()
}

/// A sync offset at the start of `name`.
fn from_zero(name: &str) -> SyncOffset {
    SyncOffset {
        name: name.to_string(),
        offset: 0,
    }
}

/// A take of P through the one write that applies a take.
async fn take(c: &Cell) -> TakeApplied {
    let check = c.s.competitor_check(&c.p, None).await.unwrap();
    let mut tx = c.s.write_tx().await.unwrap();
    let applied = apply_take_on(
        &mut tx,
        &check,
        &[("test", "seen")],
        TakeRows::Held,
        Utc::now(),
    )
    .await
    .unwrap();
    super::storage_sqlx::commit(tx).await.unwrap();
    applied
}

/// The proof task's ask keeps the competitor's proof row and this store's
/// root-check record for it, in the write that retires P.
#[tokio::test]
async fn the_ask_keeps_the_competitors_checked_proof() {
    let c = seed("sending").await;
    let r = txid_of(&r_raw());
    assert!(!checked(&c, &r).await);
    write_word(&c, Writer::Competitor).await;
    let got = read(&c).await;
    assert_eq!(
        (got.x_row.as_str(), got.y_row.as_str()),
        ("failed", "failed")
    );
    assert!(checked(&c, &r).await, "R's checked proof is on record");
}

/// A competitor proof the store's sync door placed from another store (Z's
/// R, its path's root on no header, no check of this store's) beside a
/// word: the received row keeps its word, and a take while the store has
/// no tracker of its own is applied, not refused on it.
#[tokio::test]
async fn a_stored_proof_no_check_saw_is_no_evidence() {
    let c = seed("sending").await;
    assert!(write_word(&c, Writer::Retire).await);

    let other = storage().await;
    // Z is the third user of either store (the rows' references name it).
    user(&other, &"a".repeat(66)).await;
    user(&other, &"b".repeat(66)).await;
    z_spends_c(&other, "completed").await;
    let r = txid_of(&r_raw());
    super::reorg_tests::insert_proven(
        &other,
        &r,
        950,
        &"0".repeat(64),
        &r_raw(),
        &single_leaf_bump(950, &r),
    )
    .await;
    let identity = "c".repeat(66);
    let args = RequestSyncChunkArgs {
        from_storage_identity_key: "0".repeat(64),
        to_storage_identity_key: "0".repeat(64),
        identity_key: identity,
        since: None,
        max_rough_size: 1_000_000,
        max_items: 1000,
        offsets: vec![
            from_zero(entity_names::PROVEN_TX),
            from_zero(entity_names::OUTPUT_BASKET),
            from_zero(entity_names::TRANSACTION),
            from_zero(entity_names::OUTPUT),
        ],
    };
    let chunk = get_sync_chunk_internal(&other, args.clone()).await.unwrap();
    let mut map = crate::storage::SyncMap::default();
    process_sync_chunk_internal(&c.s, args, chunk, &mut map)
        .await
        .unwrap();
    let placed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proven_txs WHERE txid = ?")
        .bind(&r)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    assert_eq!(placed, 1, "the sync door placed R's proof row");
    assert!(!checked(&c, &r).await);

    MonitorStorage::review_status(&c.s).await.unwrap();
    let got = read(&c).await;
    assert_eq!(
        (got.y_row.as_str(), got.y_out_spendable),
        ("sending", true),
        "the received row keeps its word: {got:?}"
    );

    c.s.clear_chain_tracker().await;
    assert_eq!(take(&c).await, TakeApplied::Applied);
    assert_eq!(read(&c).await.y_row, "unproven");
}

/// A stored competitor proof this store checked, while no tracker answers:
/// a take neither counts nor is refused on it; it waits (a note) and heals
/// when the tracker answers: refused while the root stands, applied once
/// the root is gone.
#[tokio::test]
async fn a_take_beside_a_checked_competitor_waits_for_the_tracker() {
    let c = seed("sending").await;
    assert!(write_word(&c, Writer::Retire).await);
    z_spends_c(&c.s, "unproven").await;
    let r = txid_of(&r_raw());
    c.s.ingest_merkle_proof(
        &r,
        &single_leaf_bump(HEIGHT, &r),
        HEIGHT,
        &"0".repeat(64),
        None,
    )
    .await
    .unwrap();
    assert!(checked(&c, &r).await);

    c.s.set_chain_tracker(Arc::new(ErringTracker)).await;
    assert_eq!(take(&c).await, TakeApplied::Withheld);
    assert_eq!(read(&c).await.y_row, "sending");
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&c.p)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    assert!(history.contains("trackerNotAnswered"), "{history}");

    let mut confirming = MockChainTracker::new(1000);
    confirming.add_root(HEIGHT, r.clone());
    c.s.set_chain_tracker(Arc::new(confirming)).await;
    assert!(matches!(take(&c).await, TakeApplied::Refused { .. }));
    assert_eq!(read(&c).await.y_row, "sending");

    c.s.set_chain_tracker(Arc::new(MockChainTracker::new(1000)))
        .await;
    assert_eq!(take(&c).await, TakeApplied::Applied);
    assert_eq!(read(&c).await.y_row, "unproven");
}

/// P retired as undelivered, then mined: the locked-input re-check reads
/// X's coin spent (by P itself), and that mark is no evidence against P:
/// the status review leaves Y's received row as it was, and P's proof
/// completes it and counts the payment.
#[tokio::test]
async fn an_undelivered_transaction_that_mines_is_not_evidence_against_itself() {
    let c = seed("sending").await;
    assert!(write_word(&c, Writer::Retire).await);
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minutes')")
        .execute(c.s.writer())
        .await
        .unwrap();
    let spent: Arc<dyn WalletServices> =
        Arc::new(serving(&p_raw(), None).spends_are_proven(true).build());
    let report =
        c.s.recheck_locked_inputs(spent.as_ref(), 10, true)
            .await
            .unwrap();
    assert_eq!(report.spent, 1, "{report:?}");

    MonitorStorage::review_status(&c.s).await.unwrap();
    let got = read(&c).await;
    assert_eq!(
        (got.y_row.as_str(), got.y_out_spendable),
        ("sending", true),
        "Y's row is not failed on its own transaction's spend: {got:?}"
    );

    let bump = single_leaf_bump(P_HEIGHT, &c.p);
    c.s.ingest_merkle_proof(&c.p, &bump, P_HEIGHT, &"0".repeat(64), None)
        .await
        .unwrap();
    let got = read(&c).await;
    assert_eq!(
        (got.y_row.as_str(), got.y_balance),
        ("completed", PAYMENT),
        "{got:?}"
    );
}
