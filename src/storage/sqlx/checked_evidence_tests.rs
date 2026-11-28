//! Evidence is a proof this store checked: its root confirmed by this
//! store's chain tracker and recorded. Beside a word, a proof row this
//! store has not checked, and a `spent-proven` check whose recorded spender
//! is the transaction itself, are not evidence against it; a take beside a
//! stored competitor proof asks the tracker of its root.
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
pub(super) struct ErringTracker;

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

pub(super) async fn checked(c: &Cell, txid: &str) -> bool {
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

/// With a tracker that refutes every root, a take of P is applied and Y's
/// row lifted.
async fn applied_once_refuted(c: &Cell) {
    c.s.set_chain_tracker(Arc::new(MockChainTracker::new(1000)))
        .await;
    assert_eq!(take(c).await, TakeApplied::Applied);
    assert_eq!(read(c).await.y_row, "unproven");
}

/// P's own proof, checked, at `P_HEIGHT`.
pub(super) async fn p_proven(c: &Cell) {
    let bump = single_leaf_bump(P_HEIGHT, &c.p);
    c.s.ingest_merkle_proof(&c.p, &bump, P_HEIGHT, &"0".repeat(64), None)
        .await
        .unwrap();
}

/// Y's row completed and its payment counted.
pub(super) async fn y_counted(c: &Cell) {
    let got = read(c).await;
    assert_eq!(
        (got.y_row.as_str(), got.y_balance),
        ("completed", PAYMENT),
        "{got:?}"
    );
}

/// A take of P through the one write that applies a take.
pub(super) async fn take(c: &Cell) -> TakeApplied {
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

/// Z's R and its proof row in `c`'s store, at a height whose root our
/// headers do not carry, with no root-check record.
async fn synced_r(c: &Cell) {
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
    assert_eq!(placed, 1, "R's proof row is stored");
    assert!(!checked(c, &r).await);
}

/// A proof row this store has not checked (Z's R, its path's root on no
/// header) beside a word: the word asks the tracker of its root. Refuted,
/// it is no evidence and the received row keeps its word; with a tracker
/// that errs it counts, and the row ends, recorded as beside a proof this
/// store had not checked. A take beside it asks the tracker too: with a
/// tracker that cannot answer it waits; with one that refutes the root it
/// is applied.
#[tokio::test]
async fn a_stored_proof_no_check_saw_is_no_evidence() {
    for erring in [false, true] {
        let c = seed("sending").await;
        assert!(write_word(&c, Writer::Retire).await);
        synced_r(&c).await;
        if erring {
            c.s.set_chain_tracker(Arc::new(ErringTracker)).await;
        }
        MonitorStorage::review_status(&c.s).await.unwrap();
        let got = read(&c).await;
        if erring {
            assert_eq!(
                (got.y_row.as_str(), got.y_out_spendable),
                ("failed", false),
                "no tracker answered: the row ends: {got:?}"
            );
            assert!(!checked(&c, &txid_of(&r_raw())).await);
            continue;
        }
        assert_eq!(
            (got.y_row.as_str(), got.y_out_spendable),
            ("sending", true),
            "refuted: the received row keeps its word: {got:?}"
        );

        c.s.set_chain_tracker(Arc::new(ErringTracker)).await;
        assert_eq!(take(&c).await, TakeApplied::Withheld);
        assert_eq!(read(&c).await.y_row, "sending");

        applied_once_refuted(&c).await;
    }
}

/// Z's R spending C beside P, `unproven`, with a proof row of R at
/// `height` whose path is valid and no root-check record. Returns R's txid.
pub(super) async fn z_r_proven_unrecorded(c: &Cell, height: u32) -> String {
    z_spends_c(&c.s, "unproven").await;
    let r = txid_of(&r_raw());
    super::reorg_tests::insert_proven(
        &c.s,
        &r,
        height,
        &"0".repeat(64),
        &r_raw(),
        &single_leaf_bump(height, &r),
    )
    .await;
    assert!(!checked(c, &r).await, "R's proof row has no record");
    r
}

/// A proof row of R this store has not checked, beside a take of P: the
/// take asks the tracker. Its root confirmed, the take is refused and the
/// root-check record is written in the take's write; at a height whose
/// root the tracker refutes, the take is applied.
#[tokio::test]
async fn a_take_asks_the_tracker_of_a_proof_row_with_no_record() {
    for (height, refused) in [(HEIGHT, true), (950, false)] {
        let c = seed("sending").await;
        assert!(write_word(&c, Writer::Retire).await);
        let r = z_r_proven_unrecorded(&c, height).await;
        let applied = take(&c).await;
        if refused {
            assert!(
                matches!(applied, TakeApplied::Refused { .. }),
                "{applied:?}"
            );
            assert_eq!(read(&c).await.y_row, "sending");
            assert!(checked(&c, &r).await, "the take's write recorded R's root");
        } else {
            assert_eq!(applied, TakeApplied::Applied);
            assert_eq!(read(&c).await.y_row, "unproven");
            assert!(!checked(&c, &r).await);
        }
    }
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

    applied_once_refuted(&c).await;
}

/// P retired as undelivered, its coin marked by the locked-input re-check
/// on a services answer naming `spender` (no spender's proof stored).
pub(super) async fn retired_then_marked(c: &Cell, spender: &[u8]) {
    assert!(write_word(c, Writer::Retire).await);
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minutes')")
        .execute(c.s.writer())
        .await
        .unwrap();
    let spent: Arc<dyn WalletServices> =
        Arc::new(serving(spender, None).spends_are_proven(true).build());
    let report =
        c.s.recheck_locked_inputs(spent.as_ref(), 10, true)
            .await
            .unwrap();
    assert_eq!(report.spent, 1, "{report:?}");
}

/// P retired as undelivered, then mined: the locked-input re-check reads
/// X's coin spent (by P itself), and P's proof arrives. With P holding a
/// proof this store checked the mark is its own spend: a word beside Y's
/// row does not fail it, and the proof has completed it and counted the
/// payment.
#[tokio::test]
async fn an_undelivered_transaction_that_mines_is_not_evidence_against_itself() {
    let c = seed("sending").await;
    retired_then_marked(&c, &p_raw()).await;
    p_proven(&c).await;
    assert!(checked(&c, &c.p).await);

    MonitorStorage::mark_transaction_rejected(&c.s, &c.p, false)
        .await
        .unwrap();
    MonitorStorage::review_status(&c.s).await.unwrap();
    y_counted(&c).await;
}

/// A mark with no spender on record, beside a transaction that holds no
/// checked proof, is evidence against it: a take of P is refused and Y's
/// held row stays held. Once P's checked proof arrives the proof completes
/// Y's row.
#[tokio::test]
async fn a_mark_with_no_spender_on_record_refuses_a_take() {
    let c = seed("sending").await;
    retired_then_marked(&c, &r_raw()).await;
    assert!(matches!(take(&c).await, TakeApplied::Refused { .. }));
    assert_eq!(read(&c).await.y_row, "sending");
    p_proven(&c).await;
    y_counted(&c).await;
}
