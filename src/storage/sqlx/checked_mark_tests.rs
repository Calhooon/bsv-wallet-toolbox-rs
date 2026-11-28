//! A mark written on a checked proof is lowered only by evidence about
//! that same proof, never by a later write that merely does not know.
//!
//! The store: our R (`sending`, spending the coinbase's output 0, its
//! outputs R:0 and R:1), our D spending R:0 and an outside coin O (output 0
//! of the completed OT), our E spending R:1. A stranger's C spends O and is
//! mined: the proof task asks for C, its proof checks, D is retired and O
//! reads `spent-proven`. Then R is retired for a reason of its own: a
//! second competitor C2 whose proof checks, the host's retire of an
//! undelivered transaction, or the poisoned chain's retire from R (asked of
//! a provider that answers every coin unspent).

use std::sync::Arc;

use bsv_rs::transaction::MockChainTracker;
use chrono::{Duration, Utc};

use super::reorg_tests::{
    open_gate, seed_user, single_leaf_bump, storage, txid_of, COINBASE_HEX, COINBASE_TXID,
};
use super::void_lineage_tests::{apply_rejected_naming, serving};
use super::StorageSqlx;
use crate::services::mock::{MockResponse, MockWalletServicesBuilder};
use crate::services::traits::GetStatusForTxidsResult;
use crate::services::{TxStatusDetail, WalletServices};
use crate::storage::traits::WalletStorageProvider;
use crate::storage::MonitorStorage;

/// C's proof height, C2's.
const C_HEIGHT: u32 = 900;
const C2_HEIGHT: u32 = 902;
/// OT's txid: a parent this store holds completed, spending a coin of
/// nobody's.
const NOBODY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

/// A transaction spending `inputs` (txid, vout) with one output of each
/// of `values`, an empty script everywhere.
pub(super) fn spending(inputs: &[(&str, u32)], values: &[u64]) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&1u32.to_le_bytes());
    raw.push(u8::try_from(inputs.len()).unwrap());
    for (txid, vout) in inputs {
        let mut prev = hex::decode(txid).unwrap();
        prev.reverse();
        raw.extend_from_slice(&prev);
        raw.extend_from_slice(&vout.to_le_bytes());
        raw.push(0);
        raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
    }
    raw.push(u8::try_from(values.len()).unwrap());
    for value in values {
        raw.extend_from_slice(&value.to_le_bytes());
        raw.push(0);
    }
    raw.extend_from_slice(&0u32.to_le_bytes());
    raw
}

fn ot_raw() -> Vec<u8> {
    spending(&[(NOBODY, 0)], &[2000])
}
fn r_raw() -> Vec<u8> {
    spending(&[(COINBASE_TXID, 0)], &[1000, 1000])
}
fn d_raw() -> Vec<u8> {
    spending(
        &[(&txid_of(&r_raw()), 0), (&txid_of(&ot_raw()), 0)],
        &[2500],
    )
}
fn e_raw() -> Vec<u8> {
    spending(&[(&txid_of(&r_raw()), 1)], &[900])
}
fn f_raw() -> Vec<u8> {
    spending(&[(&txid_of(&d_raw()), 0)], &[2400])
}
/// The stranger's spend of O.
fn c_raw() -> Vec<u8> {
    spending(&[(&txid_of(&ot_raw()), 0)], &[1999])
}
/// A second spend of the coinbase's output 0: R's own competitor.
fn c2_raw() -> Vec<u8> {
    spending(&[(COINBASE_TXID, 0)], &[777])
}

pub(super) struct Store {
    pub(super) s: StorageSqlx,
    pub(super) r: String,
    pub(super) d: String,
    pub(super) e: String,
    /// D's own child.
    pub(super) f: String,
    /// O's output row.
    pub(super) o: i64,
    pub(super) d_row: i64,
}

async fn tx_row(s: &StorageSqlx, user_id: i64, raw: &[u8], status: &str) -> i64 {
    let old = Utc::now() - Duration::days(2);
    let txid = txid_of(raw);
    sqlx::query_scalar(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, 'checked mark', 0, 1, 0, ?5, ?6, ?7, ?7) RETURNING transaction_id",
    )
    .bind(user_id)
    .bind(&txid)
    .bind(status)
    .bind(format!("ref-{}", &txid[..8]))
    .bind(raw)
    .bind(status != "completed")
    .bind(old)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

async fn out_row(
    s: &StorageSqlx,
    user_id: i64,
    owner: i64,
    txid: &str,
    vout: i64,
    spent_by: Option<i64>,
) -> i64 {
    let old = Utc::now() - Duration::days(2);
    let query = sqlx::query_scalar(
        "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, spent_by, locking_script, created_at, updated_at) \
         VALUES (?1, ?2, ?3, 1, ?4, 1000, 'storage', 'change', 'P2PKH', ?5, ?6, x'51', ?7, ?7) RETURNING output_id",
    )
    .bind(user_id)
    .bind(owner)
    .bind(spent_by.is_none())
    .bind(vout)
    .bind(txid)
    .bind(spent_by)
    .bind(old);
    query.fetch_one(s.writer()).await.unwrap()
}

async fn req_row(s: &StorageSqlx, raw: &[u8]) {
    let old = Utc::now() - Duration::days(2);
    super::reorg_tests::insert_req(s, &txid_of(raw), raw, "unsent", 0, None, old).await;
}

/// The services the proof task asks: `raw`'s bytes and a proof of it at
/// `height`; no status source holds anything; every coin reads unspent.
pub(super) fn asking(raw: &[u8], height: u32) -> Arc<dyn WalletServices> {
    Arc::new(
        status_unknown(serving(raw, Some(single_leaf_bump(height, &txid_of(raw)))))
            .is_utxo_response(MockResponse::Success(true))
            .build(),
    )
}

pub(super) fn status_unknown(b: MockWalletServicesBuilder) -> MockWalletServicesBuilder {
    b.get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
        name: "status".to_string(),
        status: "success".to_string(),
        error: None,
        results: vec![TxStatusDetail {
            txid: "00".repeat(32),
            status: "unknown".to_string(),
            ..Default::default()
        }],
    }))
}

async fn seed() -> Store {
    let s = storage().await;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(C_HEIGHT, txid_of(&c_raw()));
    tracker.add_root(C2_HEIGHT, txid_of(&c2_raw()));
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    let (user_id, _) = seed_user(&s).await;
    let coinbase = hex::decode(COINBASE_HEX).unwrap();
    let cb = tx_row(&s, user_id, &coinbase, "completed").await;
    let ot = tx_row(&s, user_id, &ot_raw(), "completed").await;
    let r_row = tx_row(&s, user_id, &r_raw(), "sending").await;
    let d_row = tx_row(&s, user_id, &d_raw(), "sending").await;
    let e_row = tx_row(&s, user_id, &e_raw(), "sending").await;
    let f_row = tx_row(&s, user_id, &f_raw(), "sending").await;
    out_row(&s, user_id, cb, COINBASE_TXID, 0, Some(r_row)).await;
    let o = out_row(&s, user_id, ot, &txid_of(&ot_raw()), 0, Some(d_row)).await;
    out_row(&s, user_id, r_row, &txid_of(&r_raw()), 0, Some(d_row)).await;
    out_row(&s, user_id, r_row, &txid_of(&r_raw()), 1, Some(e_row)).await;
    out_row(&s, user_id, d_row, &txid_of(&d_raw()), 0, Some(f_row)).await;
    out_row(&s, user_id, e_row, &txid_of(&e_raw()), 0, None).await;
    out_row(&s, user_id, f_row, &txid_of(&f_raw()), 0, None).await;
    for raw in [r_raw(), d_raw(), e_raw(), f_raw()] {
        req_row(&s, &raw).await;
    }
    Store {
        s,
        r: txid_of(&r_raw()),
        d: txid_of(&d_raw()),
        e: txid_of(&e_raw()),
        f: txid_of(&f_raw()),
        o,
        d_row,
    }
}

/// The proof task asks for `competitor`, named against `txid` by Arcade's
/// pushed REJECTED, and its proof at `height` checks.
async fn competitor_proven(s: &StorageSqlx, txid: &str, competitor: &[u8], height: u32) {
    apply_rejected_naming(s, txid, &txid_of(competitor)).await;
    WalletStorageProvider::set_services(s, asking(competitor, height));
    s.synchronize_transaction_statuses().await.unwrap();
}

/// R's own retire.
#[derive(Clone, Copy, Debug)]
enum RetireR {
    SecondCompetitor,
    Undelivered,
    PoisonedRoot,
}

const RETIRES: [RetireR; 3] = [
    RetireR::SecondCompetitor,
    RetireR::Undelivered,
    RetireR::PoisonedRoot,
];

async fn retire_r(st: &Store, how: RetireR) {
    let s = &st.s;
    match how {
        RetireR::SecondCompetitor => competitor_proven(s, &st.r, &c2_raw(), C2_HEIGHT).await,
        RetireR::Undelivered => {
            let services = asking(&c2_raw(), C2_HEIGHT);
            WalletStorageProvider::set_services(s, services.clone());
            s.retire_undeliverable_txid(services.as_ref(), &st.r, "invalid")
                .await
                .unwrap();
        }
        RetireR::PoisonedRoot => {
            let services = asking(&c2_raw(), C2_HEIGHT);
            WalletStorageProvider::set_services(s, services.clone());
            s.retire_poisoned_chain(services.as_ref(), &st.r, "invalid", true)
                .await
                .unwrap();
        }
    }
}

pub(super) async fn verdict(s: &StorageSqlx, output_id: i64) -> Option<String> {
    let sql = "SELECT last_verdict FROM locked_input_checks WHERE output_id = ?";
    let row: Option<(Option<String>,)> = sqlx::query_as(sql)
        .bind(output_id)
        .fetch_optional(s.pool())
        .await
        .unwrap();
    row.and_then(|(v,)| v)
}

pub(super) async fn coin(s: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
    let sql = "SELECT spendable, spent_by FROM outputs WHERE output_id = ?";
    sqlx::query_as(sql)
        .bind(output_id)
        .fetch_one(s.pool())
        .await
        .unwrap()
}

pub(super) async fn req_word(s: &StorageSqlx, txid: &str) -> String {
    word_of(s, "proven_tx_reqs", txid).await
}

pub(super) async fn tx_word(s: &StorageSqlx, txid: &str) -> String {
    word_of(s, "transactions", txid).await
}

async fn word_of(s: &StorageSqlx, table: &str, txid: &str) -> String {
    let sql = format!("SELECT status FROM {table} WHERE txid = ?");
    let (word,): (String,) = sqlx::query_as(&sql)
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    word
}

/// O's check row and D's request: (verdict, attempts, next check), (word,
/// attempts).
async fn d_marks(st: &Store) -> ((Option<String>, i64, String), (String, i64)) {
    let check: (Option<String>, i64, String) = sqlx::query_as(
        "SELECT last_verdict, attempts, CAST(next_check_at AS TEXT) FROM locked_input_checks WHERE output_id = ?",
    )
    .bind(st.o)
    .fetch_one(st.s.pool())
    .await
    .unwrap();
    let req: (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
            .bind(&st.d)
            .fetch_one(st.s.pool())
            .await
            .unwrap();
    (check, req)
}

/// Every locked-input re-check due now.
async fn due_now(s: &StorageSqlx) {
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minutes')")
        .execute(s.writer())
        .await
        .unwrap();
}

/// D ended on C's checked proof, then R retired each way: O's verdict
/// stays `spent-proven`, O stays locked by D, and the re-check, asked of a
/// provider that answers every coin unspent, does not ask for O and
/// releases nothing.
#[tokio::test]
async fn a_spent_proven_verdict_stays_through_every_later_retire() {
    let mut wrong = Vec::new();
    for how in RETIRES {
        let st = seed().await;
        competitor_proven(&st.s, &st.d, &c_raw(), C_HEIGHT).await;
        assert_eq!(req_word(&st.s, &st.d).await, "doubleSpend", "{how:?}");
        assert_eq!(
            verdict(&st.s, st.o).await.as_deref(),
            Some("spent-proven"),
            "{how:?}"
        );
        retire_r(&st, how).await;
        let after_retire = (verdict(&st.s, st.o).await, coin(&st.s, st.o).await);
        due_now(&st.s).await;
        let services = asking(&c2_raw(), C2_HEIGHT);
        let report =
            st.s.recheck_locked_inputs(services.as_ref(), 50, true)
                .await
                .unwrap();
        let asked_o = report.checks.iter().any(|c| c.output_id == st.o);
        let after_recheck = (verdict(&st.s, st.o).await, coin(&st.s, st.o).await);
        let want = (Some("spent-proven".to_string()), (false, Some(st.d_row)));
        if after_retire != want || after_recheck != want || asked_o {
            wrong.push(format!(
                "{how:?}: after the retire {after_retire:?}, after the re-check {after_recheck:?}, O asked {asked_o}"
            ));
        }
    }
    assert!(wrong.is_empty(), "marks lowered:\n{}", wrong.join("\n"));
}

/// The descendants' walk keeps a descendant already ended on a checked
/// double spend: none of its inputs' verdicts (O's verdict, attempts and
/// schedule) and no word of its request (`doubleSpend`, its attempts) is
/// rewritten, for each of R's retires; and where D is ended by its own
/// competitor after R's retire began (the walk cut by a busy store after
/// R's rows, finished by the status review), the same. D's child F was
/// walked when D was ended (`failed`, `invalid`) and reads so after.
#[tokio::test]
async fn the_walk_keeps_a_descendant_ended_on_a_checked_double_spend() {
    let mut wrong = Vec::new();
    for how in RETIRES {
        let st = seed().await;
        competitor_proven(&st.s, &st.d, &c_raw(), C_HEIGHT).await;
        let before = d_marks(&st).await;
        let f_before = (tx_word(&st.s, &st.f).await, req_word(&st.s, &st.f).await);
        retire_r(&st, how).await;
        let after = d_marks(&st).await;
        let f_after = (tx_word(&st.s, &st.f).await, req_word(&st.s, &st.f).await);
        if after != before || f_before != ("failed".into(), "invalid".into()) || f_after != f_before
        {
            wrong.push(format!(
                "{how:?}: D {before:?} -> {after:?}; F {f_before:?} -> {f_after:?}"
            ));
        }
        if tx_word(&st.s, &st.e).await != "failed" {
            wrong.push(format!("{how:?}: E not retired"));
        }
    }

    // D ended by its own competitor after R's retire began.
    let st = seed().await;
    st.s.busy_next_descendant_retire();
    let services = asking(&c2_raw(), C2_HEIGHT);
    WalletStorageProvider::set_services(&st.s, services.clone());
    assert!(st
        .s
        .retire_undeliverable_txid(services.as_ref(), &st.r, "invalid")
        .await
        .is_err());
    assert_eq!(tx_word(&st.s, &st.r).await, "failed");
    assert_eq!(tx_word(&st.s, &st.d).await, "sending");
    competitor_proven(&st.s, &st.d, &c_raw(), C_HEIGHT).await;
    let before = d_marks(&st).await;
    MonitorStorage::review_status(&st.s).await.unwrap();
    let after = d_marks(&st).await;
    if after != before {
        wrong.push(format!(
            "ended after the walk began: D {before:?} -> {after:?}"
        ));
    }
    if tx_word(&st.s, &st.e).await != "failed" {
        wrong.push("ended after the walk began: E not retired by the review".into());
    }
    assert!(wrong.is_empty(), "walk off the rule:\n{}", wrong.join("\n"));
}

/// The schedule's own rule: a caller that brings `unknown` or a hint to a
/// coin whose verdict reads `spent-proven` writes nothing over it, neither
/// the verdict nor its schedule; a `spent-proven` verdict is written over
/// any other.
#[tokio::test]
async fn the_schedule_keeps_a_spent_proven_verdict_over_a_weaker_one() {
    let st = seed().await;
    competitor_proven(&st.s, &st.d, &c_raw(), C_HEIGHT).await;
    let read = |s: &StorageSqlx, o: i64| {
        let s = s.clone();
        async move {
            sqlx::query_as::<_, (Option<String>, i64, String)>(
                "SELECT last_verdict, attempts, CAST(next_check_at AS TEXT) FROM locked_input_checks WHERE output_id = ?",
            )
            .bind(o)
            .fetch_one(s.pool())
            .await
            .unwrap()
        }
    };
    let before = read(&st.s, st.o).await;
    for weaker in [
        super::LOCKED_VERDICT_UNKNOWN,
        super::LOCKED_VERDICT_SPENT_HINT,
    ] {
        st.s.schedule_locked_input_check(st.o, weaker).await;
        assert_eq!(
            read(&st.s, st.o).await,
            before,
            "{weaker} over spent-proven"
        );
    }
    st.s.schedule_locked_input_check(st.o, super::LOCKED_VERDICT_UNKNOWN)
        .await;
    let other: i64 =
        sqlx::query_scalar("SELECT output_id FROM outputs WHERE txid = ? AND vout = 0")
            .bind(&st.r)
            .fetch_one(st.s.pool())
            .await
            .unwrap();
    st.s.schedule_locked_input_check(other, super::LOCKED_VERDICT_SPENT_PROVEN)
        .await;
    assert_eq!(
        verdict(&st.s, other).await.as_deref(),
        Some("spent-proven"),
        "spent-proven over unknown"
    );
}
