//! Arcade's status words replayed through every toolbox surface that reads
//! them (P0-2b, bsv-stack-lean `docs/p0/p0-2b.md`).
//!
//! The vector `tests/vectors/arcade_status_verdicts.json` carries one
//! recorded body per Arcade word (arcade@1ae1208 `models/transaction.go:89-114`)
//! on each surface: the `POST /tx` answer, the `GET /tx/{txid}` document and
//! the SSE frame / webhook body, plus the intake refusals (HTTP 400 with the
//! ARC code in the body), the two reorg-correction payloads and the latch
//! sequence of the scenario of record. Each case names the verdict each
//! client must give; every mismatch is listed before the test fails.
//!
//! The rule: REJECTED, DOUBLE_SPEND_ATTEMPTED (a conflict) and any ORPHAN word
//! fail; every other word Arcade defines is accepted as the hint it is; a word
//! Arcade does not define is an invalid response; `reorg_unmined` and
//! `reorg_reanchor` are a re-ask, never a status change by themselves.

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, Transaction, TransactionInput, TransactionOutput, BEEF_V1,
};
use bsv_wallet_toolbox_rs::services::{Arc, Arcade};
use serde_json::Value;

const VECTOR: &str = include_str!("vectors/arcade_status_verdicts.json");
const HEIGHT: u32 = 850_000;
const LOCK_HEX: &str = "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac";

fn cases(surface: &str) -> Vec<Value> {
    let v: Value = serde_json::from_str(VECTOR).expect("the vector parses");
    v["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .filter(|c| c["surface"] == surface)
        .cloned()
        .collect()
}

fn coinbase_bump_hex(txid: &str, height: u32) -> String {
    MerklePath::from_coinbase_txid(txid, height).to_hex()
}

/// The case's body with its placeholders filled.
fn body_of(case: &Value, txid: &str, height: u32) -> String {
    serde_json::to_string(&case["body"])
        .unwrap()
        .replace("{TXID}", txid)
        .replace("{BUMP}", &coinbase_bump_hex(txid, height))
        .replace("{BLOCKHASH}", &"bb".repeat(32))
        .replace("{COMPETITOR}", &"cc".repeat(32))
        .replace("{NOW}", &chrono::Utc::now().to_rfc3339())
}

fn synthetic_tx(source_txid: Option<String>, source_vout: u32, satoshis: u64) -> Transaction {
    Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some(source_txid.unwrap_or_else(|| "00".repeat(32))),
            source_output_index: source_vout,
            unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
            unlocking_script_template: None,
            sequence: 0xFFFFFFFF,
        }],
        vec![TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: LockingScript::from_hex(LOCK_HEX).unwrap(),
            change: false,
        }],
        0,
    )
}

/// A proven parent and one unproven child: Arcade gets one EF on `POST /tx`.
fn one_tx_beef() -> (Vec<u8>, Transaction, String) {
    let parent = synthetic_tx(None, 0xFFFFFFFF, 2_000_000);
    let parent_txid = parent.id();
    let child = synthetic_tx(Some(parent_txid.clone()), 0, 1_000_000);
    let child_txid = child.id();
    let mut beef = Beef::with_version(BEEF_V1);
    let idx = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, HEIGHT));
    beef.merge_raw_tx(parent.to_binary(), Some(idx));
    beef.merge_transaction(child.clone());
    (beef.to_binary(), child, child_txid)
}

/// The outcome word of one per-txid broadcast result (the vector's
/// `toolbox_*_submit` vocabulary).
fn outcome_of(status: &str, r: &bsv_wallet_toolbox_rs::services::PostTxResultForTxid) -> String {
    if status == "success" && r.is_success() {
        "success".into()
    } else if r.orphan_mempool {
        "orphan".into()
    } else if r.double_spend {
        "double_spend".into()
    } else if bsv_wallet_toolbox_rs::storage::is_definitive_rejection(r) {
        "rejected".into()
    } else if r.service_error {
        "service_error".into()
    } else {
        format!("other(status={}, data={:?})", r.status, r.data)
    }
}

fn report(what: &str, mismatches: Vec<String>, total: usize) {
    assert!(
        mismatches.is_empty(),
        "{} of {} recorded Arcade bodies get the wrong verdict ({}):\n  {}",
        mismatches.len(),
        total,
        what,
        mismatches.join("\n  ")
    );
}

#[tokio::test]
async fn every_arcade_submit_answer_is_judged_by_its_word_and_its_code() {
    let all = cases("submit");
    let mut mismatches = Vec::new();
    for case in &all {
        let name = case["name"].as_str().unwrap();
        let Some(expect) = case["expect"]["toolbox_arcade_submit"].as_str() else {
            continue;
        };
        let (beef, _child, txid) = one_tx_beef();
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/tx")
            .with_status(case["http"].as_u64().unwrap() as usize)
            .with_header("content-type", "application/json")
            .with_body(body_of(case, &txid, HEIGHT))
            .create_async()
            .await;
        let arcade = Arcade::new(server.url(), None, None).unwrap();
        let got = match arcade.post_beef(&beef, std::slice::from_ref(&txid)).await {
            Ok(result) => {
                let r = result
                    .txid_results
                    .iter()
                    .find(|r| r.txid == txid)
                    .expect("a result for the subject");
                outcome_of(&result.status, r)
            }
            Err(e) => format!("Err({e})"),
        };
        if got != expect {
            mismatches.push(format!("{name}: expected {expect}, got {got}"));
        }
    }
    report("Arcade::post_beef", mismatches, all.len());
}

#[tokio::test]
async fn every_2xx_word_through_the_arc_client_is_judged_by_one_verdict() {
    let all = cases("submit");
    let mut mismatches = Vec::new();
    for case in &all {
        let name = case["name"].as_str().unwrap();
        let Some(expect) = case["expect"]["toolbox_arc_submit"].as_str() else {
            continue;
        };
        let (_beef, child, txid) = one_tx_beef();
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/v1/tx")
            .with_status(case["http"].as_u64().unwrap() as usize)
            .with_header("content-type", "application/json")
            .with_body(body_of(case, &txid, HEIGHT))
            .create_async()
            .await;
        let arc = Arc::new(server.url(), None, None).unwrap();
        let got = match arc
            .post_raw_tx(&child.to_hex(), Some(std::slice::from_ref(&txid)))
            .await
        {
            Ok(r) => outcome_of(&r.status, &r),
            // A body ARC's client cannot read is a fault of the answer.
            Err(_) => "service_error".to_string(),
        };
        if got != expect {
            mismatches.push(format!("{name}: expected {expect}, got {got}"));
        }
    }
    report("Arc::post_raw_tx", mismatches, all.len());
}

#[tokio::test]
async fn every_arcade_status_document_maps_to_the_right_triage_word_and_proof() {
    let all = cases("status");
    let mut mismatches = Vec::new();
    for case in &all {
        let name = case["name"].as_str().unwrap();
        let txid = "ab".repeat(32);
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", format!("/tx/{txid}").as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body_of(case, &txid, HEIGHT))
            .expect_at_least(1)
            .create_async()
            .await;
        let arcade = Arcade::new(server.url(), None, None).unwrap();
        if let Some(expect) = case["expect"]["toolbox_status"].as_str() {
            let got = arcade
                .get_status_for_txids(std::slice::from_ref(&txid))
                .await
                .map(|r| r.results[0].status.clone())
                .unwrap_or_else(|e| format!("Err({e})"));
            if got != expect {
                mismatches.push(format!("{name}: status expected {expect}, got {got}"));
            }
        }
        if let Some(expect) = case["expect"]["toolbox_proof"].as_bool() {
            let got = arcade
                .get_merkle_path(&txid)
                .await
                .map(|r| r.merkle_path.is_some())
                .unwrap_or(false);
            if got != expect {
                mismatches.push(format!("{name}: proof served expected {expect}, got {got}"));
            }
        }
    }
    report("Arcade::get_status_for_txids / get_merkle_path", mismatches, all.len());
}

#[cfg(feature = "sqlite")]
mod push {
    use super::*;
    use bsv_rs::transaction::MockChainTracker;
    use bsv_wallet_toolbox_rs::monitor::ArcadeEventsTask;
    use bsv_wallet_toolbox_rs::services::ArcadeStatusEvent;
    use bsv_wallet_toolbox_rs::storage::StorageSqlx;
    use bsv_wallet_toolbox_rs::WalletStorageWriter;
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn storage_with_tracker(roots: &[(&str, u32)], tip: u32) -> StorageSqlx {
        let storage = StorageSqlx::in_memory().await.expect("in_memory storage");
        storage
            .migrate("test-arcade-vector", &("02".to_string() + &"ab".repeat(32)))
            .await
            .expect("migrate");
        storage.make_available().await.expect("make_available");
        bsv_wallet_toolbox_rs::MonitorStorage::set_max_acceptable_proof_height(&storage, u32::MAX)
            .await
            .expect("open the proof gate");
        set_tracker(&storage, roots, tip).await;
        storage
    }

    async fn set_tracker(storage: &StorageSqlx, roots: &[(&str, u32)], tip: u32) {
        let mut tracker = MockChainTracker::new(tip);
        for (txid, height) in roots {
            let root = MerklePath::from_coinbase_txid(txid, *height)
                .compute_root(Some(txid))
                .unwrap();
            tracker.add_root(*height, root);
        }
        storage.set_chain_tracker(std::sync::Arc::new(tracker)).await;
    }

    async fn seed(storage: &StorageSqlx, txid: &str, req: &str, tx: &str) {
        let (user, _) = storage
            .find_or_insert_user(&("02".to_string() + &"cd".repeat(32)))
            .await
            .unwrap();
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) \
             VALUES (?, ?, 0, '{}', 0, '{}', X'01000000', ?, ?)",
        )
        .bind(txid)
        .bind(req)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
             version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 'arcade vector tx', -500, 1, 0, X'01000000', 1, ?, ?)",
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(tx)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    async fn req_status(storage: &StorageSqlx, txid: &str) -> String {
        let (s,): (String,) = sqlx::query_as("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        s
    }

    async fn proven_height(storage: &StorageSqlx, txid: &str) -> Option<i64> {
        sqlx::query_as::<_, (i64,)>("SELECT height FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(storage.pool())
            .await
            .unwrap()
            .map(|(h,)| h)
    }

    fn event(body: &str) -> ArcadeStatusEvent {
        serde_json::from_str(body).expect("an Arcade status body parses as an event")
    }

    #[tokio::test]
    async fn every_arcade_push_applies_its_word_and_the_reorg_words_only_re_ask() {
        let all = cases("push");
        let mut mismatches = Vec::new();
        for case in &all {
            let name = case["name"].as_str().unwrap();
            let Some(expect) = case["expect"]["toolbox_push"].as_str() else {
                continue;
            };
            let txid = "ab".repeat(32);
            let storage = storage_with_tracker(&[(&txid, HEIGHT)], HEIGHT + 1).await;
            seed(&storage, &txid, "sending", "sending").await;
            let trigger = AtomicBool::new(false);
            let ev = event(&body_of(case, &txid, HEIGHT));
            if let Err(e) = ArcadeEventsTask::apply_event(&storage, &ev, &trigger).await {
                mismatches.push(format!("{name}: apply_event failed: {e}"));
                continue;
            }
            let req = req_status(&storage, &txid).await;
            let reask = trigger.load(Ordering::SeqCst);
            let got = match (req.as_str(), reask) {
                ("sending", false) => "none".to_string(),
                ("sending", true) => "reask".to_string(),
                ("unmined", false) => "seen".to_string(),
                ("unmined", true) => "seen+reask".to_string(),
                ("invalid", _) => "invalid".to_string(),
                ("doubleSpend", _) => "double_spend".to_string(),
                ("completed", _) => "proven".to_string(),
                (other, r) => format!("req={other} reask={r}"),
            };
            if got != expect {
                mismatches.push(format!("{name}: expected {expect}, got {got}"));
            }
        }
        report("ArcadeEventsTask::apply_event", mismatches, all.len());
    }

    /// The scenario of record (mined, orphaned, mined again) with Arcade's
    /// own bodies, the vector's `latch_mined_unmined_mined_again`: the
    /// reorg_unmined word changes nothing by itself and schedules a re-ask;
    /// the re-anchor's path, checked against the headers, replaces the
    /// stored anchor. No latch.
    #[tokio::test]
    async fn mined_then_reorg_unmined_then_mined_again_re_anchors_and_never_latches() {
        let v: Value = serde_json::from_str(VECTOR).unwrap();
        let steps = v["sequences"][0]["steps"].as_array().unwrap().clone();
        let txid = "ab".repeat(32);
        let a1 = HEIGHT;
        let b3 = HEIGHT + 2;
        let fill = |step: &Value| {
            serde_json::to_string(&step["body"])
                .unwrap()
                .replace("{TXID}", &txid)
                .replace("{A1}", &"a1".repeat(32))
                .replace("{B3}", &"b3".repeat(32))
                .replace("{BUMP_A1}", &coinbase_bump_hex(&txid, a1))
                .replace("{BUMP_B3}", &coinbase_bump_hex(&txid, b3))
                .replace("{NOW}", &chrono::Utc::now().to_rfc3339())
        };

        // Step 2: A1 is active; MINED with A1's path is stored.
        let storage = storage_with_tracker(&[(&txid, a1)], a1 + 1).await;
        seed(&storage, &txid, "unmined", "unproven").await;
        let trigger = AtomicBool::new(false);
        ArcadeEventsTask::apply_event(&storage, &event(&fill(&steps[0])), &trigger)
            .await
            .unwrap();
        assert_eq!(req_status(&storage, &txid).await, "completed");
        assert_eq!(proven_height(&storage, &txid).await, Some(a1 as i64));

        // Step 3b: A1 is orphaned (the headers no longer carry its root);
        // Arcade reverts T with reorg_unmined. The word applies nothing and
        // schedules a re-ask.
        set_tracker(&storage, &[], b3 + 1).await;
        let trigger = AtomicBool::new(false);
        let applied =
            ArcadeEventsTask::apply_event(&storage, &event(&fill(&steps[1])), &trigger)
                .await
                .unwrap();
        assert!(!applied, "reorg_unmined changed a status by itself");
        assert!(
            trigger.load(Ordering::SeqCst),
            "reorg_unmined did not schedule a re-ask"
        );
        assert_eq!(req_status(&storage, &txid).await, "completed");
        assert_eq!(proven_height(&storage, &txid).await, Some(a1 as i64));

        // Step 5: B3 carries T; MINED reorg_reanchor with B3's path. The path
        // is checked against the headers and replaces the anchor.
        set_tracker(&storage, &[(&txid, b3)], b3 + 1).await;
        let trigger = AtomicBool::new(false);
        ArcadeEventsTask::apply_event(&storage, &event(&fill(&steps[2])), &trigger)
            .await
            .unwrap();
        assert_eq!(req_status(&storage, &txid).await, "completed");
        assert_eq!(
            proven_height(&storage, &txid).await,
            Some(b3 as i64),
            "the re-anchor did not replace the orphaned anchor: the latch"
        );
    }
}
