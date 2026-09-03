//! `StorageSqlx::retire_poisoned_chain`: a phantom transaction and every
//! unproven descendant built on it, retired under THE RELEASE RULE.
//!
//! Real SQLite, mock chain oracle. The fixture is the 2026-09-02 beta shape:
//! a received (internalized) upvote `R` that never reached the network, a
//! spend `C1` that took `R`'s payment together with an outside, on-chain
//! coin, and `C2` chained on `C1`'s change (three deep).

#![cfg(feature = "sqlite")]

use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::TxStatusDetail;
use bsv_wallet_toolbox_rs::{
    GetStatusForTxidsResult, PoisonOutcome, RetireOutcome, StorageSqlx, WalletStorageWriter,
    BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_ACCEPTED, BROADCAST_STATUS_REJECTED,
    BROADCAST_STATUS_SEEN, PROVIDER_ARCADE_V2,
};

/// An on-chain parent (outside the poisoned set).
const P: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
/// The phantom root: a received payment.
const R: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
/// Spends R:0 (inside) and P:0 (outside).
const C1: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
/// Spends C1:0 (inside).
const C2: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

struct Ids {
    p0: i64,
    r0: i64,
    c1_change: i64,
    c1_payment: i64,
    c2_change: i64,
}

struct Seeded {
    storage: StorageSqlx,
    ids: Ids,
}

async fn insert_tx(
    storage: &StorageSqlx,
    user_id: i64,
    txid: &str,
    status: &str,
    is_outgoing: bool,
) -> i64 {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 0, 1, 0, ?, ?, X'01000000', ?, ?)",
    )
    .bind(user_id)
    .bind(status)
    .bind(&txid[..8])
    .bind(is_outgoing as i64)
    .bind(&txid[..8])
    .bind(txid)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("tx")
    .last_insert_rowid()
}

#[allow(clippy::too_many_arguments)]
async fn insert_output(
    storage: &StorageSqlx,
    user_id: i64,
    basket_id: i64,
    tx_row: i64,
    txid: &str,
    vout: i64,
    satoshis: i64,
    spendable: bool,
    spent_by: Option<i64>,
) -> i64 {
    let now = chrono::Utc::now();
    let lock = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, spent_by, provided_by, purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, 'P2PKH', ?, 1, ?, 'storage', 'change', 'coin', ?, ?)",
    )
    .bind(user_id)
    .bind(tx_row)
    .bind(basket_id)
    .bind(vout)
    .bind(satoshis)
    .bind(&lock)
    .bind(txid)
    .bind(spendable as i64)
    .bind(spent_by)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("output")
    .last_insert_rowid()
}

async fn insert_req(storage: &StorageSqlx, txid: &str, status: &str) {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) \
         VALUES (?, ?, 0, '{}', 0, '{}', X'01000000', ?, ?)",
    )
    .bind(txid)
    .bind(status)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("req");
}

/// P (completed) -> C1 <- R (unproven, received, 10,000 sats at R:0)
/// C1 (unproven): change C1:0 (40,000, spent by C2), payment C1:1 (5,000)
/// C2 (unproven): change C2:0 (39,000)
async fn seed(c2_status: &str) -> Seeded {
    let storage = StorageSqlx::in_memory().await.expect("storage");
    let storage_key = "02".to_string() + &"ab".repeat(32);
    storage
        .migrate("poison-tests", &storage_key)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let identity = "02".to_string() + &"cd".repeat(32);
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let user_id = user.user_id;
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .expect("basket")
        .basket_id;

    let p = insert_tx(&storage, user_id, P, "completed", false).await;
    let r = insert_tx(&storage, user_id, R, "unproven", false).await;
    let c1 = insert_tx(&storage, user_id, C1, "unproven", true).await;
    let c2 = insert_tx(&storage, user_id, C2, c2_status, true).await;

    let p0 = insert_output(&storage, user_id, basket, p, P, 0, 50_000, false, Some(c1)).await;
    let r0 = insert_output(&storage, user_id, basket, r, R, 0, 10_000, false, Some(c1)).await;
    let c1_change = insert_output(
        &storage,
        user_id,
        basket,
        c1,
        C1,
        0,
        40_000,
        false,
        Some(c2),
    )
    .await;
    let c1_payment = insert_output(&storage, user_id, basket, c1, C1, 1, 5_000, true, None).await;
    let c2_change = insert_output(&storage, user_id, basket, c2, C2, 0, 39_000, true, None).await;

    insert_req(&storage, C1, "unmined").await;
    insert_req(&storage, C2, "unmined").await;

    storage
        .record_broadcast_status(C1, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN)
        .await
        .unwrap();
    storage
        .record_broadcast_status(C2, PROVIDER_ARCADE_V2, BROADCAST_STATUS_ACCEPTED)
        .await
        .unwrap();

    Seeded {
        storage,
        ids: Ids {
            p0,
            r0,
            c1_change,
            c1_payment,
            c2_change,
        },
    }
}

async fn output_state(storage: &StorageSqlx, id: i64) -> (i64, Option<i64>) {
    sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(id)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn tx_status(storage: &StorageSqlx, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn req_status(storage: &StorageSqlx, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn memory_status(storage: &StorageSqlx, txid: &str, provider: &str) -> Option<String> {
    storage
        .broadcast_status_of(txid, provider)
        .await
        .unwrap()
        .map(|r| r.status)
}

fn alive_oracle(txid: &str) -> MockWalletServices {
    MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: txid.to_string(),
                status: "known".to_string(),
                depth: None,
            }],
        }))
        .build()
}

#[tokio::test]
async fn the_walk_lists_the_descendants_by_depth() {
    let s = seed("unproven").await;
    let chain = s.storage.poisoned_descendants(R).await.unwrap();
    let listed: Vec<(String, u32)> = chain.iter().map(|t| (t.txid.clone(), t.depth)).collect();
    assert_eq!(listed, vec![(C1.to_string(), 1), (C2.to_string(), 2)]);
    assert!(chain.iter().all(|t| t.is_outgoing && t.is_retirable()));
    assert!(s.storage.poisoned_descendants(C2).await.unwrap().is_empty());
    assert!(s
        .storage
        .poisoned_descendants(&"ee".repeat(32))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_dry_run_reports_the_chain_and_touches_nothing() {
    let s = seed("unproven").await;
    let report = s
        .storage
        .retire_poisoned_chain(&MockWalletServices::new(), R, "invalid", false)
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::Retired);
    assert!(!report.executed);
    assert_eq!(report.retirable_txids(), vec![R, C1, C2]);
    assert_eq!(report.chain[0].depth, 0);
    assert!(
        !report.chain[0].is_outgoing,
        "the root is a received payment"
    );
    assert_eq!(
        (report.failed, report.restored, report.invalidated),
        (0, 0, 0)
    );

    assert_eq!(tx_status(&s.storage, R).await, "unproven");
    assert_eq!(tx_status(&s.storage, C2).await, "unproven");
    assert_eq!(output_state(&s.storage, s.ids.p0).await.0, 0);
    assert_eq!(output_state(&s.storage, s.ids.c2_change).await.0, 1);
    assert_eq!(
        memory_status(&s.storage, C1, PROVIDER_ARCADE_V2)
            .await
            .as_deref(),
        Some(BROADCAST_STATUS_SEEN)
    );
}

#[tokio::test]
async fn the_chain_is_retired_three_deep_with_the_outside_input_released() {
    let s = seed("unproven").await;
    // The oracle: R is unknown (not alive); P:0 is verifiably unspent.
    let report = s
        .storage
        .retire_poisoned_chain(&MockWalletServices::new(), R, "invalid", true)
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::Retired);
    assert!(report.executed);
    assert_eq!(report.retirable_txids(), vec![R, C1, C2]);
    assert_eq!(report.failed, 3);
    assert_eq!((report.restored, report.restored_sats), (1, 50_000));
    assert_eq!(report.kept, 0);
    // Spendable outputs invalidated: C1:1 (5,000), C2:0 (39,000). R:0 was
    // already spent by C1 (a poisoned child), so it is not counted as
    // balance lost here, but it IS reported as the internalized phantom.
    assert_eq!((report.invalidated, report.invalidated_sats), (2, 44_000));
    assert_eq!(report.internalized.len(), 1);
    assert_eq!(report.internalized[0].txid, R);
    assert_eq!(report.internalized[0].vout, 0);
    assert_eq!(report.internalized[0].satoshis, 10_000);

    // The outside coin is back in coin selection.
    assert_eq!(output_state(&s.storage, s.ids.p0).await, (1, None));
    // The inside coins stay dead, still attributed to their phantom spender.
    let (spendable, spent_by) = output_state(&s.storage, s.ids.r0).await;
    assert_eq!(spendable, 0);
    assert!(spent_by.is_some(), "an inside input is never released");
    assert_eq!(output_state(&s.storage, s.ids.c1_change).await.0, 0);
    assert_eq!(output_state(&s.storage, s.ids.c1_payment).await.0, 0);
    assert_eq!(output_state(&s.storage, s.ids.c2_change).await.0, 0);

    for txid in [R, C1, C2] {
        assert_eq!(tx_status(&s.storage, txid).await, "failed", "{}", txid);
        assert_eq!(
            memory_status(&s.storage, txid, BROADCAST_PROVIDER_NETWORK)
                .await
                .as_deref(),
            Some(BROADCAST_STATUS_REJECTED),
            "{}",
            txid
        );
    }
    assert_eq!(tx_status(&s.storage, P).await, "completed");
    assert_eq!(req_status(&s.storage, C1).await, "invalid");
    assert_eq!(req_status(&s.storage, C2).await, "invalid");
    assert_eq!(
        memory_status(&s.storage, C1, PROVIDER_ARCADE_V2)
            .await
            .as_deref(),
        Some(BROADCAST_STATUS_REJECTED)
    );
    assert!(s
        .storage
        .broadcast_seen_for(PROVIDER_ARCADE_V2, &[C1.to_string(), C2.to_string()])
        .await
        .unwrap()
        .is_empty());

    // Idempotent: a second pass finds nothing left to fail.
    let again = s
        .storage
        .retire_poisoned_chain(&MockWalletServices::new(), R, "invalid", true)
        .await
        .unwrap();
    assert_eq!(again.outcome, PoisonOutcome::Retired);
    assert_eq!((again.failed, again.restored, again.invalidated), (0, 0, 0));
}

#[tokio::test]
async fn an_outside_input_the_chain_cannot_vouch_for_stays_locked() {
    let s = seed("unproven").await;
    let services = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Success(GetUtxoStatusResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            is_utxo: Some(false),
            details: vec![],
            error: None,
        }))
        .build();
    let report = s
        .storage
        .retire_poisoned_chain(&services, R, "invalid", true)
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::Retired);
    assert_eq!((report.restored, report.kept), (0, 1));
    let (spendable, spent_by) = output_state(&s.storage, s.ids.p0).await;
    assert_eq!(spendable, 0);
    assert!(spent_by.is_some(), "an unknown never releases money");
    assert_eq!(tx_status(&s.storage, C1).await, "failed");
}

#[tokio::test]
async fn an_alive_root_ends_the_retire_with_nothing_touched() {
    let s = seed("unproven").await;
    let report = s
        .storage
        .retire_poisoned_chain(&alive_oracle(R), R, "invalid", true)
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::Alive);
    assert!(!report.executed);
    assert_eq!(report.chain.len(), 3);
    assert_eq!(tx_status(&s.storage, R).await, "unproven");
    assert_eq!(tx_status(&s.storage, C2).await, "unproven");
    assert_eq!(output_state(&s.storage, s.ids.p0).await.0, 0);
    assert_eq!(output_state(&s.storage, s.ids.c2_change).await.0, 1);
}

#[tokio::test]
async fn a_proven_descendant_refuses_the_retire() {
    let s = seed("completed").await;
    let report = s
        .storage
        .retire_poisoned_chain(&MockWalletServices::new(), R, "invalid", true)
        .await
        .unwrap();
    assert_eq!(
        report.outcome,
        PoisonOutcome::Refused {
            proven_txid: C2.to_string()
        }
    );
    assert!(!report.executed);
    assert_eq!(tx_status(&s.storage, R).await, "unproven");
    assert_eq!(tx_status(&s.storage, C1).await, "unproven");
    assert_eq!(output_state(&s.storage, s.ids.c1_payment).await.0, 1);
}

#[tokio::test]
async fn an_unknown_root_is_not_found() {
    let s = seed("unproven").await;
    let report = s
        .storage
        .retire_poisoned_chain(
            &MockWalletServices::new(),
            &"ee".repeat(32),
            "invalid",
            true,
        )
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::NotFound);
    assert!(report.chain.is_empty());
}

#[tokio::test]
async fn an_already_failed_root_still_retires_its_descendants() {
    let s = seed("unproven").await;
    sqlx::query("UPDATE transactions SET status = 'failed' WHERE txid = ?")
        .bind(R)
        .execute(s.storage.pool())
        .await
        .unwrap();
    let report = s
        .storage
        .retire_poisoned_chain(&MockWalletServices::new(), R, "invalid", true)
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::Retired);
    assert_eq!(report.failed, 2, "C1 and C2");
    assert_eq!(report.retirable_txids(), vec![C1, C2]);
    assert_eq!(tx_status(&s.storage, C1).await, "failed");
    assert_eq!(tx_status(&s.storage, C2).await, "failed");
    // The failed root's phantom output is invalidated too.
    assert_eq!(output_state(&s.storage, s.ids.r0).await.0, 0);
    assert_eq!(output_state(&s.storage, s.ids.p0).await, (1, None));
}

#[tokio::test]
async fn retire_undeliverable_walks_the_descendants_too() {
    let s = seed("unproven").await;
    // C1 is the tx the broadcaster dropped; C2 is chained on its change.
    // The default oracle vouches for both of C1's inputs as unspent.
    let outcome = s
        .storage
        .retire_undeliverable_txid(&MockWalletServices::new(), C1, "invalid")
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Some(RetireOutcome::Retired {
            restored: 2,
            kept: 0
        })
    );
    assert_eq!(tx_status(&s.storage, C1).await, "failed");
    assert_eq!(tx_status(&s.storage, C2).await, "failed");
    assert_eq!(req_status(&s.storage, C2).await, "invalid");
    assert_eq!(output_state(&s.storage, s.ids.c1_payment).await.0, 0);
    assert_eq!(output_state(&s.storage, s.ids.c2_change).await.0, 0);
    // C1's change was C2's input: inside the poisoned set, never released.
    let (spendable, spent_by) = output_state(&s.storage, s.ids.c1_change).await;
    assert_eq!(spendable, 0);
    assert!(spent_by.is_some());
    assert_eq!(
        memory_status(&s.storage, C2, BROADCAST_PROVIDER_NETWORK)
            .await
            .as_deref(),
        Some(BROADCAST_STATUS_REJECTED)
    );
    // R (an ancestor, not a descendant) is untouched by this path.
    assert_eq!(tx_status(&s.storage, R).await, "unproven");
}

// =============================================================================
// 0.3.59: the upward climb and the locked-input re-checks
// =============================================================================

use bsv_wallet_toolbox_rs::services::mock::MockErrorKind;
use bsv_wallet_toolbox_rs::{
    ChainKnowledge, GetUtxoStatusResult, LockedInputVerdict, BROADCAST_PROVIDER_CHAIN,
    BROADCAST_STATUS_MINED,
};

/// A chain-known grandparent.
const G: &str = "1111111111111111111111111111111111111111111111111111111111111111";
/// Unproven and unknown to the chain: the real root of the poison.
const P2: &str = "2222222222222222222222222222222222222222222222222222222222222222";
/// Unproven and unknown: the transaction the verdict came from.
const X: &str = "3333333333333333333333333333333333333333333333333333333333333333";
/// Unproven child of X.
const CH: &str = "4444444444444444444444444444444444444444444444444444444444444444";

struct ClimbIds {
    g0: i64,
    p2_0: i64,
    x0: i64,
    ch0: i64,
}

/// G (`g_status`, known to the chain) -> P2 (unproven) -> X (unproven) -> CH
/// (unproven).
async fn seed_climb(g_status: &str) -> (StorageSqlx, ClimbIds) {
    let storage = StorageSqlx::in_memory().await.expect("storage");
    let storage_key = "02".to_string() + &"ab".repeat(32);
    storage
        .migrate("climb-tests", &storage_key)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let identity = "02".to_string() + &"cd".repeat(32);
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let user_id = user.user_id;
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .expect("basket")
        .basket_id;

    let g = insert_tx(&storage, user_id, G, g_status, false).await;
    let p2 = insert_tx(&storage, user_id, P2, "unproven", true).await;
    let x = insert_tx(&storage, user_id, X, "unproven", true).await;
    let ch = insert_tx(&storage, user_id, CH, "unproven", true).await;

    let g0 = insert_output(&storage, user_id, basket, g, G, 0, 30_000, false, Some(p2)).await;
    let p2_0 = insert_output(&storage, user_id, basket, p2, P2, 0, 29_000, false, Some(x)).await;
    let x0 = insert_output(&storage, user_id, basket, x, X, 0, 28_000, false, Some(ch)).await;
    let ch0 = insert_output(&storage, user_id, basket, ch, CH, 0, 27_000, true, None).await;
    for txid in [P2, X, CH] {
        insert_req(&storage, txid, "unmined").await;
    }
    (storage, ClimbIds { g0, p2_0, x0, ch0 })
}

/// A status service that knows only `known` (as mined).
fn chain_knows(known: &[&str]) -> MockWalletServices {
    MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            error: None,
            results: known
                .iter()
                .map(|txid| TxStatusDetail {
                    txid: txid.to_string(),
                    status: "mined".to_string(),
                    depth: Some(3),
                })
                .collect(),
        }))
        .build()
}

#[tokio::test]
async fn chain_knowledge_reads_the_status_service() {
    let services = chain_knows(&[G]);
    assert_eq!(
        bsv_wallet_toolbox_rs::chain_knowledge(&services, G).await,
        ChainKnowledge::Mined
    );
    assert_eq!(
        bsv_wallet_toolbox_rs::chain_knowledge(&services, X).await,
        ChainKnowledge::Unknown
    );
    let down = MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "down".to_string(),
        ))
        .build();
    assert_eq!(
        bsv_wallet_toolbox_rs::chain_knowledge(&down, G).await,
        ChainKnowledge::Unavailable
    );
}

#[tokio::test]
async fn the_climb_stops_at_the_first_chain_known_parent() {
    // G is still unproven in the wallet (no proof fetched): only the status
    // service can tell the climb it is on chain.
    let (storage, ids) = seed_climb("unproven").await;
    let services = chain_knows(&[G]);

    let (root, climbed) = storage.poisoned_root_of(&services, CH).await.unwrap();
    assert_eq!(root, P2, "P2 is the topmost absent ancestor");
    assert_eq!(climbed, vec![CH.to_string(), X.to_string()]);
    // The chain-known parent was recorded as chain evidence on the way.
    assert_eq!(
        memory_status(&storage, G, BROADCAST_PROVIDER_CHAIN)
            .await
            .as_deref(),
        Some(BROADCAST_STATUS_MINED)
    );
    // Nothing was touched by the climb.
    assert_eq!(tx_status(&storage, P2).await, "unproven");

    let report = storage
        .retire_poisoned_chain_from(&services, CH, "invalid", true)
        .await
        .unwrap();
    assert_eq!(report.outcome, PoisonOutcome::Retired);
    assert_eq!(report.root, P2);
    assert_eq!(report.origin, CH);
    assert_eq!(report.climbed, vec![CH.to_string(), X.to_string()]);
    assert_eq!(report.retirable_txids(), vec![P2, X, CH]);
    assert_eq!(report.failed, 3);
    // G's coin (the only outside input of the set) is back.
    assert_eq!((report.restored, report.restored_sats), (1, 30_000));
    assert_eq!(output_state(&storage, ids.g0).await, (1, None));
    // The inside coins stay dead.
    assert_eq!(output_state(&storage, ids.p2_0).await.0, 0);
    assert_eq!(output_state(&storage, ids.x0).await.0, 0);
    assert_eq!(output_state(&storage, ids.ch0).await.0, 0);
    assert_eq!(
        tx_status(&storage, G).await,
        "unproven",
        "not part of the poison"
    );
    for txid in [P2, X, CH] {
        assert_eq!(tx_status(&storage, txid).await, "failed", "{}", txid);
    }
}

#[tokio::test]
async fn the_climb_never_moves_on_a_silent_status_service() {
    let (storage, _ids) = seed_climb("unproven").await;
    let down = MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "down".to_string(),
        ))
        .build();
    let (root, climbed) = storage.poisoned_root_of(&down, CH).await.unwrap();
    assert_eq!(root, CH, "silence is not absence: no climb");
    assert!(climbed.is_empty());
}

#[tokio::test]
async fn a_kept_locked_input_is_rechecked_and_restored_on_a_later_pass() {
    let s = seed("unproven").await;
    // The UTXO lookup is rate-limited during the retire: P:0 stays locked.
    let limited = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "429".to_string(),
        ))
        .build();
    let report = s
        .storage
        .retire_poisoned_chain(&limited, R, "invalid", true)
        .await
        .unwrap();
    assert_eq!((report.restored, report.kept), (0, 1));
    assert_eq!(output_state(&s.storage, s.ids.p0).await.0, 0);
    assert_eq!(s.storage.locked_inputs_pending().await.unwrap(), 1);
    let next = s
        .storage
        .locked_input_next_check(s.ids.p0)
        .await
        .unwrap()
        .expect("scheduled");
    let wait = (next - chrono::Utc::now()).num_seconds();
    assert!(
        (30..=90).contains(&wait),
        "first re-check in a minute: {}s",
        wait
    );

    // Not due yet: the next pass leaves it alone.
    let early = s
        .storage
        .recheck_locked_inputs(&MockWalletServices::new(), 20, true)
        .await
        .unwrap();
    assert_eq!(early.due, 0);
    assert_eq!(output_state(&s.storage, s.ids.p0).await.0, 0);

    // Due, and the chain vouches for it now: restored.
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minute')")
        .execute(s.storage.pool())
        .await
        .unwrap();
    let dry = s
        .storage
        .recheck_locked_inputs(&MockWalletServices::new(), 20, false)
        .await
        .unwrap();
    assert_eq!(dry.due, 1);
    assert_eq!(dry.checks[0].verdict, LockedInputVerdict::Restored);
    assert!(!dry.executed);
    assert_eq!(
        output_state(&s.storage, s.ids.p0).await.0,
        0,
        "dry run touches nothing"
    );

    let wet = s
        .storage
        .recheck_locked_inputs(&MockWalletServices::new(), 20, true)
        .await
        .unwrap();
    assert!(wet.executed);
    assert_eq!((wet.restored, wet.restored_sats), (1, 50_000));
    assert_eq!(wet.checks[0].output_id, s.ids.p0);
    assert_eq!(wet.checks[0].source_txid, P);
    assert_eq!(wet.checks[0].locked_by, C1);
    assert_eq!(output_state(&s.storage, s.ids.p0).await, (1, None));
    assert_eq!(s.storage.locked_inputs_pending().await.unwrap(), 0);
}

#[tokio::test]
async fn a_spent_locked_input_is_left_locked_and_never_rechecked_again() {
    let s = seed("unproven").await;
    let limited = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "429".to_string(),
        ))
        .build();
    s.storage
        .retire_poisoned_chain(&limited, R, "invalid", true)
        .await
        .unwrap();
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minute')")
        .execute(s.storage.pool())
        .await
        .unwrap();

    // Not in the unspent set, and the source is on chain: spent for real.
    let spent = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Success(GetUtxoStatusResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            is_utxo: Some(false),
            details: vec![],
            error: None,
        }))
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: P.to_string(),
                status: "mined".to_string(),
                depth: Some(10),
            }],
        }))
        .build();
    let report = s
        .storage
        .recheck_locked_inputs(&spent, 20, true)
        .await
        .unwrap();
    assert_eq!(report.spent, 1);
    assert_eq!(report.checks[0].verdict, LockedInputVerdict::Spent);
    let (spendable, spent_by) = output_state(&s.storage, s.ids.p0).await;
    assert_eq!(spendable, 0);
    assert!(spent_by.is_some(), "left locked");
    assert_eq!(
        s.storage.locked_inputs_pending().await.unwrap(),
        0,
        "terminal"
    );
    let again = s
        .storage
        .recheck_locked_inputs(&spent, 20, true)
        .await
        .unwrap();
    assert_eq!(again.due, 0);
}

#[tokio::test]
async fn an_undecided_recheck_backs_off_and_an_unknown_source_is_not_a_spend() {
    let s = seed("unproven").await;
    let limited = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "429".to_string(),
        ))
        .build();
    s.storage
        .retire_poisoned_chain(&limited, R, "invalid", true)
        .await
        .unwrap();
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minute')")
        .execute(s.storage.pool())
        .await
        .unwrap();

    // Still rate-limited: second attempt, two minutes of backoff.
    let report = s
        .storage
        .recheck_locked_inputs(&limited, 20, true)
        .await
        .unwrap();
    assert_eq!(report.unknown, 1);
    assert_eq!(report.checks[0].verdict, LockedInputVerdict::Unknown);
    assert_eq!(report.checks[0].attempts, 2);
    assert_eq!(report.checks[0].next_check_minutes, Some(2));
    let next = s
        .storage
        .locked_input_next_check(s.ids.p0)
        .await
        .unwrap()
        .expect("still scheduled");
    let wait = (next - chrono::Utc::now()).num_seconds();
    assert!((90..=150).contains(&wait), "two minutes: {}s", wait);
    assert_eq!(s.storage.locked_inputs_pending().await.unwrap(), 1);

    // Not in the unspent set but the source is unknown to the chain: not a
    // spend, keep re-checking.
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minute')")
        .execute(s.storage.pool())
        .await
        .unwrap();
    let unknown_source = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Success(GetUtxoStatusResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            is_utxo: Some(false),
            details: vec![],
            error: None,
        }))
        .build();
    let report = s
        .storage
        .recheck_locked_inputs(&unknown_source, 20, true)
        .await
        .unwrap();
    assert_eq!(report.checks[0].verdict, LockedInputVerdict::Unknown);
    assert_eq!(report.checks[0].attempts, 3);
    assert_eq!(report.checks[0].next_check_minutes, Some(4));
    assert_eq!(output_state(&s.storage, s.ids.p0).await.0, 0);
}

#[tokio::test]
async fn adoption_finds_locked_inputs_of_failed_transactions_that_predate_the_table() {
    let s = seed("unproven").await;
    // C1 failed by some older path, its inputs still locked, no rows.
    sqlx::query("UPDATE transactions SET status = 'failed' WHERE txid = ?")
        .bind(C1)
        .execute(s.storage.pool())
        .await
        .unwrap();
    assert_eq!(s.storage.locked_inputs_pending().await.unwrap(), 0);

    // P:0 unspent (restored); R:0 not in the unspent set and R unknown to
    // the chain (kept, backoff).
    let services = MockWalletServices::builder()
        .get_utxo_status_response(MockResponse::Sequence(vec![
            MockResponse::Success(GetUtxoStatusResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                is_utxo: Some(true),
                details: vec![],
                error: None,
            }),
            MockResponse::Success(GetUtxoStatusResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                is_utxo: Some(false),
                details: vec![],
                error: None,
            }),
        ]))
        .build();
    let report = s
        .storage
        .recheck_locked_inputs(&services, 20, true)
        .await
        .unwrap();
    assert_eq!(report.adopted, 2, "P:0 and R:0");
    assert_eq!(report.due, 2);
    assert_eq!((report.restored, report.unknown), (1, 1));
    assert_eq!(output_state(&s.storage, s.ids.p0).await, (1, None));
    assert_eq!(output_state(&s.storage, s.ids.r0).await.0, 0);

    // Once R is retired as a phantom, its coin is dropped from the checks.
    sqlx::query("UPDATE transactions SET status = 'failed' WHERE txid = ?")
        .bind(R)
        .execute(s.storage.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE locked_input_checks SET next_check_at = datetime('now', '-1 minute')")
        .execute(s.storage.pool())
        .await
        .unwrap();
    let report = s
        .storage
        .recheck_locked_inputs(&services, 20, true)
        .await
        .unwrap();
    assert_eq!(report.checks[0].verdict, LockedInputVerdict::Phantom);
    assert_eq!(s.storage.locked_inputs_pending().await.unwrap(), 0);
}

#[tokio::test]
async fn migration_003_creates_the_locked_input_table_on_open() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    let version = storage
        .migrate("m003", &("02".to_string() + &"ab".repeat(32)))
        .await
        .unwrap();
    assert_eq!(version, "003_locked_input_checks");
    let table: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'locked_input_checks'",
    )
    .fetch_optional(storage.pool())
    .await
    .unwrap();
    assert!(table.is_some());
    // Dropped (a database from before 0.3.59) and re-created on make_available.
    sqlx::query("DROP TABLE locked_input_checks")
        .execute(storage.pool())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let table: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'locked_input_checks'",
    )
    .fetch_optional(storage.pool())
    .await
    .unwrap();
    assert!(table.is_some());
}

#[tokio::test]
async fn a_proven_parent_needs_no_lookup_to_stop_the_climb() {
    let (storage, ids) = seed_climb("completed").await;
    // The status service knows nothing at all; a completed parent is on
    // chain by definition and is never asked about.
    let services = MockWalletServices::new();
    let (root, climbed) = storage.poisoned_root_of(&services, CH).await.unwrap();
    assert_eq!(root, P2);
    assert_eq!(climbed, vec![CH.to_string(), X.to_string()]);
    assert_eq!(
        services.call_count("get_status_for_txids"),
        2,
        "X and P2 only"
    );
    let report = storage
        .retire_poisoned_chain_from(&services, CH, "invalid", true)
        .await
        .unwrap();
    assert_eq!(report.retirable_txids(), vec![P2, X, CH]);
    assert_eq!(output_state(&storage, ids.g0).await, (1, None));
    assert_eq!(tx_status(&storage, G).await, "completed");
}
