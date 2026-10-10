//! A refusal on the immediate post is a hint (bsv-stack-lean #66, the
//! immediate paths): `createAction`, `signAction` and `internalizeAction`
//! answered a 465, any other refusal or a double-spend word write no
//! `failed` and no `invalid`, return no error for the word alone, give the
//! caller the txid with BRC-100's `sending`, queue a named competitor for a
//! proof ask, and leave the inputs locked for the monitor's re-ask on the
//! send-waiting cadence. A status source holding the transaction on a later
//! pass promotes it as send-waiting promotes. A request's `attempts` is 1
//! once `process_action` queues it and 2 after a post that drew no
//! accepting word.
//!
//! Real SQLite storage, the real signer, a mock broadcaster; random
//! throwaway keys; nothing touches the network.

#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    BasketInsertion, Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel,
    SendWithResultStatus, SignActionArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    AuthId, GetStatusForTxidsResult, MonitorStorage, PostBeefResult, PostTxResultForTxid,
    StorageSqlx, TxStatusDetail, Wallet, WalletServices, WalletStorageProvider,
    WalletStorageWriter,
};

const BRC29_PROTOCOL: &str = "3241645161d8";
const PREFIX: &str = "dGVzdC1wcmVmaXg=";
const SUFFIX: &str = "dGVzdC1zdWZmaXg=";
const FUNDING_SATS: u64 = 100_000;

fn p2pkh_lock(pubkey: &[u8]) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(pubkey));
    script.extend([0x88, 0xac]);
    script
}

fn pay_lock() -> Vec<u8> {
    hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// A storage holding one completed parent with one P2PKH change output the
/// wallet can sign for.
async fn seed() -> (StorageSqlx, PrivateKey, String) {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("immediate-hint-tests", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("basket");

    let proto = ProtoWallet::new(Some(root.clone()));
    let sk = proto
        .key_deriver()
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, BRC29_PROTOCOL),
            &format!("{} {}", PREFIX, SUFFIX),
            &Counterparty::Self_,
        )
        .expect("derive funding key");
    let funding_lock = p2pkh_lock(&sk.public_key().to_compressed());
    let parent = Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some("00".repeat(32)),
            source_output_index: 0xffff_ffff,
            unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
            unlocking_script_template: None,
            sequence: 0xffff_ffff,
        }],
        vec![TransactionOutput {
            satoshis: Some(FUNDING_SATS),
            locking_script: LockingScript::from_binary(&funding_lock).unwrap(),
            change: false,
        }],
        0,
    );
    let parent_txid = parent.id();
    let now = chrono::Utc::now();
    let parent_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?, 'completed', 'parent-ref', 0, ?, 1, 0, 'seeded parent', ?, ?, ?, ?)",
    )
    .bind(user.user_id)
    .bind(FUNDING_SATS as i64)
    .bind(&parent_txid)
    .bind(parent.to_binary())
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("insert parent")
    .last_insert_rowid();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, \
                              txid, type, spendable, change, derivation_prefix, derivation_suffix, \
                              provided_by, purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, ?, ?, 'storage', 'change', 'wallet funding', ?, ?)",
    )
    .bind(user.user_id)
    .bind(parent_id)
    .bind(basket.basket_id)
    .bind(FUNDING_SATS as i64)
    .bind(&funding_lock)
    .bind(&parent_txid)
    .bind(PREFIX)
    .bind(SUFFIX)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("insert funding output");
    (storage, root, parent_txid)
}

fn pay_args(sign_and_process: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "pay someone".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: pay_lock(),
            satoshis: 1_000,
            output_description: "payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(sign_and_process),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    }
}

/// One broadcaster's word on the post.
fn word(status: &str, double_spend: bool, competitor: Option<&str>) -> PostBeefResult {
    PostBeefResult {
        name: "arc".to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: "ab".repeat(32),
            status: status.to_string(),
            double_spend,
            orphan_mempool: false,
            competing_txs: competitor.map(|c| vec![c.to_string()]),
            data: Some(format!("ARC answered {status}")),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

fn posting(result: PostBeefResult) -> MockWalletServices {
    MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![result]))
        .build()
}

/// A status source holding `txid` in a mempool.
fn holding(txid: &str, result: PostBeefResult) -> MockWalletServices {
    MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![result]))
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "mock".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: txid.to_string(),
                status: "known".to_string(),
                depth: None,
                merkle_path: None,
                block_height: None,
                block_hash: None,
            }],
        }))
        .build()
}

/// (tx status, req status, req attempts) for `txid`.
async fn words(storage: &StorageSqlx, txid: &str) -> (String, String, i64) {
    let t: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let (r, a): (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    (t, r, a)
}

async fn history(storage: &StorageSqlx, txid: &str) -> serde_json::Value {
    let h: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    serde_json::from_str(&h).unwrap()
}

async fn queued(storage: &StorageSqlx, txid: &str) -> Vec<String> {
    history(storage, txid).await["competitorsQueued"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The funding coin is locked by `txid`, and `txid`'s own outputs are not
/// written off.
async fn assert_inputs_locked_and_change_kept(storage: &StorageSqlx, parent: &str, txid: &str) {
    let (spendable, spent_by): (bool, Option<String>) = sqlx::query_as(
        "SELECT o.spendable, s.txid FROM outputs o LEFT JOIN transactions s ON s.transaction_id = o.spent_by \
         WHERE o.txid = ? AND o.vout = 0",
    )
    .bind(parent)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(
        (spendable, spent_by.as_deref()),
        (false, Some(txid)),
        "the input stays locked by the transaction"
    );
    let written_off: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outputs WHERE txid = ? AND change = 1 AND spendable = 0",
    )
    .bind(txid)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(written_off, 0, "the change is not written off");
}

async fn no_failed_word(storage: &StorageSqlx) {
    let (failed, judged): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM transactions WHERE status = 'failed'), \
                (SELECT COUNT(*) FROM proven_tx_reqs WHERE status IN ('invalid', 'doubleSpend'))",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(
        (failed, judged),
        (0, 0),
        "no failed, no invalid, no doubleSpend"
    );
}

fn note_is_immediate_hint(h: &serde_json::Value, outcome: &str) -> bool {
    h["notes"].as_array().is_some_and(|notes| {
        notes.iter().any(|n| {
            n["what"] == "immediateBroadcastHint"
                && n["outcome"] == outcome
                && n["nextReaskMinutes"].is_number()
        })
    })
}

/// RED at `32a494d`: the 465 took the release rule (`failed`, req
/// `invalid`, the funding coin released, the change written off) and
/// `createAction` returned "Transaction broadcast failed". GREEN: the txid
/// with `sending`, no word, the competitor queued, the input locked.
#[tokio::test]
async fn create_action_answered_465_naming_a_competitor_is_a_hint_not_an_error() {
    let (storage, root, parent) = seed().await;
    let competitor = "cd".repeat(32);
    let wallet = Wallet::new(
        Some(root),
        storage,
        posting(word("465", false, Some(&competitor))),
    )
    .await
    .expect("wallet");

    let result = wallet
        .create_action(pay_args(true), "test.local")
        .await
        .expect("a broadcaster's refusal is not an error to the caller");
    let txid = hex::encode(result.txid.expect("the txid"));
    let swr = result.send_with_results.expect("sendWithResults");
    assert_eq!(swr.len(), 1);
    assert!(
        matches!(swr[0].status, SendWithResultStatus::Sending),
        "BRC-100's word for a transaction not yet proven: {:?}",
        swr[0].status
    );
    assert_eq!(hex::encode(swr[0].txid), txid);

    let storage = wallet.storage();
    assert_eq!(
        words(storage, &txid).await,
        ("sending".to_string(), "unsent".to_string(), 2)
    );
    no_failed_word(storage).await;
    assert_inputs_locked_and_change_kept(storage, &parent, &txid).await;
    assert_eq!(queued(storage, &txid).await, vec![competitor]);
    assert!(note_is_immediate_hint(
        &history(storage, &txid).await,
        "doubleSpend"
    ));
}

/// RED at `32a494d`: a double-spend word naming no competitor (Arcade's
/// DOUBLE_SPEND_ATTEMPTED) failed the transaction (`doubleSpend`) and
/// returned an error. GREEN: no word, `sending`, nothing queued, and the
/// monitor's re-ask waits for the cadence and then runs.
#[tokio::test]
async fn create_action_answered_a_double_spend_word_naming_no_competitor_schedules_the_reask() {
    let (storage, root, parent) = seed().await;
    let wallet = Wallet::new(Some(root), storage, posting(word("rejected", true, None)))
        .await
        .expect("wallet");

    let result = wallet
        .create_action(pay_args(true), "test.local")
        .await
        .expect("a double-spend word is not an error to the caller");
    let txid = hex::encode(result.txid.expect("the txid"));
    assert!(matches!(
        result.send_with_results.expect("sendWithResults")[0].status,
        SendWithResultStatus::Sending
    ));

    let storage = wallet.storage();
    assert_eq!(
        words(storage, &txid).await,
        ("sending".to_string(), "unsent".to_string(), 2)
    );
    no_failed_word(storage).await;
    assert_inputs_locked_and_change_kept(storage, &parent, &txid).await;
    assert!(queued(storage, &txid).await.is_empty());
    assert!(note_is_immediate_hint(
        &history(storage, &txid).await,
        "doubleSpend"
    ));

    // Inside the cadence the monitor posts nothing; past it, it re-asks.
    assert_eq!(wallet.services().call_count("post_beef"), 1);
    storage
        .send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(
        wallet.services().call_count("post_beef"),
        1,
        "the re-ask waits for the cadence"
    );
    sqlx::query("UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ?")
        .bind(chrono::Utc::now() - chrono::Duration::days(1))
        .bind(&txid)
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(
        wallet.services().call_count("post_beef"),
        2,
        "the re-ask runs"
    );
    assert_eq!(
        words(storage, &txid).await,
        ("sending".to_string(), "unsent".to_string(), 3)
    );
    no_failed_word(storage).await;
}

/// RED at `32a494d`: `createAction` returned the 465 as an error and the
/// transaction was `failed`, beyond the reach of a later status source.
/// GREEN: on the monitor's next pass, a status source that holds the
/// transaction promotes it (`unproven` / `unmined`), as send-waiting
/// promotes.
#[tokio::test]
async fn a_status_source_holding_it_on_the_next_pass_promotes_it() {
    let (storage, root, parent) = seed().await;
    let wallet = Wallet::new(Some(root), storage, posting(word("465", false, None)))
        .await
        .expect("wallet");
    let result = wallet
        .create_action(pay_args(true), "test.local")
        .await
        .expect("a refusal is not an error");
    let txid = hex::encode(result.txid.expect("the txid"));
    let storage = wallet.storage();
    assert_eq!(
        words(storage, &txid).await,
        ("sending".to_string(), "unsent".to_string(), 2)
    );

    // The next pass: the broadcaster still refuses, a status source holds it.
    let next = Arc::new(holding(&txid, word("465", false, None)));
    WalletStorageProvider::set_services(storage, next.clone() as Arc<dyn WalletServices>);
    sqlx::query("UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ?")
        .bind(chrono::Utc::now() - chrono::Duration::days(1))
        .bind(&txid)
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(next.call_count("post_beef"), 1);
    assert_eq!(
        words(storage, &txid).await,
        ("unproven".to_string(), "unmined".to_string(), 2)
    );
    no_failed_word(storage).await;
    assert_inputs_locked_and_change_kept(storage, &parent, &txid).await;
}

/// RED at `32a494d`: a fault reaching the broadcasters on the immediate
/// post left the request `sending` with no word recorded (the monitor's
/// paths record it since 0.7.3). GREEN: the same hint as the monitor's: the
/// request `unsent`, the fault on its history, the re-ask on the cadence.
#[tokio::test]
async fn create_action_whose_post_reaches_no_broadcaster_is_the_same_hint() {
    let (storage, root, parent) = seed().await;
    let mock = MockWalletServices::builder()
        .post_beef_network_error("connection refused")
        .build();
    let wallet = Wallet::new(Some(root), storage, mock)
        .await
        .expect("wallet");
    let result = wallet
        .create_action(pay_args(true), "test.local")
        .await
        .expect("a transport fault is not an error to the caller");
    let txid = hex::encode(result.txid.expect("the txid"));
    assert!(matches!(
        result.send_with_results.expect("sendWithResults")[0].status,
        SendWithResultStatus::Sending
    ));
    let storage = wallet.storage();
    assert_eq!(
        words(storage, &txid).await,
        ("sending".to_string(), "unsent".to_string(), 2)
    );
    no_failed_word(storage).await;
    assert_inputs_locked_and_change_kept(storage, &parent, &txid).await;
    assert!(note_is_immediate_hint(
        &history(storage, &txid).await,
        "serviceError"
    ));
}

/// RED at `32a494d`: `signAction`'s post answered 465 failed the
/// transaction and returned an error. GREEN: as `createAction`.
#[tokio::test]
async fn sign_action_answered_465_is_a_hint_not_an_error() {
    let (storage, root, parent) = seed().await;
    let competitor = "ef".repeat(32);
    let wallet = Wallet::new(
        Some(root),
        storage,
        posting(word("465", false, Some(&competitor))),
    )
    .await
    .expect("wallet");

    let created = wallet
        .create_action(pay_args(false), "test.local")
        .await
        .expect("a signable transaction");
    let reference = created
        .signable_transaction
        .expect("signable transaction")
        .reference;
    let signed = wallet
        .sign_action(
            SignActionArgs {
                spends: Default::default(),
                reference: String::from_utf8(reference).expect("the reference"),
                options: None,
            },
            "test.local",
        )
        .await
        .expect("a broadcaster's refusal is not an error to the caller");
    let txid = hex::encode(signed.txid.expect("the txid"));

    let storage = wallet.storage();
    assert_eq!(
        words(storage, &txid).await,
        ("sending".to_string(), "unsent".to_string(), 2)
    );
    no_failed_word(storage).await;
    assert_inputs_locked_and_change_kept(storage, &parent, &txid).await;
    assert_eq!(queued(storage, &txid).await, vec![competitor]);
}

/// An AtomicBEEF of one transaction paying 10,000 sats to an unrelated
/// P2PKH, spending an outpoint the storage does not know.
fn incoming_atomic_beef() -> (Vec<u8>, String) {
    let mut tx = Transaction::new();
    tx.version = 1;
    let mut input = TransactionInput::new("11".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_binary(&pay_lock()).unwrap(),
        change: false,
    });
    let txid = tx.id();
    let mut beef = Beef::new();
    beef.merge_transaction(tx);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// RED at `32a494d`: `internalizeAction`'s post answered 465 marked the
/// transaction `failed`, its outputs unspendable, its req `invalid`, and
/// returned "rejected by network". GREEN: accepted, no word, the req left
/// `unsent` for the re-ask with the word on its history, the competitor
/// queued, the outputs kept.
#[tokio::test]
async fn internalize_action_answered_465_is_a_hint_not_an_error() {
    let (storage, root, _) = seed().await;
    let identity = root.public_key().to_hex();
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let competitor = "a1".repeat(32);
    let mock = Arc::new(posting(word("465", false, Some(&competitor))));
    WalletStorageProvider::set_services(&storage, mock.clone() as Arc<dyn WalletServices>);

    let (beef, txid) = incoming_atomic_beef();
    let auth = AuthId {
        identity_key: identity,
        user_id: Some(user.user_id),
        is_active: None,
    };
    let result = storage
        .internalize_action(
            &auth,
            InternalizeActionArgs {
                tx: beef,
                outputs: vec![InternalizeOutput {
                    output_index: 0,
                    protocol: "basket insertion".to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(BasketInsertion {
                        basket: "incoming".to_string(),
                        custom_instructions: None,
                        tags: None,
                    }),
                }],
                description: "an incoming transaction".to_string(),
                labels: None,
                seek_permission: None,
            },
        )
        .await
        .expect("a broadcaster's refusal is not an error to the caller");
    assert!(result.base.accepted);
    assert_eq!(mock.call_count("post_beef"), 1);

    let (t, r, a) = words(&storage, &txid).await;
    assert_ne!(t, "failed", "no failed word");
    assert_eq!((r.as_str(), a), ("unsent", 1), "re-asked on the cadence");
    no_failed_word(&storage).await;
    assert_eq!(queued(&storage, &txid).await, vec![competitor]);
    assert!(note_is_immediate_hint(
        &history(&storage, &txid).await,
        "invalidTx"
    ));
    let kept: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outputs WHERE txid = ? AND spendable = 1")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(kept, 1, "the internalized output is not written off");
}
