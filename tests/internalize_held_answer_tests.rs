//! The internalize call's answer when no broadcaster takes the payment (the
//! owner's ruling of 2026-11-17, DECISIONS 29.1, and the keeper's readings
//! 1 and 10 of section 30).
//!
//! A checked proof, or a take by at least one named instance: `accepted:
//! true`. Taken by nobody: the transaction stays held (`sending`), nothing
//! is undone, and the call answers the review-actions refusal
//! (`WalletReviewActions`, wire code 5) with a send result `sending` for
//! the txid (never `failed`) and a review result whose status is
//! `doubleSpend` when any refusal named a competitor (the competitors in
//! `competing_txs`), `invalidTx` only when every named instance said no,
//! otherwise `serviceError`. Its message is the reference's sentence, then
//! each instance's words. The same answer through a `StorageClient` and a
//! `WalletStorageManager`.
//!
//! Real SQLite storage (a mockito double for the remote storage), a mock
//! broadcaster and tracker; nothing is sent.

#![cfg(all(feature = "sqlite", feature = "remote"))]

#[path = "common/internalize_payment.rs"]
mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, InternalizeActionArgs,
    InternalizeActionResult, ListActionsArgs, ListOutputsArgs, ProtoWallet,
    ReviewActionResultStatus as Review, ReviewActionsRefusal, SendWithResultStatus as Send,
    WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::storage::{
    AuthId, ReviewActionResultStatus as StorageReview, WalletStorageProvider,
};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageClient, StorageSqlx, Wallet, WalletStorageManager,
    WalletStorageWriter,
};
use serde_json::{json, Value};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "aGVsZC1hbnN3ZXI=";
const ORIGIN: &str = "test.local";
const LABEL: &str = "heldanswer";
const SENTENCE: &str = "Undelayed createAction or signAction results require review.";
const RIVAL: &str = "cc00000000000000000000000000000000000000000000000000000000000001";

fn word(
    name: &str,
    txid: &str,
    status: &str,
    data: &str,
    competitor: Option<&str>,
) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: status.to_string(),
            double_spend: competitor.is_some(),
            orphan_mempool: false,
            competing_txs: competitor.map(|c| vec![c.to_string()]),
            data: Some(data.to_string()),
            service_error: status == "serviceError",
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

fn no_answer() -> MockResponse<Vec<PostBeefResult>> {
    MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into())
}

/// A paying BEEF to `root`'s BRC-29 key, its proven twin, the arguments
/// for each and the txid.
fn paying(
    root: &PrivateKey,
    suffix: &str,
) -> (InternalizeActionArgs, InternalizeActionArgs, String) {
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(root, &sender, PREFIX, suffix);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap();
    let (beef, txid) = common::beef_paying(lock.clone());
    let (proven, _) = common::proven_beef_paying(lock);
    let mut args = common::args(beef, common::payment(&sender, PREFIX, suffix));
    args.labels = Some(vec![LABEL.to_string()]);
    let mut proven_args = args.clone();
    proven_args.tx = proven;
    (args, proven_args, txid)
}

fn refusal(answer: bsv_rs::Result<InternalizeActionResult>) -> ReviewActionsRefusal {
    match answer {
        Err(bsv_rs::Error::WalletReviewActions(refusal)) => *refusal,
        other => panic!("expected the review-actions refusal, got {other:?}"),
    }
}

async fn tx_status(wallet: &TestWallet, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

async fn history(wallet: &TestWallet, txid: &str) -> String {
    sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

/// The held answer's shape: the txid, one send result `sending`, one
/// review result with `status`, the message opening with the sentence.
fn assert_held(refusal: &ReviewActionsRefusal, txid: &str, status: Review) {
    let bytes = hex::decode(txid).unwrap();
    assert_eq!(refusal.txid.map(|t| t.to_vec()), Some(bytes.clone()));
    assert_eq!(refusal.send_with_results.len(), 1);
    assert_eq!(refusal.send_with_results[0].status, Send::Sending);
    assert_eq!(refusal.send_with_results[0].txid.to_vec(), bytes);
    assert_eq!(refusal.review_action_results.len(), 1);
    assert_eq!(refusal.review_action_results[0].status, status);
    assert!(refusal.message.starts_with(SENTENCE), "{}", refusal.message);
}

/// Internalize with the door's post answered by `post`; the answer, the
/// wallet, the txid.
async fn held(
    post: MockResponse<Vec<PostBeefResult>>,
    suffix: &str,
) -> (bsv_rs::Result<InternalizeActionResult>, TestWallet, String) {
    let root = PrivateKey::random();
    let (args, _, txid) = paying(&root, suffix);
    let wallet = common::wallet_posting("held-answer", &root, post).await;
    let answer = wallet.internalize_action(args, Some(ORIGIN)).await;
    (answer, wallet, txid)
}

#[tokio::test]
async fn internalize_no_door_answered_is_review_actions_sending_service_error() {
    let (answer, wallet, txid) = held(no_answer(), "bm9uZQ==").await;
    let r = refusal(answer);
    println!(
        "no instance answered: {:?} {:?} message {:?}",
        r.review_action_results, r.send_with_results, r.message
    );
    assert_held(&r, &txid, Review::ServiceError);
    assert!(r.message.contains("no instance answered"), "{}", r.message);
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
}

#[tokio::test]
async fn internalize_every_door_said_no_names_each_reason_and_the_competitor_still_sending() {
    let root = PrivateKey::random();
    let (args, _, txid) = paying(&root, "cml2YWw=");
    let post = MockResponse::Success(vec![
        word(
            "arcade",
            &txid,
            "error",
            "DOUBLE_SPEND_ATTEMPTED",
            Some(RIVAL),
        ),
        word(
            "bitails",
            &txid,
            "rejected",
            "bitails refuses the body",
            None,
        ),
    ]);
    let wallet = common::wallet_posting("held-answer", &root, post).await;
    let r = refusal(wallet.internalize_action(args, Some(ORIGIN)).await);
    println!(
        "every no, one rival: {:?} message {:?}",
        r.review_action_results, r.message
    );
    assert_held(&r, &txid, Review::DoubleSpend);
    assert_eq!(
        r.review_action_results[0]
            .competing_txs
            .as_ref()
            .map(|c| c.iter().map(hex::encode).collect::<Vec<_>>()),
        Some(vec![RIVAL.to_string()])
    );
    for words in [
        "arcade: error [DOUBLE_SPEND_ATTEMPTED]",
        "bitails: rejected [bitails refuses the body]",
    ] {
        assert!(r.message.contains(words), "message: {}", r.message);
        assert!(history(&wallet, &txid).await.contains(words), "history");
    }
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
    assert_eq!(wallet.services().call_count("post_beef"), 1);
}

#[tokio::test]
async fn internalize_every_door_said_no_and_none_named_a_competitor_is_invalid_tx() {
    let (answer, wallet, txid) = held(
        MockResponse::Success(vec![
            word("arcade", &"00".repeat(32), "rejected", "no", None),
            word("bitails", &"00".repeat(32), "465", "fee too low", None),
        ]),
        "aW52YWxpZA==",
    )
    .await;
    let r = refusal(answer);
    assert_held(&r, &txid, Review::InvalidTx);
    assert!(r.review_action_results[0].competing_txs.is_none());
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
}

#[tokio::test]
async fn internalize_some_said_no_and_some_did_not_answer_is_service_error() {
    let (answer, wallet, txid) = held(
        MockResponse::Success(vec![
            word("arcade", &"00".repeat(32), "rejected", "no", None),
            word("bitails", &"00".repeat(32), "serviceError", "timeout", None),
        ]),
        "c29tZQ==",
    )
    .await;
    let r = refusal(answer);
    assert_held(&r, &txid, Review::ServiceError);
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
}

#[tokio::test]
async fn internalize_one_door_took_it_is_accepted() {
    let root = PrivateKey::random();
    let (args, _, txid) = paying(&root, "dG9vaw==");
    let post = MockResponse::Success(vec![
        common::accepted(&txid),
        word("bitails", &txid, "rejected", "no", None),
    ]);
    let wallet = common::wallet_posting("held-answer", &root, post).await;
    let answer = wallet.internalize_action(args, Some(ORIGIN)).await;
    assert!(answer.expect("accepted").accepted);
    assert_eq!(tx_status(&wallet, &txid).await, "unproven");
}

#[tokio::test]
async fn internalize_with_a_checked_proof_is_accepted_with_no_post() {
    let root = PrivateKey::random();
    let (_, proven, txid) = paying(&root, "cHJvb2Y=");
    let wallet = common::wallet_posting("held-answer", &root, no_answer()).await;
    let answer = wallet.internalize_action(proven, Some(ORIGIN)).await;
    assert!(answer.expect("accepted").accepted);
    let posts = wallet.services().call_count("post_beef");
    println!(
        "checked proof: status {} posts {posts}",
        tx_status(&wallet, &txid).await
    );
    // The proof was checked; the proof gate defers storing a proof this
    // near the mock tip, so the row is `unproven` with an `unmined`
    // request, never posted (`src/storage/sqlx/CLAUDE.md`).
    assert_eq!(
        (tx_status(&wallet, &txid).await.as_str(), posts),
        ("unproven", 0)
    );
}

/// The same storage answer through a `WalletStorageManager` over the store:
/// the two fields travel. (A `Wallet` over the manager is not built here:
/// the manager's synchronous `set_services` takes a blocking lock that
/// `Wallet::new`, an async constructor, cannot take on a runtime thread.)
#[tokio::test]
async fn internalize_held_through_a_storage_manager_is_the_same_answer() {
    let root = PrivateKey::random();
    let (args, _, txid) = paying(&root, "bWFuYWdlcg==");
    let store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    let identity = root.public_key().to_hex();
    store.migrate("held-answer", &identity).await.unwrap();
    store.make_available().await.unwrap();
    let services = Arc::new(
        MockWalletServices::builder()
            .post_beef_response(no_answer())
            .build(),
    );
    store.set_services(services.clone());
    let (user, _) = store.find_or_insert_user(&identity).await.unwrap();
    let manager = WalletStorageManager::new(identity.clone(), Some(store.clone()), None);
    manager.make_available().await.unwrap();
    let auth = AuthId::with_user_id(&identity, user.user_id);
    let result = manager.internalize_action(&auth, args).await.unwrap();
    println!(
        "through a manager: send {:?} review {:?} posts {}",
        result.send_with_results,
        result.not_delayed_results,
        services.call_count("post_beef")
    );
    let send = result.send_with_results.expect("send results");
    let review = result.not_delayed_results.expect("review results");
    assert_eq!(
        (send[0].txid.as_str(), send[0].status.as_str()),
        (txid.as_str(), "sending")
    );
    assert_eq!(review[0].status, StorageReview::ServiceError);
    assert_eq!(services.call_count("post_beef"), 1);
}

/// A JSON-RPC answer of the remote storage double to `method`
/// (`findOrInsertUser` answers the asked identity as user 1).
async fn rpc(
    server: &mut mockito::ServerGuard,
    method: &'static str,
    result: Value,
) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(json!({ "method": method })))
        .with_body_from_request(move |request| {
            let body: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            let result = if method == "findOrInsertUser" {
                json!({"isNew": false, "user": {
                    "userId": 1, "identityKey": body["params"][0], "activeStorage": null,
                    "createdAt": "2026-01-02T03:04:05Z", "updatedAt": "2026-01-02T03:04:05Z"
                }})
            } else {
                result.clone()
            };
            serde_json::to_vec(&json!({"jsonrpc": "2.0", "id": body["id"], "result": result}))
                .unwrap()
        })
        .create_async()
        .await
}

/// The same answer through a `StorageClient`: the remote store's result
/// carries `sendWithResults` and `notDelayedResults`, and the wallet raises
/// from them (its message the sentence alone where the store serves no
/// history).
#[tokio::test]
async fn internalize_held_through_a_storage_client_is_the_same_answer() {
    let root = PrivateKey::random();
    let (args, _, txid) = paying(&root, "Y2xpZW50");
    let mut server = mockito::Server::new_async().await;
    let _user = rpc(&mut server, "findOrInsertUser", Value::Null).await;
    let _internalize = rpc(
        &mut server,
        "internalizeAction",
        json!({
            "accepted": true, "isMerge": false, "txid": txid, "satoshis": 4500,
            "sendWithResults": [{"txid": txid, "status": "sending"}],
            "notDelayedResults": [{"txid": txid, "status": "doubleSpend",
                "competingTxs": [RIVAL], "competingBeef": null}]
        }),
    )
    .await;
    let _reqs = rpc(&mut server, "findProvenTxReqs", json!([])).await;
    let storage =
        StorageClient::new_unauthenticated(ProtoWallet::new(Some(root.clone())), server.url());
    let wallet = Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap();
    let r = refusal(wallet.internalize_action(args, Some(ORIGIN)).await);
    println!(
        "through a client: {:?} message {:?}",
        r.review_action_results, r.message
    );
    assert_held(&r, &txid, Review::DoubleSpend);
    assert_eq!(r.message, SENTENCE);
}

fn spend_under_the_label() -> CreateActionArgs {
    CreateActionArgs {
        description: "a spend".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: common::other_lock().to_binary(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["underReceivedRule".to_string()]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(true),
            ..Default::default()
        }),
    }
}

/// Item 2 of the piece: after the refusal everything the held word wrote
/// stays true (the transaction `sending`, out of `listOutputs` and the
/// balance, offered by no rule, `listActions` lists it `sending`). A caller
/// that ignores the refusal and calls again makes a repeat: TODAY'S answer,
/// pinned and not changed here (the repeat piece's to change), is the merge
/// path's `accepted: true` with no post and the row still `sending`.
#[tokio::test]
async fn after_the_refusal_the_held_rows_read_as_held_and_a_repeat_answers_todays_merge() {
    let root = PrivateKey::random();
    let (args, _, txid) = paying(&root, "cmVhZA==");
    let wallet = common::wallet_posting("held-answer", &root, no_answer()).await;
    let answer = wallet.internalize_action(args.clone(), Some(ORIGIN)).await;
    assert_held(&refusal(answer), &txid, Review::ServiceError);
    let listed = wallet
        .list_outputs(
            ListOutputsArgs {
                basket: "default".to_string(),
                tags: None,
                tag_query_mode: None,
                include: None,
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: None,
                offset: None,
                seek_permission: None,
            },
            Some(ORIGIN),
        )
        .await
        .unwrap()
        .total_outputs;
    let balance = wallet.balance().await.unwrap().total;
    let offered = wallet
        .create_action(spend_under_the_label(), Some(ORIGIN))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    let actions = wallet
        .list_actions(
            ListActionsArgs {
                labels: vec![LABEL.to_string()],
                label_query_mode: None,
                include_labels: None,
                include_inputs: None,
                include_input_source_locking_scripts: None,
                include_input_unlocking_scripts: None,
                include_outputs: None,
                include_output_locking_scripts: None,
                limit: None,
                offset: None,
                seek_permission: None,
            },
            Some(ORIGIN),
        )
        .await
        .unwrap();
    let status = actions
        .actions
        .iter()
        .find(|a| hex::encode(a.txid) == txid)
        .map(|a| format!("{:?}", a.status));
    println!(
        "after the refusal: tx {} listed {listed} balance {balance} offered {offered:?} listActions {status:?}",
        tx_status(&wallet, &txid).await
    );
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
    assert_eq!((listed, balance), (0, 0));
    assert!(offered.unwrap_err().contains("Insufficient funds"));
    assert_eq!(status.as_deref(), Some("Sending"));

    // The caller ignores the refusal and presents the same body again.
    let again = wallet.internalize_action(args, Some(ORIGIN)).await;
    let again = common::answer_word(again, "the repeat");
    let posts = wallet.services().call_count("post_beef");
    println!(
        "the repeat: answer {again} posts {posts} tx {}",
        tx_status(&wallet, &txid).await
    );
    assert_eq!(
        again, "accepted",
        "today's merge path; the repeat piece's to change"
    );
    assert_eq!(posts, 1, "the repeat posts nothing");
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
}
