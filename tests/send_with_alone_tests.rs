//! `createAction` with a `sendWith` and no inputs and no outputs builds
//! nothing: the named transactions are sent, and the answer carries one
//! `sendWithResults` entry a named txid.
//!
//! The payer's sequence is the reference client's (ts-stack 57c5f130,
//! `packages/sdk/src/auth/clients/AuthFetch.ts`): call 1 a `createAction`
//! with the payment's output, `noSend: true` and `acceptDelayedBroadcast:
//! false`; call 2 a `createAction` with `sendWith: [txid]` and
//! `acceptDelayedBroadcast: false` alone, of which it requires exactly one
//! entry, of that txid, reading `unproven`.
//!
//! The real `Wallet` over an in-memory `StorageSqlx`, the real signer, a
//! mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

#[path = "common/sendwith_seed.rs"]
mod seed;

use std::sync::{Arc, Mutex};

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    AbortActionArgs, CreateActionArgs, CreateActionOptions, CreateActionOutput, CreateActionResult,
    ProtoWallet, SendWithResult, SignActionArgs, SignActionOptions, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    Error, PermissionRequest, StorageSqlx, Wallet, WalletPermissionsManager,
    WalletPermissionsManagerConfig, WalletStorageManager, WalletStorageProvider,
    WalletStorageWriter,
};

const ORIGIN: &str = "test.local";
const SIK: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const PAYEE: &str = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";

type W<S = StorageSqlx> = Wallet<S, MockWalletServices>;

async fn store() -> StorageSqlx {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("send-with-alone", SIK).await.unwrap();
    store.make_available().await.unwrap();
    store
}

fn accepting() -> MockWalletServices {
    MockWalletServices::builder().post_beef_success().build()
}

/// A wallet of `root` on `store` holding `coins` proven coins of its own.
async fn wallet_on(
    store: &StorageSqlx,
    root: &PrivateKey,
    coins: u32,
    services: MockWalletServices,
) -> W {
    let identity = root.public_key().to_hex();
    let (user, _) = store.find_or_insert_user(&identity).await.unwrap();
    seed::seed_coins(store, root, user.user_id, coins, 100_000).await;
    Wallet::new(Some(root.clone()), store.clone(), services)
        .await
        .unwrap()
}

async fn payer(coins: u32) -> W {
    wallet_on(&store().await, &PrivateKey::random(), coins, accepting()).await
}

fn payment_output() -> CreateActionOutput {
    CreateActionOutput {
        locking_script: hex::decode(PAYEE).unwrap(),
        satoshis: 1_000,
        output_description: "HTTP request payment".to_string(),
        basket: None,
        custom_instructions: None,
        tags: None,
    }
}

fn action(
    outputs: Option<Vec<CreateActionOutput>>,
    options: CreateActionOptions,
) -> CreateActionArgs {
    CreateActionArgs {
        description: "an action of the payer".to_string(),
        input_beef: None,
        inputs: None,
        outputs,
        lock_time: None,
        version: None,
        labels: None,
        options: Some(options),
    }
}

/// Call 1's arguments, as the reference payer builds them.
fn prepare() -> CreateActionArgs {
    action(
        Some(vec![payment_output()]),
        CreateActionOptions {
            randomize_outputs: Some(false),
            no_send: Some(true),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        },
    )
}

fn naming(txids: &[[u8; 32]], delayed: bool) -> CreateActionOptions {
    CreateActionOptions {
        send_with: Some(txids.to_vec()),
        accept_delayed_broadcast: Some(delayed),
        ..Default::default()
    }
}

/// Call 2's arguments, as the reference payer builds them: a description
/// and two options, no inputs, no outputs, no labels.
fn submit(txid: [u8; 32]) -> CreateActionArgs {
    action(None, naming(&[txid], false))
}

/// Call 1 through `wallet`: the prepared payment's txid.
async fn prepared<S>(wallet: &W<S>) -> [u8; 32]
where
    S: WalletStorageProvider + Send + Sync + 'static,
{
    let made = wallet
        .create_action(prepare(), Some(ORIGIN))
        .await
        .expect("call 1");
    made.txid.expect("call 1's txid")
}

/// (transaction statuses, request statuses) of `txid`.
async fn words(store: &StorageSqlx, txid: &[u8; 32]) -> (Vec<String>, Vec<String>) {
    let txid = hex::encode(txid);
    let tx = sqlx::query_scalar::<_, String>("SELECT status FROM transactions WHERE txid = ?")
        .bind(&txid)
        .fetch_all(store.pool())
        .await
        .unwrap();
    let req = sqlx::query_scalar::<_, String>("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(&txid)
        .fetch_all(store.pool())
        .await
        .unwrap();
    (tx, req)
}

fn word(tx: &str, req: &str) -> (Vec<String>, Vec<String>) {
    (vec![tx.to_string()], vec![req.to_string()])
}

/// (transaction rows, output rows, coins taken).
async fn counts(store: &StorageSqlx) -> (i64, i64, i64) {
    let one = |sql: &'static str| async move {
        sqlx::query_scalar::<_, i64>(sql)
            .fetch_one(store.pool())
            .await
            .unwrap()
    };
    (
        one("SELECT COUNT(*) FROM transactions").await,
        one("SELECT COUNT(*) FROM outputs").await,
        one("SELECT COUNT(*) FROM outputs WHERE spent_by IS NOT NULL").await,
    )
}

/// The arguments of each `post_beef` the mock broadcaster was asked.
fn posts<S>(wallet: &W<S>) -> Vec<String>
where
    S: WalletStorageProvider + Send + Sync + 'static,
{
    wallet
        .services()
        .call_history()
        .into_iter()
        .filter(|c| c.method == "post_beef")
        .map(|c| c.args.join(" "))
        .collect()
}

/// `sendWithResults` as `(txid, word)` pairs.
fn entries(result: &CreateActionResult) -> Vec<(String, String)> {
    listed(result.send_with_results.as_deref())
}

fn listed(results: Option<&[SendWithResult]>) -> Vec<(String, String)> {
    results
        .unwrap_or_default()
        .iter()
        .map(|r| (hex::encode(r.txid), format!("{:?}", r.status)))
        .collect()
}

fn entry(txid: &[u8; 32], word: &str) -> (String, String) {
    (hex::encode(txid), word.to_string())
}

/// The answer of a call that built nothing carries `sendWithResults` alone.
fn assert_nothing_made(result: &CreateActionResult) {
    assert!(result.txid.is_none(), "no txid of a transaction not made");
    assert!(result.tx.is_none(), "no tx of a transaction not made");
    assert!(result.no_send_change.is_none(), "no noSendChange");
    assert!(result.signable_transaction.is_none(), "nothing to sign");
    assert!(result.beef.is_none(), "no beef");
}

/// The payer's two calls on a wallet of `coins` coins.
async fn the_payers_sequence(coins: u32) {
    let wallet = payer(coins).await;
    let txid = prepared(&wallet).await;
    assert_eq!(
        words(wallet.storage(), &txid).await,
        word("nosend", "nosend")
    );
    assert!(posts(&wallet).is_empty(), "call 1 posts nothing");
    let before = counts(wallet.storage()).await;

    let answer = wallet
        .create_action(submit(txid), Some(ORIGIN))
        .await
        .expect("call 2 is accepted");

    assert_eq!(
        entries(&answer),
        vec![entry(&txid, "Unproven")],
        "exactly one entry, of the prepared txid, reading unproven"
    );
    assert_nothing_made(&answer);
    assert_eq!(
        counts(wallet.storage()).await,
        before,
        "call 2 adds no transaction row, no output row and takes no coin"
    );
    assert_eq!(
        words(wallet.storage(), &txid).await,
        word("unproven", "unmined")
    );
    let posted = posts(&wallet);
    assert_eq!(posted.len(), 1, "one post: {posted:?}");
    assert!(
        posted[0].contains(&format!("txids=[\"{}\"]", hex::encode(txid))),
        "exactly the payment: {posted:?}"
    );
}

#[tokio::test]
async fn the_payers_two_calls_send_exactly_the_payment() {
    the_payers_sequence(3).await;
}

#[tokio::test]
async fn the_payers_two_calls_with_no_spare_coin_send_exactly_the_payment() {
    the_payers_sequence(1).await;
}

/// `abortAction` by txid between the calls: call 2 answers the txid not
/// sent, nothing is posted and the action stays aborted.
#[tokio::test]
async fn a_payment_aborted_between_the_calls_is_not_sent() {
    let wallet = payer(3).await;
    let txid = prepared(&wallet).await;
    let aborted = wallet
        .abort_action(
            AbortActionArgs {
                reference: hex::encode(txid),
            },
            Some(ORIGIN),
        )
        .await
        .expect("the abort");
    assert!(aborted.aborted);
    let before = counts(wallet.storage()).await;

    let answer = wallet
        .create_action(submit(txid), Some(ORIGIN))
        .await
        .expect("call 2 answers");

    assert_eq!(entries(&answer), vec![entry(&txid, "Failed")]);
    assert_nothing_made(&answer);
    assert_eq!(counts(wallet.storage()).await, before);
    assert_eq!(
        words(wallet.storage(), &txid).await,
        word("failed", "invalid")
    );
    assert!(posts(&wallet).is_empty(), "nothing posted");
}

/// One cell of the call: the options beside the named txid, the
/// broadcaster's answer, and what is read back.
async fn cell(
    services: MockWalletServices,
    options: impl FnOnce([u8; 32]) -> CreateActionOptions,
) -> (Vec<String>, (Vec<String>, Vec<String>), usize) {
    let wallet = wallet_on(&store().await, &PrivateKey::random(), 3, services).await;
    let txid = prepared(&wallet).await;
    let before = counts(wallet.storage()).await;
    let answer = wallet
        .create_action(action(None, options(txid)), Some(ORIGIN))
        .await
        .expect("the call answers");
    assert_nothing_made(&answer);
    assert_eq!(counts(wallet.storage()).await, before, "nothing built");
    let read = entries(&answer);
    assert!(read.iter().all(|(t, _)| *t == hex::encode(txid)));
    (
        read.into_iter().map(|(_, w)| w).collect(),
        words(wallet.storage(), &txid).await,
        posts(&wallet).len(),
    )
}

fn refusing(kind: MockErrorKind, text: &str) -> MockWalletServices {
    MockWalletServices::builder()
        .post_beef_response(MockResponse::Error(kind, text.to_string()))
        .build()
}

/// The cells of the call beside the payer's: each answers as the stored
/// post answers (`Wallet::process_action`), building nothing.
#[tokio::test]
async fn the_cells_of_a_send_with_alone() {
    let now = |txid| naming(&[txid], false);

    // The broadcaster refuses, answers a fault, or times out: the entry
    // reads sending, the transaction waits to be sent again.
    for (kind, text) in [
        (MockErrorKind::BroadcastFailed, "rejected"),
        (
            MockErrorKind::ServiceError,
            "Service temporarily unavailable (503)",
        ),
        (MockErrorKind::NetworkError, "Request timed out"),
    ] {
        let got = cell(refusing(kind, text), now).await;
        assert_eq!(
            got,
            (vec!["Sending".to_string()], word("sending", "unsent"), 1),
            "{text}"
        );
    }

    // acceptDelayedBroadcast: true: queued, nothing posted by the call.
    let got = cell(accepting(), |txid| naming(&[txid], true)).await;
    assert_eq!(
        got,
        (vec!["Sending".to_string()], word("sending", "unsent"), 0),
        "delayed"
    );

    // noSend: true beside the sendWith: the named transaction is sent.
    let got = cell(accepting(), |txid| CreateActionOptions {
        no_send: Some(true),
        ..naming(&[txid], false)
    })
    .await;
    assert_eq!(
        got,
        (vec!["Unproven".to_string()], word("unproven", "unmined"), 1),
        "noSend beside sendWith"
    );

    // signAndProcess: false beside it: no signable transaction, sent.
    let got = cell(accepting(), |txid| CreateActionOptions {
        sign_and_process: Some(false),
        ..naming(&[txid], false)
    })
    .await;
    assert_eq!(
        got,
        (vec!["Unproven".to_string()], word("unproven", "unmined"), 1),
        "signAndProcess false beside sendWith"
    );
}

/// The empty call with no `sendWith` stays a new transaction (the
/// reference's remix of change).
#[tokio::test]
async fn the_empty_call_with_no_send_with_stays_a_new_transaction() {
    let wallet = payer(3).await;
    let before = counts(wallet.storage()).await;
    let answer = wallet
        .create_action(action(None, CreateActionOptions::default()), Some(ORIGIN))
        .await
        .expect("the remix");
    assert!(answer.txid.is_some(), "a transaction of its own");
    let after = counts(wallet.storage()).await;
    assert_eq!(after.0, before.0 + 1, "one transaction row added");
}

/// Two wallets on one store: `a` the caller, `b` another user.
async fn two_users(a_services: MockWalletServices) -> (W, W) {
    let store = store().await;
    // The store posts through the services of the wallet made last: `a`'s.
    let b = wallet_on(&store, &PrivateKey::random(), 3, accepting()).await;
    let a = wallet_on(&store, &PrivateKey::random(), 3, a_services).await;
    (a, b)
}

async fn abort<S>(wallet: &W<S>, txid: &[u8; 32])
where
    S: WalletStorageProvider + Send + Sync + 'static,
{
    wallet
        .abort_action(
            AbortActionArgs {
                reference: hex::encode(txid),
            },
            Some(ORIGIN),
        )
        .await
        .expect("the abort");
}

/// The named txid of each cell of the rule, on a fresh pair of users:
/// the cell's name, the caller's wallet, the txid and the store's words
/// for it before the call.
async fn named_cell(name: &str) -> (W, [u8; 32]) {
    let (a, b) = two_users(accepting()).await;
    let txid = match name {
        "another user's" => prepared(&b).await,
        "aborted" => {
            let txid = prepared(&a).await;
            abort(&a, &txid).await;
            txid
        }
        "unknown" => [0x5a; 32],
        "already unproven" => {
            let txid = prepared(&a).await;
            a.create_action(submit(txid), Some(ORIGIN)).await.unwrap();
            txid
        }
        "sending" => {
            let txid = prepared(&a).await;
            a.create_action(action(None, naming(&[txid], true)), Some(ORIGIN))
                .await
                .unwrap();
            txid
        }
        _ => unreachable!(),
    };
    (a, txid)
}

/// The rule's cells with the txid named alone: sent only when it is a
/// `noSend` transaction of the caller's own that was not aborted.
#[tokio::test]
async fn a_named_txid_alone_is_sent_only_when_it_is_the_callers_no_send_action() {
    for (name, answer, after, posted) in [
        ("another user's", "Failed", word("nosend", "nosend"), 0),
        ("aborted", "Failed", word("failed", "invalid"), 0),
        ("unknown", "Failed", (vec![], vec![]), 0),
        (
            "already unproven",
            "Unproven",
            word("unproven", "unmined"),
            0,
        ),
        ("sending", "Unproven", word("unproven", "unmined"), 1),
    ] {
        let (a, txid) = named_cell(name).await;
        let before = (counts(a.storage()).await, posts(&a).len());
        let got = a
            .create_action(submit(txid), Some(ORIGIN))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(entries(&got), vec![entry(&txid, answer)], "{name}");
        assert_nothing_made(&got);
        assert_eq!(counts(a.storage()).await, before.0, "{name}: nothing built");
        assert_eq!(words(a.storage(), &txid).await, after, "{name}");
        assert_eq!(posts(&a).len() - before.1, posted, "{name}: posts");
    }
}

/// The rule's cells with the txid named beside a new action: the new
/// action is made and sent; the named txid is judged alone.
#[tokio::test]
async fn a_named_txid_beside_a_new_action_is_sent_only_when_it_is_the_callers_no_send_action() {
    for (name, answer, after) in [
        ("another user's", "Failed", word("nosend", "nosend")),
        ("aborted", "Failed", word("failed", "invalid")),
        ("unknown", "Failed", (vec![], vec![])),
        ("already unproven", "Unproven", word("unproven", "unmined")),
        ("sending", "Sending", word("sending", "unsent")),
    ] {
        let (a, txid) = named_cell(name).await;
        let got = a
            .create_action(
                action(Some(vec![payment_output()]), naming(&[txid], false)),
                Some(ORIGIN),
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let own = got.txid.expect("the new action's txid");
        assert_eq!(
            entries(&got),
            vec![entry(&txid, answer), entry(&own, "Unproven")],
            "{name}"
        );
        assert_eq!(words(a.storage(), &txid).await, after, "{name}");
        let posted = posts(&a);
        let last = posted.last().expect("the new action's post");
        assert!(
            last.contains(&format!("txids=[\"{}\"]", hex::encode(own))),
            "{name}: the new action alone is posted: {last}"
        );
    }
}

/// Two named txids of which one is not the caller's, beside a new action:
/// the caller's own is sent with the action in one post (`unproven`), the
/// other is answered not sent and nothing of it is written.
#[tokio::test]
async fn of_two_named_txids_beside_a_new_action_the_one_not_the_callers_is_not_sent() {
    let (a, b) = two_users(accepting()).await;
    let mine = prepared(&a).await;
    let theirs = prepared(&b).await;
    let got = a
        .create_action(
            action(Some(vec![payment_output()]), naming(&[mine, theirs], false)),
            Some(ORIGIN),
        )
        .await
        .expect("the action");
    let own = got.txid.expect("the new action's txid");
    assert_eq!(
        entries(&got),
        vec![
            entry(&mine, "Unproven"),
            entry(&theirs, "Failed"),
            entry(&own, "Unproven")
        ]
    );
    assert_eq!(words(a.storage(), &theirs).await, word("nosend", "nosend"));
    assert_eq!(words(a.storage(), &mine).await, word("unproven", "unmined"));
    let last = posts(&a).pop().expect("the one post");
    let both = format!("txids={:?}", [hex::encode(mine), hex::encode(own)]);
    assert!(last.contains(&both), "mine with the action: {last}");
    assert!(
        posts(&a).iter().all(|p| !p.contains(&hex::encode(theirs))),
        "nothing of the other user's is posted"
    );
}

/// Two named txids of which one is not the caller's, named alone: the
/// stored post sends a set whole or not at all, so neither is sent, and
/// nothing is written.
#[tokio::test]
async fn of_two_named_txids_alone_the_one_not_the_callers_is_not_sent() {
    let (a, b) = two_users(accepting()).await;
    let mine = prepared(&a).await;
    let theirs = prepared(&b).await;
    let got = a
        .create_action(action(None, naming(&[mine, theirs], false)), Some(ORIGIN))
        .await
        .expect("the call answers");
    assert_eq!(
        entries(&got),
        vec![entry(&mine, "Failed"), entry(&theirs, "Failed")]
    );
    assert_eq!(words(a.storage(), &theirs).await, word("nosend", "nosend"));
    assert_eq!(words(a.storage(), &mine).await, word("nosend", "nosend"));
    assert!(posts(&a).is_empty(), "nothing posted");
}

/// The payer's sequence through a `WalletStorageManager` over the store.
#[tokio::test(flavor = "multi_thread")]
async fn the_payers_two_calls_through_a_storage_manager() {
    let store = Arc::new(store().await);
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    let (user, _) = store.find_or_insert_user(&identity).await.unwrap();
    seed::seed_coins(&store, &root, user.user_id, 1, 100_000).await;
    let manager = WalletStorageManager::new(identity, Some(store.clone()), None);
    manager.make_available().await.unwrap();
    // The manager's `set_services` blocks, so the wallet is made on a
    // thread of its own, inside the runtime's context.
    let handle = tokio::runtime::Handle::current();
    let wallet = std::thread::spawn(move || {
        let _entered = handle.enter();
        futures::executor::block_on(Wallet::new(Some(root), manager, accepting())).unwrap()
    })
    .join()
    .unwrap();

    let txid = prepared(&wallet).await;
    let before = counts(&store).await;
    let answer = wallet
        .create_action(submit(txid), Some(ORIGIN))
        .await
        .expect("call 2");

    assert_eq!(entries(&answer), vec![entry(&txid, "Unproven")]);
    assert_nothing_made(&answer);
    assert_eq!(counts(&store).await, before);
    assert_eq!(words(&store, &txid).await, word("unproven", "unmined"));
    assert_eq!(posts(&wallet).len(), 1);
}

/// Call 2 through a wallet whose permissions manager refuses every
/// request: the call spends nothing new, so nothing is asked and it is
/// not refused.
#[tokio::test]
async fn a_permissions_manager_is_asked_nothing_for_a_send_with_alone() {
    let root = PrivateKey::random();
    let mut wallet = wallet_on(&store().await, &root, 1, accepting()).await;
    let txid = prepared(&wallet).await;

    let manager = Arc::new(WalletPermissionsManager::new(
        Arc::new(ProtoWallet::new(Some(root.clone()))),
        "admin.local".to_string(),
        WalletPermissionsManagerConfig::all_enabled(),
    ));
    let asked: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&asked);
    manager
        .set_permission_request_handler(Arc::new(move |r: PermissionRequest| {
            seen.lock().unwrap().push(format!("{r:?}"));
            Box::pin(async move { Err(Error::AccessDenied("refused by the user".into())) })
        }))
        .await;
    wallet.set_permissions_manager(manager);
    let before = counts(wallet.storage()).await;

    let answer = wallet
        .create_action(submit(txid), Some(ORIGIN))
        .await
        .expect("call 2 is not refused");

    assert_eq!(entries(&answer), vec![entry(&txid, "Unproven")]);
    assert_nothing_made(&answer);
    assert_eq!(counts(wallet.storage()).await, before);
    assert!(asked.lock().unwrap().is_empty(), "nothing asked: {asked:?}");
}

/// Call 2 through a `StorageClient`: the wallet sends the remote store one
/// `processAction` naming the txid, not a new transaction, and no
/// `createAction`.
#[cfg(feature = "remote")]
#[tokio::test]
async fn a_send_with_alone_through_a_storage_client_is_one_process_action() {
    use bsv_wallet_toolbox_rs::StorageClient;
    use serde_json::{json, Value};

    let root = PrivateKey::random();
    let txid = [0x7c; 32];
    let seen: Arc<Mutex<Vec<Value>>> = Arc::default();
    let mut server = mockito::Server::new_async().await;
    let mut mocks = Vec::new();
    for method in ["findOrInsertUser", "processAction"] {
        let seen = Arc::clone(&seen);
        let mock = server
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
                    seen.lock().unwrap().push(body["params"][1].clone());
                    json!({"sendWithResults": [
                        {"txid": hex::encode([0x7c; 32]), "status": "unproven"}
                    ]})
                };
                serde_json::to_vec(&json!({"jsonrpc": "2.0", "id": body["id"], "result": result}))
                    .unwrap()
            })
            .create_async()
            .await;
        mocks.push(mock);
    }
    let storage =
        StorageClient::new_unauthenticated(ProtoWallet::new(Some(root.clone())), server.url());
    let wallet = Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap();

    let answer = wallet
        .create_action(submit(txid), Some(ORIGIN))
        .await
        .expect("call 2 over the wire");

    assert_eq!(entries(&answer), vec![entry(&txid, "Unproven")]);
    assert_nothing_made(&answer);
    let sent = seen.lock().unwrap().clone();
    assert_eq!(sent.len(), 1, "one processAction: {sent:?}");
    assert_eq!(sent[0]["isNewTx"], json!(false));
    assert_eq!(sent[0]["isSendWith"], json!(true));
    assert_eq!(sent[0]["isNoSend"], json!(false));
    assert_eq!(sent[0]["isDelayed"], json!(false));
    assert_eq!(sent[0]["sendWith"], json!([hex::encode(txid)]));
}

/// A deferred action with nothing of the caller's (`signAndProcess: false`,
/// no inputs, no outputs, no `sendWith`) signed with `sendWith` and
/// `acceptDelayedBroadcast`: (the action's txid, the answer's entries).
async fn signed_deferred(
    wallet: &W,
    send_with: Vec<[u8; 32]>,
    delayed: bool,
) -> ([u8; 32], Vec<(String, String)>) {
    use base64::Engine;
    let deferred = action(
        None,
        CreateActionOptions {
            sign_and_process: Some(false),
            ..Default::default()
        },
    );
    let made = wallet.create_action(deferred, Some(ORIGIN)).await.unwrap();
    let reference = made.signable_transaction.unwrap().reference;
    let options = SignActionOptions {
        send_with: Some(send_with),
        accept_delayed_broadcast: Some(delayed),
        ..Default::default()
    };
    let args = SignActionArgs {
        spends: std::collections::HashMap::new(),
        reference: base64::engine::general_purpose::STANDARD.encode(reference),
        options: Some(options),
    };
    let answer = wallet.sign_action(args, Some(ORIGIN)).await.unwrap();
    let got = listed(answer.send_with_results.as_deref());
    (answer.txid.unwrap(), got)
}

/// Each post names exactly `txid`.
fn posted_alone(wallet: &W, txid: &[u8; 32], count: usize) {
    let posted = posts(wallet);
    assert_eq!(posted.len(), count, "{posted:?}");
    let alone = format!("txids=[\"{}\"]", hex::encode(txid));
    assert!(posted.iter().all(|p| p.contains(&alone)), "{posted:?}");
}

/// Signed with `acceptDelayedBroadcast: true`: the action and the named
/// payment are queued together and nothing is posted by the call.
#[tokio::test]
async fn a_deferred_action_signed_delayed_with_a_named_payment_queues_both() {
    let (a, _b) = two_users(accepting()).await;
    let payment = prepared(&a).await;
    let (own, got) = signed_deferred(&a, vec![payment], true).await;
    assert_eq!(
        got,
        vec![entry(&payment, "Sending"), entry(&own, "Sending")]
    );
    assert_eq!(
        words(a.storage(), &payment).await,
        word("sending", "unsent")
    );
    assert_eq!(words(a.storage(), &own).await, word("sending", "unsent"));
    posted_alone(&a, &own, 0);
}

/// Signed with `sendWith: []`: the action alone is posted and the prepared
/// payment stays `nosend`.
#[tokio::test]
async fn a_deferred_action_signed_with_an_empty_send_with_posts_the_action_alone() {
    let (a, _b) = two_users(accepting()).await;
    let payment = prepared(&a).await;
    let (own, got) = signed_deferred(&a, vec![], false).await;
    assert_eq!(got, vec![entry(&own, "Unproven")]);
    assert_eq!(words(a.storage(), &own).await, word("unproven", "unmined"));
    assert_eq!(words(a.storage(), &payment).await, word("nosend", "nosend"));
    posted_alone(&a, &own, 1);
}

/// A txid named at the sign that is unknown to the store or is not a
/// transaction of the caller's: its entry reads failed, nothing is written
/// for it, and the action alone is posted.
#[tokio::test]
async fn a_txid_named_at_the_sign_that_is_not_the_callers_reads_failed_and_is_not_sent() {
    for name in ["unknown", "another user's"] {
        let (a, b) = two_users(accepting()).await;
        let (named, before) = match name {
            "unknown" => ([0x5a; 32], (vec![], vec![])),
            _ => (prepared(&b).await, word("nosend", "nosend")),
        };
        let (own, got) = signed_deferred(&a, vec![named], false).await;
        assert_eq!(
            got,
            vec![entry(&named, "Failed"), entry(&own, "Unproven")],
            "{name}"
        );
        assert_eq!(words(a.storage(), &named).await, before, "{name}");
        assert_eq!(
            words(a.storage(), &own).await,
            word("unproven", "unmined"),
            "{name}"
        );
        posted_alone(&a, &own, 1);
    }
}
