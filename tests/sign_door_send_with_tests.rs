//! `signAction` with a `sendWith` naming the caller's prepared `noSend`
//! payment posts the action and the payment in one post, as `createAction`
//! with `signAndProcess` does beside a new action: the reference's storage
//! `processAction` shares `[...sendWith, txid]` in one `postBeef` (ts-stack
//! 57c5f130 `storage/methods/processAction.ts` 124, 148;
//! `attemptToPostReqsToNetwork.ts` 51) and answers both `unproven` when a
//! broadcaster takes them.
//!
//! The real `Wallet` over an in-memory `StorageSqlx`, the real signer, a
//! mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

#[path = "common/sendwith_seed.rs"]
mod seed;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, SendWithResult, SignActionArgs,
    SignActionOptions, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

const ORIGIN: &str = "test.local";
const SIK: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const PAYEE: &str = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";

type W = Wallet<StorageSqlx, MockWalletServices>;

/// A store, a user of `root` on it holding three proven coins.
async fn store_of(root: &PrivateKey) -> StorageSqlx {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("sign-door-send-with", SIK).await.unwrap();
    store.make_available().await.unwrap();
    let (user, _) = store
        .find_or_insert_user(&root.public_key().to_hex())
        .await
        .unwrap();
    seed::seed_coins(&store, root, user.user_id, 3, 100_000).await;
    store
}

async fn wallet(store: &StorageSqlx, root: &PrivateKey, services: MockWalletServices) -> W {
    Wallet::new(Some(root.clone()), store.clone(), services)
        .await
        .unwrap()
}

fn accepting() -> MockWalletServices {
    MockWalletServices::builder().post_beef_success().build()
}

fn refusing(kind: MockErrorKind, text: &str) -> MockWalletServices {
    MockWalletServices::builder()
        .post_beef_response(MockResponse::Error(kind, text.to_string()))
        .build()
}

fn args(
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

/// The payer's prepared payment: a `noSend` action of the caller's.
async fn prepared(wallet: &W) -> [u8; 32] {
    let options = CreateActionOptions {
        randomize_outputs: Some(false),
        no_send: Some(true),
        accept_delayed_broadcast: Some(false),
        ..Default::default()
    };
    let made = wallet
        .create_action(args(Some(vec![payment_output()]), options), Some(ORIGIN))
        .await
        .expect("the prepared payment");
    made.txid.expect("its txid")
}

/// A deferred action with nothing of the caller's, signed with `sendWith`
/// and `acceptDelayedBroadcast`: (the action's txid, the answer's entries).
async fn signed_deferred(
    wallet: &W,
    send_with: Vec<[u8; 32]>,
    delayed: bool,
) -> ([u8; 32], Vec<(String, String)>) {
    use base64::Engine;
    let deferred = CreateActionOptions {
        sign_and_process: Some(false),
        ..Default::default()
    };
    let made = wallet
        .create_action(args(None, deferred), Some(ORIGIN))
        .await
        .unwrap();
    let reference = made.signable_transaction.unwrap().reference;
    let sign = SignActionArgs {
        spends: std::collections::HashMap::new(),
        reference: base64::engine::general_purpose::STANDARD.encode(reference),
        options: Some(SignActionOptions {
            send_with: Some(send_with),
            accept_delayed_broadcast: Some(delayed),
            ..Default::default()
        }),
    };
    let answer = wallet.sign_action(sign, Some(ORIGIN)).await.unwrap();
    (
        answer.txid.unwrap(),
        listed(answer.send_with_results.as_deref()),
    )
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

/// (transaction statuses, request statuses) of `txid`.
async fn words(store: &StorageSqlx, txid: &[u8; 32]) -> (Vec<String>, Vec<String>) {
    let txid = hex::encode(txid);
    let read = |sql: &'static str| {
        let txid = txid.clone();
        async move {
            sqlx::query_scalar::<_, String>(sql)
                .bind(txid)
                .fetch_all(store.pool())
                .await
                .unwrap()
        }
    };
    (
        read("SELECT status FROM transactions WHERE txid = ?").await,
        read("SELECT status FROM proven_tx_reqs WHERE txid = ?").await,
    )
}

fn word(tx: &str, req: &str) -> (Vec<String>, Vec<String>) {
    (vec![tx.to_string()], vec![req.to_string()])
}

/// The txids named by each `post_beef` the wallet's broadcaster was asked.
fn posts(wallet: &W) -> Vec<String> {
    wallet
        .services()
        .call_history()
        .into_iter()
        .filter(|c| c.method == "post_beef")
        .map(|c| c.args.join(" "))
        .collect()
}

/// Exactly one post since `before` posts, naming `txids` in that order.
fn one_post_naming(wallet: &W, before: usize, txids: &[[u8; 32]]) {
    let posted = posts(wallet);
    assert_eq!(posted.len(), before + 1, "{posted:?}");
    let named: Vec<String> = txids.iter().map(hex::encode).collect();
    let expected = format!("txids={named:?}");
    assert!(posted[before].contains(&expected), "{posted:?}");
}

/// S1: the sign door posts the action and the named payment in one post;
/// both read `unproven`, nothing is left queued for the monitor.
#[tokio::test]
async fn a_deferred_action_signed_with_a_named_payment_posts_both_in_one_post() {
    let root = PrivateKey::random();
    let store = store_of(&root).await;
    let a = wallet(&store, &root, accepting()).await;
    let payment = prepared(&a).await;
    let (own, got) = signed_deferred(&a, vec![payment], false).await;
    assert_eq!(
        got,
        vec![entry(&payment, "Unproven"), entry(&own, "Unproven")]
    );
    assert_eq!(words(&store, &payment).await, word("unproven", "unmined"));
    assert_eq!(words(&store, &own).await, word("unproven", "unmined"));
    one_post_naming(&a, 0, &[payment, own]);
}

/// The create door beside a new action: the same one post, both `unproven`.
#[tokio::test]
async fn a_new_action_with_a_named_payment_posts_both_in_one_post() {
    let root = PrivateKey::random();
    let store = store_of(&root).await;
    let a = wallet(&store, &root, accepting()).await;
    let payment = prepared(&a).await;
    let options = CreateActionOptions {
        send_with: Some(vec![payment]),
        ..Default::default()
    };
    let made = a
        .create_action(args(Some(vec![payment_output()]), options), Some(ORIGIN))
        .await
        .expect("the action");
    let own = made.txid.expect("its txid");
    assert_eq!(
        listed(made.send_with_results.as_deref()),
        vec![entry(&payment, "Unproven"), entry(&own, "Unproven")]
    );
    assert_eq!(words(&store, &payment).await, word("unproven", "unmined"));
    assert_eq!(words(&store, &own).await, word("unproven", "unmined"));
    one_post_naming(&a, 0, &[payment, own]);
}

/// No broadcaster takes the post (a refusal, a fault, a timeout): one post
/// naming both, each entry `sending`, each row queued for the monitor.
#[tokio::test]
async fn a_signed_set_no_broadcaster_takes_reads_sending_and_stays_queued() {
    for (kind, text) in [
        (MockErrorKind::BroadcastFailed, "rejected"),
        (
            MockErrorKind::ServiceError,
            "Service temporarily unavailable (503)",
        ),
        (MockErrorKind::NetworkError, "Request timed out"),
    ] {
        let root = PrivateKey::random();
        let store = store_of(&root).await;
        let a = wallet(&store, &root, refusing(kind, text)).await;
        let payment = prepared(&a).await;
        let (own, got) = signed_deferred(&a, vec![payment], false).await;
        assert_eq!(
            got,
            vec![entry(&payment, "Sending"), entry(&own, "Sending")],
            "{text}"
        );
        assert_eq!(
            words(&store, &payment).await,
            word("sending", "unsent"),
            "{text}"
        );
        assert_eq!(
            words(&store, &own).await,
            word("sending", "unsent"),
            "{text}"
        );
        one_post_naming(&a, 0, &[payment, own]);
    }
}

/// A double-spend word naming the payment: a hint, never an error; the
/// payment and the action read `sending`, queued to be asked again.
#[tokio::test]
async fn a_double_spend_word_on_the_named_payment_is_a_hint() {
    let root = PrivateKey::random();
    let store = store_of(&root).await;
    let payment = prepared(&wallet(&store, &root, accepting()).await).await;
    let competitor = hex::encode([0x77; 32]);
    let services = MockWalletServices::builder()
        .post_beef_double_spend(&hex::encode(payment), &competitor)
        .build();
    let a = wallet(&store, &root, services).await;
    let (own, got) = signed_deferred(&a, vec![payment], false).await;
    assert_eq!(
        got,
        vec![entry(&payment, "Sending"), entry(&own, "Sending")]
    );
    assert_eq!(words(&store, &payment).await, word("sending", "unsent"));
    assert_eq!(words(&store, &own).await, word("sending", "unsent"));
    one_post_naming(&a, 0, &[payment, own]);
}

/// The same user's second named payment already on its way (`sending`
/// before the call) is not carried: the set is the action and the payment
/// the call moved.
#[tokio::test]
async fn a_named_payment_already_sending_is_not_carried() {
    let root = PrivateKey::random();
    let store = store_of(&root).await;
    let a = wallet(&store, &root, accepting()).await;
    let waiting = prepared(&a).await;
    let delayed = CreateActionOptions {
        send_with: Some(vec![waiting]),
        accept_delayed_broadcast: Some(true),
        ..Default::default()
    };
    a.create_action(args(None, delayed), Some(ORIGIN))
        .await
        .unwrap();
    assert_eq!(words(&store, &waiting).await, word("sending", "unsent"));
    let payment = prepared(&a).await;
    let (own, got) = signed_deferred(&a, vec![waiting, payment], false).await;
    assert_eq!(
        got,
        vec![
            entry(&waiting, "Sending"),
            entry(&payment, "Unproven"),
            entry(&own, "Unproven")
        ]
    );
    assert_eq!(words(&store, &waiting).await, word("sending", "unsent"));
    one_post_naming(&a, 0, &[payment, own]);
}

// =============================================================================
// noSend beside a non-empty sendWith: refused at both doors, nothing written
// =============================================================================

const REFUSAL: &str = "Validation error: noSend and sendWith cannot be combined: a noSend action is not sent, and sendWith sends the named transactions with this action";

/// (transaction rows, output rows, coins taken, requests).
async fn rows(store: &StorageSqlx) -> (i64, i64, i64, i64) {
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
        one("SELECT COUNT(*) FROM proven_tx_reqs").await,
    )
}

fn assert_refused(answer: bsv_rs::Error, cell: &str) {
    match answer {
        bsv_rs::Error::WalletRefusal { class, message } => {
            assert_eq!(
                class,
                bsv_rs::WalletRefusalClass::InvalidParameter,
                "{cell}"
            );
            assert_eq!(message, REFUSAL, "{cell}");
        }
        other => panic!("{cell}: not the refusal: {other:?}"),
    }
}

/// The create door: an action of the caller's under `noSend` naming the
/// prepared payment, signed in the call or deferred: refused, nothing
/// built, written, reserved or queued, nothing posted.
#[tokio::test]
async fn the_create_door_refuses_no_send_beside_send_with() {
    for sign_and_process in [true, false] {
        let cell = format!("signAndProcess {sign_and_process}");
        let root = PrivateKey::random();
        let store = store_of(&root).await;
        let a = wallet(&store, &root, accepting()).await;
        let payment = prepared(&a).await;
        let before = rows(&store).await;
        let options = CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(sign_and_process),
            send_with: Some(vec![payment]),
            ..Default::default()
        };
        let answer = a
            .create_action(args(Some(vec![payment_output()]), options), Some(ORIGIN))
            .await
            .expect_err("refused");
        assert_refused(answer, &cell);
        assert_eq!(rows(&store).await, before, "{cell}: nothing written");
        assert_eq!(
            words(&store, &payment).await,
            word("nosend", "nosend"),
            "{cell}"
        );
        assert!(posts(&a).is_empty(), "{cell}: nothing posted");
    }
}

/// A deferred action and its sign, `noSend` and `sendWith` from either
/// call: (the create's options, the sign's options). A create naming the
/// payment carries an output of the caller's: with none it would build
/// nothing (a `sendWith` alone).
fn sign_cells(payment: [u8; 32]) -> Vec<(&'static str, CreateActionOptions, SignActionOptions)> {
    let deferred = CreateActionOptions {
        sign_and_process: Some(false),
        ..Default::default()
    };
    vec![
        (
            "noSend cached from the create, sendWith at the sign",
            CreateActionOptions {
                no_send: Some(true),
                ..deferred.clone()
            },
            SignActionOptions {
                send_with: Some(vec![payment]),
                ..Default::default()
            },
        ),
        (
            "both at the sign",
            deferred.clone(),
            SignActionOptions {
                no_send: Some(true),
                send_with: Some(vec![payment]),
                ..Default::default()
            },
        ),
        (
            "sendWith cached from the create, noSend at the sign",
            CreateActionOptions {
                send_with: Some(vec![payment]),
                ..deferred
            },
            SignActionOptions {
                no_send: Some(true),
                ..Default::default()
            },
        ),
    ]
}

/// The sign door (S2): refused before anything is signed or written; the
/// deferred action stays unsigned with its session, and signs afterwards
/// with no `sendWith`.
#[tokio::test]
async fn the_sign_door_refuses_no_send_beside_send_with() {
    use base64::Engine;
    for index in 0..3 {
        let root = PrivateKey::random();
        let store = store_of(&root).await;
        let a = wallet(&store, &root, accepting()).await;
        let payment = prepared(&a).await;
        let (cell, create, sign) = sign_cells(payment).swap_remove(index);
        let outputs = create.send_with.is_some().then(|| vec![payment_output()]);
        let made = a
            .create_action(args(outputs, create), Some(ORIGIN))
            .await
            .unwrap_or_else(|e| panic!("{cell}: the create: {e}"));
        let reference = base64::engine::general_purpose::STANDARD
            .encode(made.signable_transaction.unwrap().reference);
        let before = (rows(&store).await, posts(&a).len());
        let call = |options: SignActionOptions| SignActionArgs {
            spends: std::collections::HashMap::new(),
            reference: reference.clone(),
            options: Some(options),
        };
        let answer = a
            .sign_action(call(sign), Some(ORIGIN))
            .await
            .expect_err("refused");
        assert_refused(answer, cell);
        assert_eq!(
            (rows(&store).await, posts(&a).len()),
            before,
            "{cell}: nothing written, nothing posted"
        );
        assert_eq!(
            words(&store, &payment).await,
            word("nosend", "nosend"),
            "{cell}"
        );
        let unsigned: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE status = 'unsigned'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(unsigned, 1, "{cell}: the deferred action stays unsigned");
        let again = SignActionOptions {
            no_send: Some(true),
            send_with: Some(vec![]),
            ..Default::default()
        };
        a.sign_action(call(again), Some(ORIGIN))
            .await
            .unwrap_or_else(|e| panic!("{cell}: the session stays: {e}"));
    }
}
