//! The reference payer's submit on the stable line: `createAction` naming a
//! prepared `noSend` payment in `sendWith`, with no inputs and no outputs,
//! is refused by name unless the caller accepts a delayed post, before
//! anything is built or written.
//!
//! The reference's paying client (ts-stack
//! 57c5f13055b555b83c1ef368fff3650abd21b6ef,
//! `packages/sdk/src/auth/clients/AuthFetch.ts` 1247-1275) prepares the
//! payment with `noSend: true, acceptDelayedBroadcast: false`, then submits
//! it by `createAction` with `options: { sendWith: [txid],
//! acceptDelayedBroadcast: false }` and nothing else. This line has no
//! stored post of named transactions alone: before the refusal it funded
//! that call as a new action, posted a change-only transaction of its own
//! and left the payment `sending` for the send-waiting task. The 0.14
//! line answers the call as the reference does (beta's
//! `tests/send_with_alone_tests.rs`).
//!
//! One in-memory store seeded with three proven change coins of the
//! wallet's own, the real signer, a mock broadcaster that accepts; nothing
//! touches the network. W3 and W4 pin what the base (0.13.9's tree, main
//! 67b98d7a) printed for the same calls, which the guard leaves as it was;
//! W7 pins what main 9a53f497 printed for the sign door with an output. W6
//! is the same build through `signAction`, W8 and W9 the refusal's text.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    AbortActionArgs, Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    CreateActionResult, KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel, SignActionArgs,
    SignActionOptions, SignActionResult, WalletInterface,
};
use bsv_wallet_toolbox_rs::monitor::tasks::SendWaitingTask;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{MonitorTask, StorageSqlx, Wallet, WalletStorageWriter};

type Payer = Wallet<StorageSqlx, MockWalletServices>;

const ORIGIN: &str = "test.local";
const BRC29_PROTOCOL: &str = "3241645161d8";
const PREFIX: &str = "YXV0aGZldGNo";
const SUFFIXES: [&str; 3] = ["Y29pbi0w", "Y29pbi0x", "Y29pbi0y"];
const FUNDING_SATS: u64 = 100_000;
const PAYMENT_SATS: u64 = 1_000;

fn p2pkh_lock(pubkey: &[u8]) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(pubkey));
    script.extend([0x88, 0xac]);
    script
}

/// The payee's lock: the P2PKH the payer derives for the server's key.
fn payee_lock() -> Vec<u8> {
    hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// A wallet over a store holding three completed, proven parents, each with
/// one P2PKH change coin the wallet can sign for; its broadcaster accepts.
async fn payer() -> Payer {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("send-with-alone", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("basket");
    let proto = ProtoWallet::new(Some(root.clone()));
    for (i, suffix) in SUFFIXES.iter().enumerate() {
        let sk = proto
            .key_deriver()
            .derive_private_key(
                &Protocol::new(SecurityLevel::Counterparty, BRC29_PROTOCOL),
                &format!("{PREFIX} {suffix}"),
                &Counterparty::Self_,
            )
            .expect("derive funding key");
        let funding_lock = p2pkh_lock(&sk.public_key().to_compressed());
        let satoshis = FUNDING_SATS + i as u64;
        let height = 100_000 + i as u32;
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
                satoshis: Some(satoshis),
                locking_script: LockingScript::from_binary(&funding_lock).unwrap(),
                change: false,
            }],
            0,
        );
        let parent_txid = parent.id();
        let now = chrono::Utc::now();
        let bump = MerklePath::from_coinbase_txid(&parent_txid, height);
        sqlx::query(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, \
             raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&parent_txid)
        .bind(i64::from(height))
        .bind(format!("a{i}").repeat(32))
        .bind(&parent_txid)
        .bind(bump.to_binary())
        .bind(parent.to_binary())
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .expect("proven funding parent");
        let parent_id = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
             lock_time, description, txid, raw_tx, created_at, updated_at) \
             VALUES (?, 'completed', ?, 0, ?, 1, 0, 'seeded parent', ?, ?, ?, ?)",
        )
        .bind(user.user_id)
        .bind(format!("parent-ref-{i}"))
        .bind(satoshis as i64)
        .bind(&parent_txid)
        .bind(parent.to_binary())
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .expect("insert parent")
        .last_insert_rowid();
        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, \
             txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, \
             purpose, output_description, created_at, updated_at) \
             VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, ?, ?, 'storage', 'change', 'wallet funding', ?, ?)",
        )
        .bind(user.user_id)
        .bind(parent_id)
        .bind(basket.basket_id)
        .bind(satoshis as i64)
        .bind(&funding_lock)
        .bind(&parent_txid)
        .bind(PREFIX)
        .bind(*suffix)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .expect("insert funding output");
    }
    let services = MockWalletServices::builder().post_beef_success().build();
    Wallet::new(Some(root), storage, services)
        .await
        .expect("wallet")
}

/// Call 1's arguments, as `AuthFetch.ts` 1135-1162 builds them.
fn prepare() -> CreateActionArgs {
    CreateActionArgs {
        description: "BRC-105 HTTP request payment".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: payee_lock(),
            satoshis: PAYMENT_SATS,
            output_description: "HTTP request payment".to_string(),
            basket: None,
            custom_instructions: Some(
                r#"{"derivationPrefix":"cHJlZml4","derivationSuffix":"c3VmZml4","payee":"02aa"}"#
                    .to_string(),
            ),
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec![format!(
            "brc105 {} {}",
            "11".repeat(32),
            "22".repeat(32)
        )]),
        options: Some(CreateActionOptions {
            randomize_outputs: Some(false),
            no_send: Some(true),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    }
}

/// Call 2's arguments, as `AuthFetch.ts` 1251-1254 builds them, with
/// `acceptDelayedBroadcast` as `delayed` says (`None`: not said at all).
fn submit(txid: [u8; 32], delayed: Option<bool>) -> CreateActionArgs {
    CreateActionArgs {
        description: "Submit prepared HTTP payment".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            send_with: Some(vec![txid]),
            accept_delayed_broadcast: delayed,
            ..Default::default()
        }),
    }
}

/// A new action of one real output naming `txid` in `sendWith`, not
/// accepting a delayed post.
fn beside_an_output(txid: [u8; 32]) -> CreateActionArgs {
    CreateActionArgs {
        description: "A payment beside the prepared one".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: payee_lock(),
            satoshis: 500,
            output_description: "another payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            randomize_outputs: Some(false),
            send_with: Some(vec![txid]),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    }
}

/// (transaction statuses, request statuses) of `txid`.
async fn words(storage: &StorageSqlx, txid: &str) -> (Vec<String>, Vec<String>) {
    let tx = sqlx::query_scalar::<_, String>("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_all(storage.pool())
        .await
        .unwrap();
    let req = sqlx::query_scalar::<_, String>("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_all(storage.pool())
        .await
        .unwrap();
    (tx, req)
}

/// (transaction rows, output rows, coins taken).
async fn counts(storage: &StorageSqlx) -> (i64, i64, i64) {
    let one = |sql: &'static str| async move {
        sqlx::query_scalar::<_, i64>(sql)
            .fetch_one(storage.pool())
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
fn posts(wallet: &Payer) -> Vec<String> {
    wallet
        .services()
        .call_history()
        .into_iter()
        .filter(|c| c.method == "post_beef")
        .map(|c| c.args.join(" "))
        .collect()
}

/// The monitor's send-waiting task itself (`SendWaitingTask::run`), every
/// request aged past its bound. The task posts through the store's
/// services, the wallet's broadcaster (`Wallet::new` sets them on the
/// store, and a clone of the store shares them); its own services argument
/// is unused.
async fn send_waiting(storage: &StorageSqlx) {
    sqlx::query(
        "UPDATE proven_tx_reqs SET created_at = datetime('now', '-1 hour'), \
         updated_at = datetime('now', '-1 hour')",
    )
    .execute(storage.writer())
    .await
    .unwrap();
    let task = SendWaitingTask::new(
        std::sync::Arc::new(storage.clone()),
        std::sync::Arc::new(MockWalletServices::builder().build()),
    );
    let ran = task.run().await.expect("the send-waiting task");
    println!(
        "STEP send-waiting task: items processed {}, errors {:?}",
        ran.items_processed, ran.errors
    );
}

/// Call 1 through `wallet`: the prepared payment's txid.
async fn prepared(wallet: &Payer) -> [u8; 32] {
    let made = wallet
        .create_action(prepare(), ORIGIN)
        .await
        .expect("call 1");
    made.txid.expect("txid")
}

/// What a call answered and left, with the payment's txid written
/// `<payment>` and any other txid `<own>`, so a run's words compare with
/// another run's.
async fn described(
    wallet: &Payer,
    payment: &str,
    answer: &bsv_rs::Result<CreateActionResult>,
    before: (i64, i64, i64),
) -> String {
    let name = |t: String| {
        if t == payment {
            "<payment>".to_string()
        } else {
            "<own>".to_string()
        }
    };
    let head = match answer {
        Ok(r) => format!(
            "answered: txid {:?}; sendWithResults {:?}",
            r.txid.map(|t| name(hex::encode(t))),
            r.send_with_results
                .iter()
                .flatten()
                .map(|e| (name(hex::encode(e.txid)), format!("{:?}", e.status)))
                .collect::<Vec<_>>()
        ),
        Err(e) => format!("refused: {}", e.to_string().replace(payment, "<payment>")),
    };
    let after = counts(wallet.storage()).await;
    let posted: Vec<String> = posts(wallet)
        .iter()
        .map(|p| format!("post naming the payment: {}", p.contains(payment)))
        .collect();
    format!(
        "{head}\ntransaction rows +{}, output rows +{}, coins taken +{}\nthe payment reads {:?}\nposts {posted:?}",
        after.0 - before.0,
        after.1 - before.1,
        after.2 - before.2,
        words(wallet.storage(), payment).await
    )
}

/// The refusal's text names the payment and says the three things an
/// application can show.
fn assert_refused_by_name(answer: &bsv_rs::Result<CreateActionResult>, payment: &str) {
    let text = match answer {
        Err(e) => e.to_string(),
        Ok(r) => panic!(
            "call 2 was answered, not refused: txid {:?}, sendWithResults {:?}",
            r.txid.map(hex::encode),
            r.send_with_results
        ),
    };
    assert!(
        matches!(answer, Err(bsv_rs::Error::WalletError(_))),
        "{text}"
    );
    for words in [
        "Validation error: ",
        payment,
        "were NOT sent by this call",
        "those that are your own unsent noSend transactions are still reserved under their txids",
        "(1) name them in sendWith beside a new action",
        "(2) release one with abortAction",
        "(1) name them in sendWith beside a new action, which queues them for the monitor's \
         send-waiting task, with no change-only transaction of the wallet's own;",
        "(3) call again with acceptDelayedBroadcast: true, which on this line also builds a \
         change-only transaction of the wallet's own, with its fee, and queues it and the \
         named transactions for the monitor's send-waiting task.",
        "A host with no monitor running never posts what (1) or (3) queues.",
    ] {
        assert!(text.contains(words), "{words:?} is not in {text:?}");
    }
}

/// Call 2 refused, as `delayed` says: nothing built, written, taken or
/// posted, the payment still `nosend`.
async fn refused_and_untouched(delayed: Option<bool>) -> (Payer, String) {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let before = counts(wallet.storage()).await;
    let answer = wallet.create_action(submit(txid, delayed), ORIGIN).await;
    println!(
        "STEP call 2 (acceptDelayedBroadcast {delayed:?})\n{}",
        described(&wallet, &payment, &answer, before).await
    );
    assert_refused_by_name(&answer, &payment);
    assert_eq!(
        counts(wallet.storage()).await,
        before,
        "no row added, no coin taken"
    );
    assert_eq!(
        words(wallet.storage(), &payment).await,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );
    assert!(
        posts(&wallet).is_empty(),
        "nothing posted: {:?}",
        posts(&wallet)
    );
    (wallet, payment)
}

/// W1: the payer's two calls as the reference's client makes them. Call 2
/// is refused by name; then the application's `abortAction` answers
/// `aborted: true` and the payment's coins are free.
#[tokio::test]
async fn w1_the_payers_submit_is_refused_and_the_payment_can_be_released() {
    let (wallet, payment) = refused_and_untouched(Some(false)).await;
    let aborted = wallet
        .abort_action(
            AbortActionArgs {
                reference: payment.clone(),
            },
            ORIGIN,
        )
        .await
        .map(|r| r.aborted);
    assert_eq!(aborted.map_err(|e| e.to_string()), Ok(true));
    assert_eq!(counts(wallet.storage()).await.2, 0, "no coin stays taken");
    assert!(posts(&wallet).is_empty());
}

/// W2: the same call with `acceptDelayedBroadcast` not said: this line's
/// default is `false`, so it is refused the same way.
#[tokio::test]
async fn w2_the_submit_with_no_word_on_delay_is_refused() {
    refused_and_untouched(None).await;
}

/// What the base printed for W3 (`acceptDelayedBroadcast: true`).
const W3_ON_BASE: &str = "answered: txid Some(\"<own>\"); sendWithResults [(\"<payment>\", \"Sending\"), (\"<own>\", \"Sending\")]\n\
     transaction rows +1, output rows +5, coins taken +1\n\
     the payment reads ([\"sending\"], [\"unsent\"])\n\
     posts []";

/// What the base printed for W4 (`sendWith` beside a real output).
const W4_ON_BASE: &str = "answered: txid Some(\"<own>\"); sendWithResults [(\"<payment>\", \"Sending\"), (\"<own>\", \"Unproven\")]\n\
     transaction rows +1, output rows +6, coins taken +1\n\
     the payment reads ([\"sending\"], [\"unsent\"])\n\
     posts [\"post naming the payment: false\"]";

/// W3: the same form with `acceptDelayedBroadcast: true` is as it was.
#[tokio::test]
async fn w3_the_submit_accepting_delay_is_unchanged() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let before = counts(wallet.storage()).await;
    let answer = wallet.create_action(submit(txid, Some(true)), ORIGIN).await;
    let seen = described(&wallet, &payment, &answer, before).await;
    println!("STEP W3\n{seen}\nEND");
    assert_eq!(seen, W3_ON_BASE);
}

/// W4: `sendWith` beside a real output, no delayed post, is as it was.
#[tokio::test]
async fn w4_send_with_beside_an_output_is_unchanged() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let before = counts(wallet.storage()).await;
    let answer = wallet.create_action(beside_an_output(txid), ORIGIN).await;
    let seen = described(&wallet, &payment, &answer, before).await;
    println!("STEP W4\n{seen}\nEND");
    assert_eq!(seen, W4_ON_BASE);
}

/// W5: after W1's refused submit (asserted refused here), the monitor's
/// send-waiting task posts nothing: the payment is not queued.
#[tokio::test]
async fn w5_send_waiting_after_the_refused_submit_posts_nothing() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let answer = wallet
        .create_action(submit(txid, Some(false)), ORIGIN)
        .await;
    assert_refused_by_name(&answer, &payment);
    send_waiting(wallet.storage()).await;
    let posted = posts(&wallet);
    println!(
        "STEP W5: call 2 {:?}; after send-waiting the payment reads {:?}; posts {posted:?}",
        answer
            .as_ref()
            .map(|r| r.txid.map(hex::encode))
            .map_err(|e| e.to_string()),
        words(wallet.storage(), &payment).await
    );
    assert!(posted.is_empty(), "nothing posted: {posted:?}");
    assert_eq!(
        words(wallet.storage(), &payment).await,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );
}

/// A deferred action (`signAndProcess: false`, no delayed post): no inputs
/// and no outputs of the caller's, or `outputs` when given; no `sendWith`.
fn deferred(outputs: Option<Vec<CreateActionOutput>>) -> CreateActionArgs {
    CreateActionArgs {
        description: "A deferred action to sign later".to_string(),
        input_beef: None,
        inputs: None,
        outputs,
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(false),
            randomize_outputs: Some(false),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    }
}

/// `signAction` on `reference`, naming `send_with`, no delayed post.
fn sign_naming(reference: String, send_with: Vec<[u8; 32]>) -> SignActionArgs {
    SignActionArgs {
        spends: Default::default(),
        reference,
        options: Some(SignActionOptions {
            send_with: Some(send_with),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    }
}

/// The deferred action's reference, as the caller reads it.
async fn deferred_reference(wallet: &Payer, outputs: Option<Vec<CreateActionOutput>>) -> String {
    let made = wallet
        .create_action(deferred(outputs), ORIGIN)
        .await
        .expect("the deferred create");
    String::from_utf8(made.signable_transaction.expect("signable").reference).unwrap()
}

/// A sign answer in the words `described` reads.
fn as_create(answer: bsv_rs::Result<SignActionResult>) -> bsv_rs::Result<CreateActionResult> {
    answer.map(|r| CreateActionResult {
        txid: r.txid,
        tx: None,
        no_send_change: None,
        send_with_results: r.send_with_results,
        signable_transaction: None,
        input_type: None,
        inputs: None,
        reference_number: None,
        beef: None,
    })
}

/// W6: the same build through the sign door. A deferred action with
/// nothing of the caller's in it, asked at signing to send the payment
/// without a delayed post, is refused by name before any signature, lock
/// or write; the deferred action stays abortable.
#[tokio::test]
async fn w6_the_sign_door_refuses_send_with_alone() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let reference = deferred_reference(&wallet, None).await;
    let before = counts(wallet.storage()).await;
    let answer = as_create(
        wallet
            .sign_action(sign_naming(reference.clone(), vec![txid]), ORIGIN)
            .await,
    );
    println!(
        "STEP W6 signAction\n{}\nEND",
        described(&wallet, &payment, &answer, before).await
    );
    assert_refused_by_name(&answer, &payment);
    let text = answer.unwrap_err().to_string();
    assert!(text.contains("signAction"), "{text}");
    assert!(
        text.contains(
            "The deferred action is as it was: sign it with an empty sendWith or release it \
             with abortAction."
        ),
        "{text}"
    );
    assert_eq!(counts(wallet.storage()).await, before, "nothing written");
    assert_eq!(
        words(wallet.storage(), &payment).await,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );
    assert!(posts(&wallet).is_empty(), "nothing posted");
    let aborted = wallet
        .abort_action(AbortActionArgs { reference }, ORIGIN)
        .await
        .map(|r| r.aborted);
    assert_eq!(aborted.map_err(|e| e.to_string()), Ok(true));
}

/// What the base printed for W7 (a deferred action with an output, signed
/// naming the payment, no delayed post).
const W7_ON_BASE: &str = "answered: txid Some(\"<own>\"); sendWithResults [(\"<payment>\", \"Sending\"), (\"<own>\", \"Unproven\")]\n\
     transaction rows +0, output rows +0, coins taken +0\n\
     the payment reads ([\"sending\"], [\"unsent\"])\n\
     posts [\"post naming the payment: false\"]";

/// W7: the sign door with an output of the caller's and a `sendWith` is as
/// it was.
#[tokio::test]
async fn w7_the_sign_door_with_an_output_is_unchanged() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let outputs = beside_an_output(txid).outputs;
    let reference = deferred_reference(&wallet, outputs).await;
    let before = counts(wallet.storage()).await;
    let answer = as_create(
        wallet
            .sign_action(sign_naming(reference, vec![txid]), ORIGIN)
            .await,
    );
    let seen = described(&wallet, &payment, &answer, before).await;
    println!("STEP W7\n{seen}\nEND");
    assert_eq!(seen, W7_ON_BASE);
}

/// The text of a refused `createAction` naming `send_with` alone, no delay.
async fn refusal_text(wallet: &Payer, send_with: Vec<[u8; 32]>) -> String {
    let mut args = submit([0u8; 32], Some(false));
    args.options.as_mut().unwrap().send_with = Some(send_with);
    match wallet.create_action(args, ORIGIN).await {
        Err(e) => e.to_string(),
        Ok(r) => panic!("answered, not refused: {:?}", r.txid.map(hex::encode)),
    }
}

/// W8: a `sendWith` of 1,000 txids: the text prints three and the count of
/// the rest, so its length is bounded.
#[tokio::test]
async fn w8_a_thousand_named_txids_give_a_short_text() {
    let wallet = payer().await;
    let txids: Vec<[u8; 32]> = (0..1000u32)
        .map(|i| {
            let mut t = [0u8; 32];
            t[..4].copy_from_slice(&i.to_be_bytes());
            t
        })
        .collect();
    let before = counts(wallet.storage()).await;
    let text = refusal_text(&wallet, txids.clone()).await;
    println!("STEP W8 ({} bytes)\n{text}\nEND", text.len());
    for shown in &txids[..3] {
        assert!(text.contains(&hex::encode(shown)), "{text}");
    }
    assert!(!text.contains(&hex::encode(txids[3])), "{text}");
    assert!(text.contains(" and 997 more]"), "{text}");
    assert!(text.len() < 1_200, "{} bytes: {text}", text.len());
    assert_eq!(counts(wallet.storage()).await, before, "nothing written");
}

/// W9: a txid the wallet does not know: the text claims nothing false of it
/// (it says this call did not send it, and holds the reservation only of
/// the caller's own unsent noSend transactions).
#[tokio::test]
async fn w9_an_unknown_txid_is_not_said_to_be_reserved() {
    let wallet = payer().await;
    let unknown = [0x5au8; 32];
    let text = refusal_text(&wallet, vec![unknown]).await;
    println!("STEP W9\n{text}\nEND");
    assert!(text.contains(&format!(
        "[{}] were NOT sent by this call",
        hex::encode(unknown)
    )));
    assert!(!text.contains("they are still reserved"), "{text}");
    assert!(
        text.contains("those that are your own unsent noSend transactions"),
        "{text}"
    );
    assert!(!text.contains(" more]"), "{text}");
}

/// `signAction` on an aborted deferred action, its reference written `<ref>`.
async fn sign_aborted(wallet: &Payer, send_with: Option<Vec<[u8; 32]>>) -> String {
    let reference = deferred_reference(wallet, None).await;
    let aborted = wallet
        .abort_action(
            AbortActionArgs {
                reference: reference.clone(),
            },
            ORIGIN,
        )
        .await
        .map(|r| r.aborted);
    assert_eq!(aborted.map_err(|e| e.to_string()), Ok(true));
    let args = SignActionArgs {
        spends: Default::default(),
        reference: reference.clone(),
        options: Some(SignActionOptions {
            send_with,
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    };
    match wallet.sign_action(args, ORIGIN).await {
        Err(e) => e.to_string().replace(&reference, "<ref>"),
        Ok(r) => format!("answered: txid {:?}", r.txid.map(hex::encode)),
    }
}

/// W10: a bare deferred action aborted, then signed naming the payment in
/// `sendWith`: the answer is the one an aborted reference gets with no
/// `sendWith`, not the guard's refusal, and the payment stays `nosend`.
#[tokio::test]
async fn w10_an_aborted_bare_deferred_action_gets_the_sign_paths_answer() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let without = sign_aborted(&wallet, None).await;
    let with = sign_aborted(&wallet, Some(vec![txid])).await;
    println!("STEP W10\nwithout sendWith: {without}\nwith sendWith: {with}\nEND");
    assert!(!with.contains("is refused unless"), "{with}");
    assert_eq!(with, without);
    assert_eq!(
        words(wallet.storage(), &payment).await,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );
    assert!(
        posts(&wallet).is_empty(),
        "nothing posted: {:?}",
        posts(&wallet)
    );
}

/// W11: a bare deferred action whose create named the payment with a
/// delayed post, signed with no delayed post: with no `sendWith` it falls
/// back to the create's list and is refused; with `sendWith: []` it is
/// signed.
#[tokio::test]
async fn w11_an_empty_send_with_signs_what_the_create_named() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let mut create = deferred(None);
    let options = create.options.as_mut().unwrap();
    options.send_with = Some(vec![txid]);
    options.accept_delayed_broadcast = Some(true);
    let made = wallet
        .create_action(create, ORIGIN)
        .await
        .expect("the deferred create naming the payment");
    let reference =
        String::from_utf8(made.signable_transaction.expect("signable").reference).unwrap();
    let sign = |send_with: Option<Vec<[u8; 32]>>| SignActionArgs {
        spends: Default::default(),
        reference: reference.clone(),
        options: Some(SignActionOptions {
            send_with,
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    };
    let fallback = as_create(wallet.sign_action(sign(None), ORIGIN).await);
    assert_refused_by_name(&fallback, &hex::encode(txid));
    let empty = wallet.sign_action(sign(Some(vec![])), ORIGIN).await;
    println!(
        "STEP W11 no sendWith: {:?}\nsendWith []: {:?}\nEND",
        fallback
            .map(|r| r.txid.map(hex::encode))
            .map_err(|e| e.to_string()),
        empty
            .as_ref()
            .map(|r| r.txid.map(hex::encode))
            .map_err(|e| e.to_string())
    );
    let own = hex::encode(empty.expect("sendWith [] signs").txid.expect("txid"));
    println!("STEP W11 posts {:?}", posts(&wallet));
    let seen = posts(&wallet);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].ends_with(&format!("txids=[\"{own}\"]")), "{seen:?}");
}
