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
//! 67b98d7a) printed for the same calls, which the guard leaves as it was.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    AbortActionArgs, Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    CreateActionResult, KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{MonitorStorage, StorageSqlx, Wallet, WalletStorageWriter};

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

/// The monitor's send-waiting pass, every request aged past its bound.
async fn send_waiting(storage: &StorageSqlx) {
    sqlx::query(
        "UPDATE proven_tx_reqs SET created_at = datetime('now', '-1 hour'), \
         updated_at = datetime('now', '-1 hour')",
    )
    .execute(storage.writer())
    .await
    .unwrap();
    storage
        .send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .expect("send-waiting");
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
        "were NOT sent",
        "still reserved under their txids",
        "acceptDelayedBroadcast: true",
        "name them in sendWith beside a new action",
        "abortAction",
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

/// W5: after W1's refused submit, the monitor's send-waiting pass posts
/// nothing: the payment is not queued.
#[tokio::test]
async fn w5_send_waiting_after_the_refused_submit_posts_nothing() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let answer = wallet
        .create_action(submit(txid, Some(false)), ORIGIN)
        .await;
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
