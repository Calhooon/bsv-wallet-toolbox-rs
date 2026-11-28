//! The sign door's guard and a busy `abortAction` (W12). A bare deferred
//! action whose `abortAction` the store refused before acting (busy) is
//! still unsigned and cached, so it stays in the sign door's set: a later
//! `signAction` naming a prepared `noSend` payment in `sendWith` with no
//! delayed post is refused by the guard, nothing is posted and the payment
//! stays `nosend`. Before, the reference left the set whatever the store
//! answered, and that `signAction` signed and posted the deferred
//! change-only transaction of the wallet's own.
//!
//! The fixture is `send_with_alone_refused_tests.rs`'s: one in-memory store
//! seeded with three proven change coins of the wallet's own, the real
//! signer, a mock broadcaster that accepts. The writer is held through
//! `write_tx`, as the busy door tests hold it, so the abort waits the
//! store's ceiling (5 s) and answers busy having written nothing.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    AbortActionArgs, Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel, SignActionArgs, SignActionOptions,
    WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

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

/// Call 1 through `wallet`: the prepared payment's txid.
async fn prepared(wallet: &Payer) -> [u8; 32] {
    let made = wallet
        .create_action(prepare(), ORIGIN)
        .await
        .expect("call 1");
    made.txid.expect("txid")
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

/// The deferred action's reference, as the caller reads it.
async fn deferred_reference(wallet: &Payer, outputs: Option<Vec<CreateActionOutput>>) -> String {
    let made = wallet
        .create_action(deferred(outputs), ORIGIN)
        .await
        .expect("the deferred create");
    String::from_utf8(made.signable_transaction.expect("signable").reference).unwrap()
}

/// `signAction` on `reference` with `send_with` and no delayed post, the
/// answer in words with the reference written `<ref>`.
async fn sign_words(wallet: &Payer, reference: &str, send_with: Option<Vec<[u8; 32]>>) -> String {
    let args = SignActionArgs {
        spends: Default::default(),
        reference: reference.to_string(),
        options: Some(SignActionOptions {
            send_with,
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    };
    match wallet.sign_action(args, ORIGIN).await {
        Err(e) => e.to_string().replace(reference, "<ref>"),
        Ok(r) => format!("answered: txid {:?}", r.txid.map(hex::encode)),
    }
}

/// `abortAction` of `reference`: `aborted`, or the error's text.
async fn abort(wallet: &Payer, reference: &str) -> Result<bool, String> {
    wallet
        .abort_action(
            AbortActionArgs {
                reference: reference.to_string(),
            },
            ORIGIN,
        )
        .await
        .map(|r| r.aborted)
        .map_err(|e| e.to_string())
}

/// W12: a busy `abortAction` leaves the bare deferred action in the sign
/// door's set. The guard refuses its `signAction` naming the payment with
/// no delayed post; the abort repeated answers `aborted: true`; then the
/// reference gets the answer an aborted reference gets (as W10).
#[tokio::test]
async fn w12_a_busy_abort_keeps_the_sign_doors_guard() {
    let wallet = payer().await;
    let txid = prepared(&wallet).await;
    let payment = hex::encode(txid);
    let reference = deferred_reference(&wallet, None).await;

    let held = wallet.storage().write_tx().await.unwrap();
    let busy = abort(&wallet, &reference).await;
    held.rollback().await.unwrap();
    println!("STEP W12 busy abortAction: {busy:?}");
    let busy = busy.expect_err("the held writer makes the abort busy");
    assert!(busy.to_lowercase().contains("busy"), "{busy}");

    let signed = sign_words(&wallet, &reference, Some(vec![txid])).await;
    println!(
        "STEP W12 signAction after the busy abort: {signed}\nposts {:?}",
        posts(&wallet)
    );
    assert!(
        signed.contains(
            "signAction: sendWith on a deferred action with no inputs and no outputs of the caller's \
             is refused unless acceptDelayedBroadcast is true"
        ),
        "{signed}"
    );
    assert!(signed.contains(&payment), "{signed}");
    assert!(
        posts(&wallet).is_empty(),
        "nothing posted: {:?}",
        posts(&wallet)
    );
    assert_eq!(
        words(wallet.storage(), &payment).await,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );

    assert_eq!(abort(&wallet, &reference).await, Ok(true));
    let after = sign_words(&wallet, &reference, Some(vec![txid])).await;
    let other = deferred_reference(&wallet, None).await;
    assert_eq!(abort(&wallet, &other).await, Ok(true));
    let aborted = sign_words(&wallet, &other, None).await;
    println!("STEP W12 after the abort: {after}\nan aborted reference: {aborted}\nEND");
    assert!(!after.contains("is refused unless"), "{after}");
    assert_eq!(after, aborted);
    assert!(
        posts(&wallet).is_empty(),
        "nothing posted: {:?}",
        posts(&wallet)
    );
    assert_eq!(
        words(wallet.storage(), &payment).await,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );
}
