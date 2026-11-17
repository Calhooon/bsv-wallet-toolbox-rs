//! A wallet payment's sender identity key, from `internalizeAction` through
//! to a signed spend.
//!
//! Each run ends in `Wallet::create_action` (or `sign_action` for a deferred
//! action) selecting the coin: the signer derives its key from the stored
//! row, and the spending input's unlocking script is run against the coin's
//! P2PKH by bsv-rs's script interpreter. Or it ends in the refusal the door
//! answers.
//!
//! - a wallet payment whose sender is empty (the wallet itself) is stored
//!   with no sender, as the wallet's own change is, and signs;
//! - a row holding an empty sender, as an earlier release wrote it, signs;
//! - the wallet's own change, reclassified the way an earlier release wrote
//!   it, restored with its prefix and suffix and the wallet's own identity
//!   key (or an empty sender), signs;
//! - a received payment reclassified the same way is restored with its
//!   prefix, suffix and the sender key the row held, and signs; an empty
//!   sender derives another key and is refused;
//! - at the storage door, a sender that is neither empty nor a compressed
//!   public key is refused before anything is written.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::collections::HashMap;

use bsv_rs::primitives::bsv::{parse_transaction, TxInput, TxOutput};
use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, Spend, SpendParams, UnlockingScript};
use bsv_rs::wallet::{
    Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput, InternalizeActionArgs,
    KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel, SignActionArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{beef_paying, other_lock, p2pkh_bytes, paying_derived, payment};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "c2VuZGVyLXByZWZpeA==";
const SUFFIX: &str = "c2VuZGVyLXN1ZmZpeA==";

/// The key `root` derives for itself under `prefix` and `suffix`.
fn self_key(root: &PrivateKey, prefix: &str, suffix: &str) -> PublicKey {
    ProtoWallet::new(Some(root.clone()))
        .key_deriver()
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            &format!("{prefix} {suffix}"),
            &Counterparty::Self_,
            true,
        )
        .unwrap()
}

fn args(tx: Vec<u8>, vout: u32, prefix: &str, suffix: &str, sender: &str) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(vout, prefix, suffix, sender)],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    }
}

/// A `createAction` of 1000 satoshis to another key, funded by the
/// wallet's change: signed and processed, or (`deferred`) signed by a
/// `signAction` after. The signed transaction, or the refusal's text.
async fn spend(wallet: &TestWallet, deferred: bool) -> Result<Vec<u8>, String> {
    let created = wallet
        .create_action(
            CreateActionArgs {
                description: "a spend of the wallet's change".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: other_lock().to_binary(),
                    satoshis: 1_000,
                    output_description: "a payment".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: None,
                options: Some(CreateActionOptions {
                    sign_and_process: Some(!deferred),
                    no_send: Some(true),
                    randomize_outputs: Some(false),
                    ..Default::default()
                }),
            },
            "test.local",
        )
        .await
        .map_err(|e| e.to_string())?;
    if !deferred {
        return Ok(created.tx.expect("the signed transaction"));
    }
    let signable = created
        .signable_transaction
        .expect("a signable transaction");
    let signed = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: String::from_utf8(signable.reference).unwrap(),
                options: None,
            },
            "test.local",
        )
        .await
        .map_err(|e| e.to_string())?;
    let atomic = signed.tx.expect("the signed transaction");
    let beef = bsv_rs::transaction::Beef::from_binary(&atomic).expect("an AtomicBEEF");
    let txid = beef.atomic_txid.clone().expect("its txid");
    Ok(beef
        .find_txid(&txid)
        .expect("the signed transaction")
        .raw_tx()
        .expect("raw")
        .to_vec())
}

/// Runs the input of `raw` spending `txid`.`vout` against `lock` holding
/// `satoshis` in bsv-rs's script interpreter: whether it verifies.
fn verify_spend(raw: &[u8], txid: &str, vout: u32, satoshis: u64, lock: &[u8]) -> bool {
    let parsed = parse_transaction(raw).expect("a transaction");
    let mut outpoint: [u8; 32] = hex::decode(txid).unwrap().try_into().unwrap();
    outpoint.reverse();
    let index = parsed
        .inputs
        .iter()
        .position(|i| i.txid == outpoint && i.output_index == vout)
        .expect("the spend selects the coin");
    let ours = &parsed.inputs[index];
    let other_inputs: Vec<TxInput> = parsed
        .inputs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, i)| i.clone())
        .collect();
    let outputs: Vec<TxOutput> = parsed.outputs.clone();
    let mut spend = Spend::new(SpendParams {
        source_txid: ours.txid,
        source_output_index: vout,
        source_satoshis: satoshis,
        locking_script: LockingScript::from_binary(lock).unwrap(),
        transaction_version: parsed.version,
        other_inputs,
        outputs,
        input_index: index,
        unlocking_script: UnlockingScript::from_binary(&ours.script).unwrap(),
        input_sequence: ours.sequence,
        lock_time: parsed.locktime,
        memory_limit: None,
        work_limit: None,
    });
    matches!(spend.validate(), Ok(true))
}

/// The row's sender identity key, spendable flag and spender.
async fn held(wallet: &TestWallet, txid: &str, vout: u32) -> (Option<String>, bool, Option<i64>) {
    sqlx::query_as(
        "SELECT sender_identity_key, spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?",
    )
    .bind(txid)
    .bind(vout)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

/// A wallet of `root` holding one coin: a wallet payment to the key it
/// derives for itself, internalized with `sender`.
async fn self_paid(root: &PrivateKey, sender: &str) -> (TestWallet, String, Vec<u8>) {
    let lock = p2pkh_bytes(&self_key(root, PREFIX, SUFFIX));
    let (tx, txid) = beef_paying(LockingScript::from_binary(&lock).unwrap());
    let wallet = common::wallet("internalize-sender-spend-tests", root, &txid).await;
    wallet
        .internalize_action(args(tx, 0, PREFIX, SUFFIX, sender), "test.local")
        .await
        .expect("the wallet's own payment");
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
    (wallet, txid, lock)
}

#[tokio::test]
async fn a_self_payment_with_an_empty_sender_is_stored_with_no_sender_and_signs() {
    for deferred in [false, true] {
        let root = PrivateKey::random();
        let (wallet, txid, lock) = self_paid(&root, "").await;
        let row = held(&wallet, &txid, 0).await;
        println!("empty sender, the row: {row:?}");
        assert_eq!(row, (None, true, None));
        let signed = spend(&wallet, deferred).await;
        println!(
            "empty sender, deferred={deferred}: {:?}",
            signed.as_ref().map(|_| "signed")
        );
        let signed = signed.unwrap();
        assert!(verify_spend(&signed, &txid, 0, 9_500, &lock));
        // The interpreter refuses the same unlocking script against another
        // lock and another amount.
        assert!(!verify_spend(
            &signed,
            &txid,
            0,
            9_500,
            &other_lock().to_binary()
        ));
        assert!(!verify_spend(&signed, &txid, 0, 9_499, &lock));
    }
}

#[tokio::test]
async fn a_row_holding_an_empty_sender_signs() {
    for deferred in [false, true] {
        let root = PrivateKey::random();
        let identity = root.public_key().to_hex();
        let (wallet, txid, lock) = self_paid(&root, &identity).await;
        // The row as an earlier release wrote it for an empty sender.
        sqlx::query("UPDATE outputs SET sender_identity_key = '' WHERE txid = ? AND vout = 0")
            .bind(&txid)
            .execute(wallet.storage().writer())
            .await
            .unwrap();
        let signed = spend(&wallet, deferred).await;
        println!(
            "stored empty sender, deferred={deferred}: {:?}",
            signed.as_ref().map(|_| "signed")
        );
        assert!(verify_spend(&signed.unwrap(), &txid, 0, 9_500, &lock));
    }
}

/// A wallet whose one spendable coin is the change of an action it made,
/// the change row reclassified the way an earlier release wrote a basket
/// insertion (custom, not change, derivation and sender cleared). The
/// wallet, the action's AtomicBEEF, its txid, the change's vout, satoshis,
/// lock, prefix and suffix.
struct Reclassified {
    wallet: TestWallet,
    tx: Vec<u8>,
    txid: String,
    vout: u32,
    satoshis: u64,
    lock: Vec<u8>,
    prefix: String,
    suffix: String,
}

async fn reclassified_change(root: &PrivateKey) -> Reclassified {
    let sender = PrivateKey::random().public_key();
    let (bytes, paid) = paying_derived(root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-sender-spend-tests", root, &paid).await;
    wallet
        .internalize_action(
            args(bytes.clone(), 0, PREFIX, SUFFIX, &sender.to_hex()),
            "test.local",
        )
        .await
        .expect("the payment funds the wallet");
    let raw = spend(&wallet, false).await.expect("the action with change");
    let txid = bsv_rs::transaction::Transaction::from_binary(&raw)
        .unwrap()
        .id();
    let mut beef = bsv_rs::transaction::Beef::from_binary(&bytes).unwrap();
    beef.merge_raw_tx(raw, None);
    let tx = beef.to_binary_atomic(&txid).unwrap();
    let pool = wallet.storage().writer();
    // As if the action had been sent: `unproven`, its change spendable.
    for sql in [
        "UPDATE transactions SET status = 'unproven' WHERE txid = ?",
        "UPDATE outputs SET spendable = 1 WHERE txid = ? AND change = 1",
    ] {
        sqlx::query(sql).bind(&txid).execute(pool).await.unwrap();
    }
    let (vout, satoshis, lock, prefix, suffix, held_sender): (
        u32,
        i64,
        Vec<u8>,
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT vout, satoshis, locking_script, derivation_prefix, derivation_suffix, \
         sender_identity_key FROM outputs WHERE txid = ? AND change = 1",
    )
    .bind(&txid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(held_sender, None, "the wallet's own change holds no sender");
    reclassify(&wallet, &txid, vout).await;
    assert_eq!(wallet.balance().await.unwrap().total, 0);
    Reclassified {
        wallet,
        tx,
        txid,
        vout,
        satoshis: satoshis as u64,
        lock,
        prefix,
        suffix,
    }
}

/// The row an earlier release wrote for a basket insertion into "tokens".
async fn reclassify(wallet: &TestWallet, txid: &str, vout: u32) {
    let pool = wallet.storage().writer();
    sqlx::query(
        "INSERT OR IGNORE INTO output_baskets (user_id, name, created_at, updated_at) \
         SELECT user_id, 'tokens', created_at, updated_at FROM output_baskets WHERE name = 'default'",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE name = 'tokens'), \
         type = 'custom', change = 0, custom_instructions = NULL, derivation_prefix = NULL, \
         derivation_suffix = NULL, sender_identity_key = NULL WHERE txid = ? AND vout = ?",
    )
    .bind(txid)
    .bind(vout)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn own_change_restored_with_the_wallets_identity_key_signs() {
    let root = PrivateKey::random();
    let r = reclassified_change(&root).await;
    let identity = root.public_key().to_hex();
    r.wallet
        .internalize_action(
            args(r.tx.clone(), r.vout, &r.prefix, &r.suffix, &identity),
            "test.local",
        )
        .await
        .expect("the wallet's own identity key restores it");
    assert_eq!(r.wallet.balance().await.unwrap().total, r.satoshis);
    println!(
        "own change, identity sender, the row: {:?}",
        held(&r.wallet, &r.txid, r.vout).await
    );
    let signed = spend(&r.wallet, false).await;
    println!(
        "own change, identity sender: {:?}",
        signed.as_ref().map(|_| "signed")
    );
    assert!(verify_spend(
        &signed.unwrap(),
        &r.txid,
        r.vout,
        r.satoshis,
        &r.lock
    ));
}

#[tokio::test]
async fn own_change_restored_with_an_empty_sender_signs() {
    for deferred in [false, true] {
        let root = PrivateKey::random();
        let r = reclassified_change(&root).await;
        r.wallet
            .internalize_action(
                args(r.tx.clone(), r.vout, &r.prefix, &r.suffix, ""),
                "test.local",
            )
            .await
            .expect("the held derivation restores it");
        assert_eq!(r.wallet.balance().await.unwrap().total, r.satoshis);
        let row = held(&r.wallet, &r.txid, r.vout).await;
        println!("own change, empty sender, the row: {row:?}");
        assert_eq!(row, (None, true, None));
        let signed = spend(&r.wallet, deferred).await;
        println!(
            "own change, empty sender, deferred={deferred}: {:?}",
            signed.as_ref().map(|_| "signed")
        );
        assert!(verify_spend(
            &signed.unwrap(),
            &r.txid,
            r.vout,
            r.satoshis,
            &r.lock
        ));
    }
}

#[tokio::test]
async fn a_reclassified_received_payment_restored_with_its_sender_signs() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let lock = p2pkh_bytes(&common::derived_key(&root, &sender, PREFIX, SUFFIX));
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-sender-spend-tests", &root, &txid).await;
    wallet
        .internalize_action(
            args(tx.clone(), 0, PREFIX, SUFFIX, &sender.to_hex()),
            "test.local",
        )
        .await
        .expect("the payment");
    reclassify(&wallet, &txid, 0).await;
    assert_eq!(wallet.balance().await.unwrap().total, 0);

    let without = wallet
        .internalize_action(args(tx.clone(), 0, PREFIX, SUFFIX, ""), "test.local")
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    println!("received payment, empty sender: {without:?}");
    assert!(without
        .as_ref()
        .is_err_and(|e| e.starts_with("wallet error: Derivation mismatch for output 0")));
    assert_eq!(wallet.balance().await.unwrap().total, 0);

    wallet
        .internalize_action(args(tx, 0, PREFIX, SUFFIX, &sender.to_hex()), "test.local")
        .await
        .expect("prefix, suffix and sender restore it");
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
    let signed = spend(&wallet, false).await;
    println!(
        "received payment, its sender: {:?}",
        signed.as_ref().map(|_| "signed")
    );
    assert!(verify_spend(&signed.unwrap(), &txid, 0, 9_500, &lock));
}

const INVALID_SENDER: &str =
    "Wallet payment outputs require a sender_identity_key that is empty or a compressed public key";

#[tokio::test]
async fn the_storage_door_refuses_a_sender_that_is_not_a_key() {
    let root = PrivateKey::random();
    let lock = p2pkh_bytes(&self_key(&root, PREFIX, SUFFIX));
    let (tx, txid) = beef_paying(LockingScript::from_binary(&lock).unwrap());
    let wallet = common::wallet("internalize-sender-spend-tests", &root, &txid).await;
    let off_curve = format!("02{}", "00".repeat(32));
    let uncompressed = hex::encode(root.public_key().to_uncompressed());
    // 66 hex characters of a point's x with the uncompressed prefix.
    let prefix_04 = format!("04{}", &root.public_key().to_hex()[2..]);
    for sender in [
        "not a key",
        "02",
        off_curve.as_str(),
        uncompressed.as_str(),
        prefix_04.as_str(),
    ] {
        let storage = common::internalize_at(
            "storage",
            &wallet,
            &root,
            args(tx.clone(), 0, PREFIX, SUFFIX, sender),
        )
        .await;
        println!("storage door, sender {sender:?}: {storage:?}");
        assert_eq!(
            storage,
            Err(format!("Validation error: {}", INVALID_SENDER)),
            "{sender:?}"
        );
        assert_eq!(common::stored_outputs(&wallet).await, 0);
    }
    // The wallet door's own parse refuses what is not a point; an
    // uncompressed key parses and derives the coin's key, and is refused
    // at the storage door it reaches.
    for sender in ["not a key", off_curve.as_str(), uncompressed.as_str()] {
        let at_wallet = common::internalize_at(
            "wallet",
            &wallet,
            &root,
            args(tx.clone(), 0, PREFIX, SUFFIX, sender),
        )
        .await;
        println!("wallet door, sender {sender:?}: {at_wallet:?}");
        if sender == uncompressed {
            assert_eq!(
                at_wallet,
                Err(format!(
                    "wallet error: Validation error: {}",
                    INVALID_SENDER
                ))
            );
        } else {
            assert!(at_wallet
                .as_ref()
                .is_err_and(|e| e.starts_with("wallet error: Invalid sender_identity_key")));
        }
        assert_eq!(common::stored_outputs(&wallet).await, 0);
    }
}
