//! Host backends at the createAction and signAction signing boundaries.

use super::{verify_own, SourceTx};
use crate::services::mock::MockWalletServices;
use crate::storage::sqlx::StorageSqlxOptions;
use crate::storage::traits::WalletStorageWriter;
use crate::{Result, SpendRequest, SpendVerificationContext, SpendVerifier, StorageSqlx, Wallet};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::template::{SignOutputs, SigningContext};
use bsv_rs::script::{templates::P2PKH, LockingScript, ProtocolEra, Script, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, CreateActionArgs, CreateActionInput, CreateActionOptions, CreateActionOutput,
    KeyDeriverApi, Outpoint, ProtoWallet, Protocol, SecurityLevel, SignActionArgs, SignActionSpend,
    SignableTransaction, WalletInterface,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

type Item = (SpendRequest, SpendVerificationContext);
type StoredAction = (i64, String, Option<String>, Option<Vec<u8>>);
const HEIGHT: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT + 100;
const SATOSHIS: u64 = 100_000;

struct RecordingVerifier {
    accept: AtomicBool,
    skip: Option<usize>,
    considered: Mutex<Vec<Item>>,
    calls: Mutex<Vec<Item>>,
}

impl RecordingVerifier {
    fn new(accept: bool, skip: Option<usize>) -> Self {
        Self {
            accept: AtomicBool::new(accept),
            skip,
            considered: Mutex::new(Vec::new()),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl SpendVerifier for RecordingVerifier {
    fn should_verify_spend(
        &self,
        spend: &SpendRequest,
        context: &SpendVerificationContext,
    ) -> bool {
        self.considered
            .lock()
            .unwrap()
            .push((spend.clone(), *context));
        self.skip != Some(spend.vin)
    }

    fn verify_spend(
        &self,
        spend: &SpendRequest,
        context: &SpendVerificationContext,
    ) -> Result<bool> {
        self.calls.lock().unwrap().push((spend.clone(), *context));
        Ok(self.accept.load(Ordering::SeqCst))
    }
}

struct BatchVerifier {
    selection: RecordingVerifier,
    batches: Mutex<Vec<Vec<Item>>>,
    count: usize,
}

impl SpendVerifier for BatchVerifier {
    fn should_verify_spend(
        &self,
        spend: &SpendRequest,
        context: &SpendVerificationContext,
    ) -> bool {
        self.selection.should_verify_spend(spend, context)
    }

    fn verify_spend(&self, _: &SpendRequest, _: &SpendVerificationContext) -> Result<bool> {
        panic!("the signing boundary must use the batch override")
    }

    fn verify_spends_batch(&self, items: &[Item]) -> Result<Vec<bool>> {
        self.batches.lock().unwrap().push(items.to_vec());
        Ok(vec![true; self.count])
    }
}

fn batch(count: usize) -> Arc<BatchVerifier> {
    Arc::new(BatchVerifier {
        selection: RecordingVerifier::new(true, Some(1)),
        batches: Mutex::new(Vec::new()),
        count,
    })
}

struct Scene {
    wallet: Wallet<StorageSqlx, MockWalletServices>,
    parent: Transaction,
    key: PrivateKey,
}

async fn scene(verifier: Arc<dyn SpendVerifier>, count: usize) -> Scene {
    let root = PrivateKey::from_hex(&format!("{:064x}", 1)).unwrap();
    let proto = ProtoWallet::new(Some(root.clone()));
    let protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
    let key = proto
        .key_deriver()
        .derive_private_key(&protocol, "cHJlZml4 c3VmZml4", &Counterparty::Self_)
        .unwrap();
    let hash = bsv_rs::primitives::hash160(&key.public_key().to_compressed());
    let lock = [&[0x76, 0xa9, 0x14][..], &hash[..], &[0x88, 0xac][..]].concat();
    let storage = StorageSqlx::new_with_options(
        "sqlite::memory:",
        StorageSqlxOptions {
            script_verifier: Some(verifier),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    storage
        .migrate("host-signing", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    // The clone must retain the configured backend.
    let services = MockWalletServices::builder()
        .height(ProtocolEra::MAINNET_CHRONICLE_HEIGHT + 100)
        .build();
    let wallet = Wallet::new(Some(root), storage.clone(), services)
        .await
        .unwrap();
    let (user, _) = storage
        .find_or_insert_user(wallet.identity_key())
        .await
        .unwrap();
    let basket_id = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap()
        .basket_id;
    sqlx::query("UPDATE output_baskets SET number_of_desired_utxos = 0")
        .execute(storage.writer())
        .await
        .unwrap();
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("00".repeat(32), u32::MAX);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0]).unwrap());
    parent.inputs.push(input);
    parent.outputs = vec![
        TransactionOutput {
            satoshis: Some(SATOSHIS),
            locking_script: LockingScript::from_binary(&lock).unwrap(),
            change: false,
        };
        count
    ];
    let txid = parent.id();
    let raw = parent.to_binary();
    let now = chrono::Utc::now();
    sqlx::query("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)")
        .bind(&txid).bind(HEIGHT).bind("aa".repeat(32)).bind(&txid)
        .bind(MerklePath::from_coinbase_txid(&txid, HEIGHT).to_binary()).bind(&raw).bind(now).bind(now)
        .execute(storage.writer()).await.unwrap();
    let id = sqlx::query("INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) VALUES (?, 'completed', 'funding', 0, ?, 1, 0, 'funding', ?, ?, ?, ?)")
        .bind(user.user_id).bind(i64::try_from(SATOSHIS * u64::try_from(count).unwrap()).unwrap()).bind(&txid).bind(&raw).bind(now).bind(now)
        .execute(storage.writer()).await.unwrap().last_insert_rowid();
    for vout in 0..count {
        sqlx::query("INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'P2PKH', 1, 1, 'cHJlZml4', 'c3VmZml4', 'storage', 'change', 'funding', ?, ?)")
            .bind(user.user_id).bind(id).bind(basket_id).bind(i64::try_from(vout).unwrap()).bind(i64::try_from(SATOSHIS).unwrap())
            .bind(&lock).bind(&txid).bind(now).bind(now).execute(storage.writer()).await.unwrap();
    }
    Scene {
        wallet,
        parent,
        key,
    }
}

fn payment(satoshis: u64) -> CreateActionArgs {
    let output = CreateActionOutput {
        locking_script: vec![0x51],
        satoshis,
        output_description: "local payment".into(),
        basket: None,
        custom_instructions: None,
        tags: None,
    };
    CreateActionArgs {
        description: "host signing witness".into(),
        outputs: Some(vec![output]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            randomize_outputs: Some(false),
            ..Default::default()
        }),
        input_beef: None,
        inputs: None,
        lock_time: None,
        version: None,
        labels: None,
    }
}

async fn deferred(scene: &Scene, amount: u64) -> SignableTransaction {
    let mut args = payment(amount);
    args.options.as_mut().unwrap().sign_and_process = Some(false);
    scene
        .wallet
        .create_action(args, None)
        .await
        .unwrap()
        .signable_transaction
        .unwrap()
}

async fn sign(
    scene: &Scene,
    signable: &SignableTransaction,
) -> bsv_rs::Result<bsv_rs::wallet::SignActionResult> {
    scene
        .wallet
        .sign_action(
            SignActionArgs {
                reference: bsv_rs::primitives::to_base64(&signable.reference),
                spends: HashMap::new(),
                options: None,
            },
            None,
        )
        .await
}

async fn snapshot(scene: &Scene) -> Vec<String> {
    let storage = scene.wallet.storage();
    let mut rows = Vec::new();
    for table in [
        "commissions",
        "output_baskets",
        "output_tags",
        "output_tags_map",
        "outputs",
        "proven_tx_reqs",
        "transactions",
        "tx_labels",
        "tx_labels_map",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(storage.pool())
            .await
            .unwrap();
        rows.push(format!("{table}={count}"));
    }
    let locks: Vec<(i64, bool, Option<i64>)> =
        sqlx::query_as("SELECT output_id, spendable, spent_by FROM outputs ORDER BY output_id")
            .fetch_all(storage.pool())
            .await
            .unwrap();
    rows.extend(locks.iter().map(|row| format!("{row:?}")));
    let actions: Vec<StoredAction> = sqlx::query_as(
        "SELECT transaction_id, status, txid, raw_tx FROM transactions ORDER BY transaction_id",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();
    rows.extend(actions.iter().map(|row| format!("{row:?}")));
    let sequences: Vec<(String, i64)> =
        sqlx::query_as("SELECT name, seq FROM sqlite_sequence ORDER BY name")
            .fetch_all(storage.pool())
            .await
            .unwrap();
    rows.extend(sequences.iter().map(|row| format!("{row:?}")));
    rows
}

fn interpreter(scene: &Scene, raw: &[u8]) -> Result<()> {
    verify_own(
        raw,
        &HashMap::from([(
            scene.parent.id(),
            SourceTx {
                raw: scene.parent.to_binary(),
                proven_height: Some(u64::from(HEIGHT)),
            },
        )]),
        None,
        "main",
        None,
    )
}

fn refusal<T>(result: bsv_rs::Result<T>, expected: &str) {
    match result {
        Err(bsv_rs::Error::WalletRefusal {
            class: bsv_rs::WalletRefusalClass::InvalidParameter,
            message,
        }) => {
            assert_eq!(message, expected);
            eprintln!("{message}");
        }
        Err(error) => panic!("unexpected refusal: {error}"),
        Ok(_) => panic!("the signing boundary ignored the refusal"),
    }
}

fn host_reason(item: &Item, reason: &str) -> String {
    format!(
        "spend refused at offset 5, input 0 of {}: {reason}",
        bsv_rs::transaction::beef_stream::display_hex(&item.0.txid)
    )
}

async fn assert_host_refusal<T>(
    scene: &Scene,
    host: &RecordingVerifier,
    result: bsv_rs::Result<T>,
    before: &[String],
) {
    assert!(
        result.is_err(),
        "the signing boundary ignored the host refusal"
    );
    let item = host.calls.lock().unwrap()[0].clone();
    interpreter(scene, &item.0.body.raw).unwrap();
    refusal(
        result,
        &host_reason(&item, "host script verifier refused the spend"),
    );
    assert_eq!(
        snapshot(scene).await,
        before,
        "a refusal must preserve rows and input locks"
    );
    assert_eq!(scene.wallet.services().call_count("post_beef"), 0);
}

#[tokio::test]
async fn create_action_host_refusal_releases_inputs() {
    let host = Arc::new(RecordingVerifier::new(false, None));
    let scene = scene(host.clone(), 1).await;
    let before = snapshot(&scene).await;
    let mut args = payment(1_000);
    args.options.as_mut().unwrap().no_send = Some(false);
    let result = scene.wallet.create_action(args, None).await;
    assert_host_refusal(&scene, &host, result, &before).await;
    host.accept.store(true, Ordering::SeqCst);
    scene
        .wallet
        .create_action(payment(1_000), None)
        .await
        .unwrap();
    assert_eq!(
        host.calls.lock().unwrap().len(),
        2,
        "the same input is available for retry"
    );
    eprintln!(
        "createAction: valid signature refused, rows unchanged, input released, retry accepted"
    );
}

#[tokio::test]
async fn sign_action_host_refusal_keeps_unsigned_action() {
    let host = Arc::new(RecordingVerifier::new(false, None));
    let scene = scene(host.clone(), 1).await;
    let signable = deferred(&scene, 1_000).await;
    let before = snapshot(&scene).await;
    let result = sign(&scene, &signable).await;
    assert_host_refusal(&scene, &host, result, &before).await;
    host.accept.store(true, Ordering::SeqCst);
    sign(&scene, &signable).await.unwrap();
    assert_eq!(host.calls.lock().unwrap().len(), 2);
    eprintln!(
        "signAction: valid signature refused, unsigned action and locks retained, retry accepted"
    );
}

fn caller_input(scene: &Scene, script: Option<Vec<u8>>) -> CreateActionInput {
    CreateActionInput {
        outpoint: Outpoint {
            txid: hex::decode(scene.parent.id()).unwrap().try_into().unwrap(),
            vout: 0,
        },
        unlocking_script_length: script.is_none().then_some(110),
        unlocking_script: script,
        input_description: "caller signs a funded input".into(),
        sequence_number: None,
    }
}

fn caller_script(scene: &Scene, bad: bool) -> Vec<u8> {
    let mut draft = Transaction::new();
    draft
        .inputs
        .push(TransactionInput::new(scene.parent.id(), 0));
    let lock = Script::from_binary(&scene.parent.outputs[0].locking_script.to_binary()).unwrap();
    let mut script = P2PKH::unlock(&scene.key, SignOutputs::None, true)
        .sign(&SigningContext::new(&draft.to_binary(), 0, SATOSHIS, &lock))
        .unwrap()
        .to_binary();
    if bad {
        let sighash = usize::from(script[0]);
        script[sighash] &= !0x40;
    }
    script
}

async fn skipped_signing(defer: bool) {
    for bad in [false, true] {
        let host = Arc::new(RecordingVerifier::new(bad, Some(0)));
        let scene = scene(host.clone(), 1).await;
        sqlx::query("UPDATE outputs SET change = 0")
            .execute(scene.wallet.storage().writer())
            .await
            .unwrap();
        let script = caller_script(&scene, bad);
        let mut args = payment(1_000);
        args.inputs = Some(vec![caller_input(
            &scene,
            if defer { None } else { Some(script.clone()) },
        )]);
        args.options.as_mut().unwrap().sign_and_process = Some(!defer);
        let (passed, message) = if defer {
            let signable = scene
                .wallet
                .create_action(args, None)
                .await
                .unwrap()
                .signable_transaction
                .unwrap();
            let result = scene
                .wallet
                .sign_action(
                    SignActionArgs {
                        reference: bsv_rs::primitives::to_base64(&signable.reference),
                        spends: HashMap::from([(
                            0,
                            SignActionSpend {
                                unlocking_script: script,
                                sequence_number: None,
                            },
                        )]),
                        options: None,
                    },
                    None,
                )
                .await;
            (result.is_ok(), result.err().map(|e| e.to_string()))
        } else {
            let result = scene.wallet.create_action(args, None).await;
            (result.is_ok(), result.err().map(|e| e.to_string()))
        };
        let items = host.considered.lock().unwrap();
        assert_eq!(items.len(), 1);
        let expected = interpreter(&scene, &items[0].0.body.raw);
        assert_eq!(passed, expected.is_ok());
        assert_eq!(message, expected.err().map(|e| e.to_string()));
        assert!(host.calls.lock().unwrap().is_empty());
        assert_eq!(passed, !bad);
        eprintln!("deferred={defer}, invalid_signature={bad}: skipped host, interpreter accepted={passed}");
    }
}

#[tokio::test]
async fn create_action_unselected_input_uses_interpreter() {
    skipped_signing(false).await;
}

#[tokio::test]
async fn sign_action_unselected_input_uses_interpreter() {
    skipped_signing(true).await;
}

fn indices(items: &[Item]) -> Vec<usize> {
    items.iter().map(|(spend, _)| spend.vin).collect()
}

async fn batched_signing(defer: bool) {
    let host = batch(2);
    let scene = scene(host.clone(), 3).await;
    if defer {
        let signable = deferred(&scene, 250_000).await;
        sign(&scene, &signable).await.unwrap();
    } else {
        scene
            .wallet
            .create_action(payment(250_000), None)
            .await
            .unwrap();
    }
    let batches = host.batches.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(indices(&batches[0]), [0, 2]);
    assert_eq!(
        indices(&host.selection.considered.lock().unwrap()),
        [0, 1, 2]
    );
    assert!(Arc::ptr_eq(&batches[0][0].0.body, &batches[0][1].0.body));
    for (spend, context) in &batches[0] {
        assert_eq!(context.proven_height, Some(u64::from(HEIGHT)));
        assert_eq!(
            context.word,
            bsv_rs::script::ScriptFlags::standard(ProtocolEra::PostChronicle)
                .without(bsv_rs::script::ScriptFlags::UTXO_AFTER_CHRONICLE)
        );
        assert_eq!(spend.satoshis, SATOSHIS);
        assert_eq!(
            &*spend.script,
            scene.parent.outputs[0].locking_script.to_binary()
        );
    }
    interpreter(&scene, &batches[0][0].0.body.raw).unwrap();
    eprintln!("deferred={defer}: one batch, selected inputs [0, 2], input 1 interpreted, source heights retained");
}

#[tokio::test]
async fn create_action_batches_selected_inputs_once() {
    batched_signing(false).await;
}

#[tokio::test]
async fn sign_action_batches_selected_inputs_once() {
    batched_signing(true).await;
}

#[tokio::test]
async fn signing_batch_wrong_count_refuses_both_routes() {
    for defer in [false, true] {
        for count in [1, 3] {
            let host = batch(count);
            let scene = scene(host.clone(), 3).await;
            let signable = if defer {
                Some(deferred(&scene, 250_000).await)
            } else {
                None
            };
            let before = snapshot(&scene).await;
            let expected_reason =
                format!("host script verifier returned {count} verdicts for 2 spends");
            if let Some(signable) = signable {
                let result = sign(&scene, &signable).await;
                let item = host.batches.lock().unwrap()[0][0].clone();
                refusal(result, &host_reason(&item, &expected_reason));
            } else {
                let result = scene.wallet.create_action(payment(250_000), None).await;
                let item = host.batches.lock().unwrap()[0][0].clone();
                refusal(result, &host_reason(&item, &expected_reason));
            }
            assert_eq!(snapshot(&scene).await, before);
            assert_eq!(scene.wallet.services().call_count("post_beef"), 0);
        }
    }
}

#[tokio::test]
async fn create_caller_input_beef_uses_host_before_signing() {
    let host = Arc::new(RecordingVerifier::new(false, None));
    let scene = scene(host.clone(), 1).await;
    let mut child = Transaction::new();
    child
        .inputs
        .push(TransactionInput::new(scene.parent.id(), 0));
    child.outputs.push(TransactionOutput {
        satoshis: Some(SATOSHIS - 1_000),
        locking_script: scene.parent.outputs[0].locking_script.clone(),
        change: false,
    });
    let lock = Script::from_binary(&scene.parent.outputs[0].locking_script.to_binary()).unwrap();
    child.inputs[0].unlocking_script = Some(
        P2PKH::unlock(&scene.key, SignOutputs::All, false)
            .sign(&SigningContext::new(&child.to_binary(), 0, SATOSHIS, &lock))
            .unwrap(),
    );
    child.invalidate_caches();
    interpreter(&scene, &child.to_binary()).unwrap();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&scene.parent.id(), HEIGHT));
    beef.merge_raw_tx(scene.parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    let mut args = payment(1_000);
    args.input_beef = Some(beef.to_binary());
    args.inputs = Some(vec![CreateActionInput {
        outpoint: Outpoint {
            txid: hex::decode(child.id()).unwrap().try_into().unwrap(),
            vout: 0,
        },
        unlocking_script: None,
        unlocking_script_length: Some(110),
        input_description: "supplied child".into(),
        sequence_number: None,
    }]);
    args.options.as_mut().unwrap().sign_and_process = Some(false);
    let before = snapshot(&scene).await;
    let result = scene.wallet.create_action(args.clone(), None).await;
    let item = host.calls.lock().unwrap()[0].clone();
    assert_eq!(
        bsv_rs::transaction::beef_stream::display_hex(&item.0.txid),
        child.id()
    );
    refusal(
        result,
        &format!(
            "spend refused at offset {}, input 0 of {}: host script verifier refused the spend",
            item.0.body.inputs[0].at,
            child.id()
        ),
    );
    assert_eq!(snapshot(&scene).await, before);
    host.accept.store(true, Ordering::SeqCst);
    assert!(scene
        .wallet
        .create_action(args, None)
        .await
        .unwrap()
        .signable_transaction
        .is_some());
    eprintln!("caller inputBEEF: host refuses the live pre-check before deferred signing");
}
