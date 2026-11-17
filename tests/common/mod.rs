//! Helpers shared by the internalize and BEEF link budget integration tests:
//! a broadcaster's acceptance, a wallet over in-memory SQLite, the count of
//! stored outputs, the P2PKH of a key, the BRC-29 key a recipient derives,
//! an AtomicBEEF of one output, and a sweep of one parent past bsv-rs
//! 0.8.11's default link budget; a wallet payment, an AtomicBEEF paying a
//! derived key, and an internalization at the wallet or the storage door.
//!
//! Each test file uses part of this module, hence `dead_code` allowed.

#![allow(dead_code)]

pub mod forwarding;

use bsv_rs::primitives::bsv::{parse_transaction, TxInput, TxOutput};
use bsv_rs::primitives::{hash160, PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, Spend, SpendParams, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, InternalizeActionArgs, InternalizeOutput, KeyDeriverApi, ProtoWallet, Protocol,
    SecurityLevel, WalletInterface, WalletPayment,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    AuthId, PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

/// Outputs of the one parent the sweep spends: past the 702 P2PKH outputs
/// bsv-rs 0.8.11 states as the default link budget's single-sweep reach.
pub const SWEPT: u32 = 800;

/// A 25-byte P2PKH paying neither party of the tests.
pub const OTHER_HASH: &str = "dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa";

/// The P2PKH of `OTHER_HASH`.
pub fn other_lock() -> LockingScript {
    LockingScript::from_hex(&format!("76a914{OTHER_HASH}88ac")).unwrap()
}

/// The 25 bytes of the P2PKH of `pubkey`.
pub fn p2pkh_bytes(pubkey: &PublicKey) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(&pubkey.to_compressed()));
    script.extend([0x88, 0xac]);
    script
}

/// The key the recipient `root` derives for the remittance from `sender`
/// with the derivation `prefix` and `suffix`.
pub fn derived_key(root: &PrivateKey, sender: &PublicKey, prefix: &str, suffix: &str) -> PublicKey {
    ProtoWallet::new(Some(root.clone()))
        .key_deriver()
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            &format!("{prefix} {suffix}"),
            &Counterparty::Other(sender.clone()),
            true,
        )
        .unwrap()
}

/// A broadcaster's acceptance of `txid`.
pub fn accepted(txid: &str) -> PostBeefResult {
    PostBeefResult {
        name: "arc".to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "success".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: None,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

/// A wallet of `root` over in-memory SQLite named `name`, whose mock
/// broadcaster accepts `txid`.
pub async fn wallet(
    name: &str,
    root: &PrivateKey,
    txid: &str,
) -> Wallet<StorageSqlx, MockWalletServices> {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let identity = root.public_key().to_hex();
    storage.migrate(name, &identity).await.expect("migrate");
    storage.make_available().await.expect("make_available");
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(txid)]))
        .build();
    Wallet::new(Some(root.clone()), storage, services)
        .await
        .expect("wallet")
}

/// The number of outputs the wallet's store holds.
pub async fn stored_outputs(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

fn input(txid: &str, vout: u32) -> TransactionInput {
    let mut input = TransactionInput::new(txid.to_string(), vout);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    input
}

/// A proven parent of `outputs` outputs locked by `other_lock`.
pub fn proven_parent(outputs: u32) -> Transaction {
    let mut parent = Transaction::new();
    parent.inputs.push(input(&"11".repeat(32), 0));
    for _ in 0..outputs {
        parent.outputs.push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: other_lock(),
            change: false,
        });
    }
    parent
}

/// A child spending outputs 0 to `spent - 1` of `parent_txid` into one
/// output locked by `lock`.
pub fn child(parent_txid: &str, spent: u32, lock: LockingScript) -> Transaction {
    let mut child = Transaction::new();
    child.version = 1;
    for vout in 0..spent {
        child.inputs.push(input(parent_txid, vout));
    }
    child.outputs.push(TransactionOutput {
        satoshis: Some(u64::from(spent) * 1_000 - 500),
        locking_script: lock,
        change: false,
    });
    child
}

/// An AtomicBEEF of a proven parent of `SWEPT` outputs and a child sweeping
/// every one of them into one output locked by `lock`.
pub fn sweep_beef(lock: LockingScript) -> (Vec<u8>, String) {
    let parent = proven_parent(SWEPT);
    let parent_txid = parent.id();
    let child = child(&parent_txid, SWEPT, lock);
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// An AtomicBEEF of a child with one output locked by `lock`, spending a
/// parent the BEEF carries whole with its BUMP.
pub fn beef_paying(lock: LockingScript) -> (Vec<u8>, String) {
    beef_paying_with(lock, true)
}

/// An AtomicBEEF of one output locked by `lock`, spending a parent the BEEF
/// proves with a BUMP at height 100_003, and its txid: the parent carried
/// whole when `whole`, else by its txid alone beside its BUMP.
pub fn beef_paying_with(lock: LockingScript, whole: bool) -> (Vec<u8>, String) {
    let mut parent = Transaction::new();
    let mut parent_input = TransactionInput::new("22".repeat(32), 0);
    parent_input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(parent_input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_hex(&format!("76a914{OTHER_HASH}88ac")).unwrap(),
        change: false,
    });
    let parent_txid = parent.id();
    let mut child = Transaction::new();
    child.version = 1;
    let mut input = TransactionInput::new(parent_txid.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: lock,
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_003));
    if whole {
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
    } else {
        beef.merge_txid_only(parent_txid);
    }
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// The storage door's `AuthId` for the wallet of `root`.
pub async fn auth(wallet: &Wallet<StorageSqlx, MockWalletServices>, root: &PrivateKey) -> AuthId {
    let identity = root.public_key().to_hex();
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&identity)
        .await
        .unwrap();
    AuthId::with_user_id(&identity, user.user_id)
}

/// A wallet payment of output `vout` under `prefix` and `suffix` from
/// `sender`.
pub fn payment(vout: u32, prefix: &str, suffix: &str, sender: &str) -> InternalizeOutput {
    InternalizeOutput {
        output_index: vout,
        protocol: "wallet payment".to_string(),
        payment_remittance: Some(WalletPayment {
            derivation_prefix: prefix.to_string(),
            derivation_suffix: suffix.to_string(),
            sender_identity_key: sender.to_string(),
        }),
        insertion_remittance: None,
    }
}

/// An AtomicBEEF of one output locked to the key `root` derives for
/// `sender` under `prefix` and `suffix`, and its txid.
pub fn paying_derived(
    root: &PrivateKey,
    sender: &PublicKey,
    prefix: &str,
    suffix: &str,
) -> (Vec<u8>, String) {
    let derived = derived_key(root, sender, prefix, suffix);
    beef_paying(LockingScript::from_binary(&p2pkh_bytes(&derived)).unwrap())
}

/// `args` internalized at `door` (`wallet` or `storage`): whether accepted,
/// or the refusal's text.
pub async fn internalize_at(
    door: &str,
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
    root: &PrivateKey,
    args: InternalizeActionArgs,
) -> Result<bool, String> {
    if door == "wallet" {
        wallet
            .internalize_action(args, "test.local")
            .await
            .map(|r| r.accepted)
            .map_err(|e| e.to_string())
    } else {
        wallet
            .storage()
            .internalize_action(&auth(wallet, root).await, args)
            .await
            .map(|r| r.base.accepted)
            .map_err(|e| e.to_string())
    }
}

/// Whether `raw` spends `txid`.`vout` with an unlocking script that runs
/// clean against `lock` holding `satoshis`; `None` when it does not spend it.
pub fn spends(raw: &[u8], txid: &str, vout: u32, satoshis: u64, lock: &[u8]) -> Option<bool> {
    let parsed = parse_transaction(raw).expect("a transaction");
    let mut outpoint: [u8; 32] = hex::decode(txid).unwrap().try_into().unwrap();
    outpoint.reverse();
    let index = parsed
        .inputs
        .iter()
        .position(|i| i.txid == outpoint && i.output_index == vout)?;
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
    Some(matches!(spend.validate(), Ok(true)))
}

/// A wallet of `root` on `manager` with `services`. `Wallet::new` calls the
/// storage's `set_services`, which on a `WalletStorageManager` is a sync
/// trait method that takes the manager's locks with a blocking read; a
/// blocking read panics on a runtime thread, so the wallet is built on a
/// thread of its own that holds the runtime's handle while the runtime's
/// workers drive the store's I/O (hence a multi-thread runtime).
pub fn wallet_on_manager<V>(
    root: &PrivateKey,
    manager: bsv_wallet_toolbox_rs::WalletStorageManager,
    services: V,
) -> Wallet<bsv_wallet_toolbox_rs::WalletStorageManager, V>
where
    V: bsv_wallet_toolbox_rs::WalletServices + Send + Sync + 'static,
{
    let handle = tokio::runtime::Handle::current();
    let key = root.clone();
    std::thread::spawn(move || {
        let _entered = handle.enter();
        futures::executor::block_on(Wallet::new(Some(key), manager, services))
    })
    .join()
    .unwrap()
    .unwrap()
}
