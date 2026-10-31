//! A8 census scenes (auto-research charter, section 2): drives one door scene
//! in a loop for a profiler. Measuring only; no assertion is a gate.
//!
//! ```sh
//! cargo run --release --example a8_census -- <scene> <size> <iterations>
//! ```
//!
//! Scenes: `create N` (createAction allocating and signing N funded coins,
//! `no_send`), `process N` (the same, posted to the mock services),
//! `internalize 1` (the door bench's proven basket insertion),
//! `beef-internalize MB` and `beef-create MB` (an atomic BEEF of a standard
//! transaction of about MB megabytes with one signed P2PKH input, through
//! internalize's stream verification and through createAction's caller BEEF
//! pre-check). Prints each door call's window as epoch milliseconds so the
//! profile can be cut to the calls, and the loop's wall time.

#[path = "../benches/common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::template::{SignOutputs, SigningContext};
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{LockingScript, Script};
use bsv_rs::transaction::{Beef, ChainTracker, ChainTrackerError, MerklePath, Transaction};
use bsv_rs::transaction::{TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionInput, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, Outpoint, WalletInterface,
};
use bsv_wallet_toolbox_rs::{MonitorStorage, WalletStorageWriter};

use common::{
    funding_transaction, payment_args, seed, seed_shared, Store, HEIGHT, ORIGINATOR, SATOSHIS,
};

/// A post-Genesis height for the BEEF scenes' proven parent and tip, so the
/// spend is judged rather than refused as pre-Genesis. Not the door seed's
/// `HEIGHT`: every root at `HEIGHT` is accepted, so the BEEF parent's root is
/// checked against the set only at a height of its own.
const BEEF_HEIGHT: u32 = 900_100;
const BEEF_COIN: u64 = 1_000_000;

/// Accepts any root at the door seed's `HEIGHT` and, at any other height,
/// only the roots in the set (the BEEF parent's at `BEEF_HEIGHT`), with a tip
/// at `BEEF_HEIGHT`.
struct CensusRoots(HashSet<(u32, String)>);

#[async_trait::async_trait]
impl ChainTracker for CensusRoots {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(height == HEIGHT || self.0.contains(&(height, root.to_string())))
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(BEEF_HEIGHT)
    }

    async fn mining_tip(
        &self,
    ) -> Result<Option<bsv_rs::transaction::beef_stream::ChainTip>, ChainTrackerError> {
        #[allow(deprecated)]
        let tip = bsv_rs::transaction::beef_stream::ChainTip {
            height: BEEF_HEIGHT,
            median_time_past: 1_700_000_000,
            genesis_height: bsv_rs::block::ChainParams::mainnet().genesis_height,
            csv_height: 419_328,
            next_block_time: None,
        };
        Ok(Some(tip))
    }
}

/// The construction: a proven parent (a single-leaf proof at `BEEF_HEIGHT`)
/// paying `BEEF_COIN` to a P2PKH key; a child spending it with one
/// ALL|FORKID P2PKH input, output 0 a 10,000-satoshi P2PKH coin and output 1
/// an `OP_FALSE OP_RETURN` carrying `bytes` of data. Returns the atomic BEEF
/// of the child and the child's txid.
fn big_beef(bytes: usize) -> (Vec<u8>, String, String) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).expect("fixture key");
    let hash = hash160(&key.public_key().to_compressed());
    let lock = [&[0x76, 0xa9, 0x14][..], &hash[..], &[0x88, 0xac][..]].concat();
    let mut parent = funding_transaction(1 << 20, &lock);
    parent.outputs[0].satoshis = Some(BEEF_COIN);
    let parent_id = parent.id();

    let mut child = Transaction::new();
    child
        .inputs
        .push(TransactionInput::new(parent_id.clone(), 0));
    child.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_binary(&lock).expect("lock"),
        change: false,
    });
    let mut data = Vec::with_capacity(bytes + 8);
    data.extend([0x00, 0x6a, 0x4e]);
    data.extend(u32::try_from(bytes).expect("data length").to_le_bytes());
    data.extend((0..=250u8).cycle().take(bytes));
    child.outputs.push(TransactionOutput {
        satoshis: Some(0),
        locking_script: LockingScript::from_binary(&data).expect("data"),
        change: false,
    });
    let unsigned = child.to_binary();
    let unlock = P2PKH::unlock(&key, SignOutputs::All, false)
        .sign(&SigningContext::new(
            &unsigned,
            0,
            BEEF_COIN,
            &Script::from_binary(&lock).expect("script"),
        ))
        .expect("sign");
    child.inputs[0].unlocking_script = Some(unlock);
    child.invalidate_caches();
    let child_id = child.id();

    let mut beef = Beef::new();
    let index = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_id, BEEF_HEIGHT));
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_raw_tx(child.to_binary(), None);
    let atomic = beef.to_binary_atomic(&child_id).expect("atomic");
    (atomic, child_id, parent_id)
}

fn internalize_big(beef: &[u8]) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx: beef.to_vec(),
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".into(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "census".into(),
                custom_instructions: None,
                tags: None,
            }),
        }],
        description: "census large intake".into(),
        labels: None,
        seek_permission: None,
    }
}

fn create_from_beef(beef: &[u8], child_id: &str) -> CreateActionArgs {
    let mut txid = [0u8; 32];
    hex::decode_to_slice(child_id, &mut txid).expect("txid");
    CreateActionArgs {
        description: "census caller beef".into(),
        input_beef: Some(beef.to_vec()),
        inputs: Some(vec![CreateActionInput {
            outpoint: Outpoint::new(txid, 0),
            input_description: "census caller input".into(),
            unlocking_script: None,
            unlocking_script_length: Some(108),
            sequence_number: None,
        }]),
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x51],
            satoshis: 1_000,
            output_description: "census output".into(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(false),
            no_send: Some(true),
            ..Default::default()
        }),
    }
}

/// A payment of `n` funded coins: the output needs all `n` of them.
fn payment_of(n: usize, no_send: bool) -> CreateActionArgs {
    let mut args = payment_args();
    args.outputs.as_mut().expect("outputs")[0].satoshis = (n as u64 - 1) * SATOSHIS + SATOSHIS / 2;
    args.options.as_mut().expect("options").no_send = Some(no_send);
    args
}

fn epoch_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs_f64()
        * 1e3
}

#[inline(never)]
async fn one(scene: &str, store: &Store, size: usize, beef: &[(Vec<u8>, String)]) -> usize {
    match scene {
        "create" | "process" => {
            let result = store
                .wallet
                .create_action(payment_of(size, scene == "create"), Some(ORIGINATOR))
                .await
                .expect("create action");
            let tx = Transaction::from_binary(result.tx.as_deref().expect("signed")).expect("tx");
            assert_eq!(tx.inputs.len(), size, "every offered coin spent");
            let posts = store.wallet.services().call_count("post_beef");
            assert_eq!(posts, usize::from(scene == "process"), "posted to the mock");
            tx.inputs.len()
        }
        "internalize" => {
            let result = store
                .wallet
                .storage()
                .internalize_action(&store.auth, store.internalize_args())
                .await
                .expect("internalize");
            assert!(result.base.accepted);
            1
        }
        "beef-internalize" => {
            let result = store
                .wallet
                .internalize_action(internalize_big(&beef[0].0), Some(ORIGINATOR))
                .await
                .expect("internalize large");
            assert!(result.accepted);
            beef[0].0.len()
        }
        "beef-create" => {
            let result = store
                .wallet
                .create_action(create_from_beef(&beef[0].0, &beef[0].1), Some(ORIGINATOR))
                .await
                .expect("create from caller beef");
            assert!(result.signable_transaction.is_some());
            beef[0].0.len()
        }
        other => panic!("unknown scene {other}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scene = args.get(1).expect("scene").as_str();
    let size: usize = args.get(2).expect("size").parse().expect("size");
    let iterations: usize = args.get(3).expect("iterations").parse().expect("iters");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    // createAction scenes spend coins of one shared proven parent; the
    // others take the door bench's per-coin seed.
    let store = match scene {
        "create" | "process" => runtime.block_on(seed_shared(size.max(100))),
        _ => runtime.block_on(seed(10)),
    };
    let mut beef = Vec::new();
    if scene.starts_with("beef") {
        let (bytes, child, parent) = big_beef(size * 1_000_000);
        let mut roots = HashSet::new();
        roots.insert((BEEF_HEIGHT, parent));
        runtime.block_on(async {
            store
                .wallet
                .storage()
                .set_max_acceptable_proof_height(BEEF_HEIGHT)
                .await
                .expect("proof gate");
            store
                .wallet
                .storage()
                .set_chain_tracker(Arc::new(CensusRoots(roots)))
                .await;
        });
        eprintln!("beef bytes {}", bytes.len());
        beef.push((bytes, child));
    }
    // Each door call's window in epoch milliseconds, so the profile is cut
    // to the calls and the reset between them is left out.
    let mut windows = Vec::with_capacity(iterations);
    let started = Instant::now();
    let mut measured = std::time::Duration::ZERO;
    for _ in 0..iterations {
        let (from, call) = (epoch_ms(), Instant::now());
        let work = runtime.block_on(one(scene, &store, size, &beef));
        measured += call.elapsed();
        windows.push([from, epoch_ms()]);
        std::hint::black_box(work);
        runtime.block_on(store.reset());
    }
    let wall = started.elapsed();
    let report = serde_json::json!({
        "scene": scene, "size": size, "iterations": iterations,
        "loop_wall_ms": wall.as_secs_f64() * 1e3,
        "door_wall_ms": measured.as_secs_f64() * 1e3,
        "windows_epoch_ms": windows,
    });
    println!("{report}");
}
