//! bsv-rs 0.8.11's default BEEF link budget (GHSA-gvmh-f5gf-5vjj) and this
//! crate's BEEF doors.
//!
//! From 0.8.11, `Transaction::from_beef` and `from_atomic_beef` link a BEEF
//! under a default budget of 16 MiB of re-materialized parent bytes and
//! refuse past it with `Error::BeefResourceLimit`. An honest child that
//! sweeps more than about 702 P2PKH outputs of one parent passes that
//! budget. The storage doors that read a BEEF here (`internalize_action`,
//! `create_action`'s input BEEF and stored ancestry) parse it with
//! `Beef::from_binary` and judge it with `verify_valid`, which link nothing,
//! so the budget cannot refuse such a sweep there. These tests pin both
//! halves: the linking door refuses the sweep, and the storage internalize
//! door takes it.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    AuthId, PostBeefResult, PostTxResultForTxid, StorageSqlx, WalletServices,
    WalletStorageProvider, WalletStorageWriter,
};

/// Outputs of the one parent the child sweeps: past the 702 P2PKH outputs
/// bsv-rs 0.8.11 states as the default budget's single-sweep reach.
const SWEPT: u32 = 800;

fn pay_lock() -> LockingScript {
    LockingScript::from_hex("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// A proven parent of `SWEPT` P2PKH outputs and an unmined child spending
/// every one of them into a single output, as an AtomicBEEF.
fn sweep_atomic_beef() -> (Vec<u8>, String) {
    let mut parent = Transaction::new();
    let mut parent_input = TransactionInput::new("11".repeat(32), 0);
    parent_input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(parent_input);
    for _ in 0..SWEPT {
        parent.outputs.push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: pay_lock(),
            change: false,
        });
    }
    let parent_txid = parent.id();

    let mut child = Transaction::new();
    child.version = 1;
    for vout in 0..SWEPT {
        let mut input = TransactionInput::new(parent_txid.clone(), vout);
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        child.inputs.push(input);
    }
    child.outputs.push(TransactionOutput {
        satoshis: Some(u64::from(SWEPT) * 1_000 - 500),
        locking_script: pay_lock(),
        change: false,
    });
    let txid = child.id();

    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

fn accepted(txid: &str) -> PostBeefResult {
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

/// The linking door refuses the sweep under its default budget, with the
/// resource error of its own class. RED on bsv-rs 0.8.10, which links it.
#[test]
fn the_linking_door_refuses_the_sweep_by_default() {
    let (beef, txid) = sweep_atomic_beef();
    match Transaction::from_beef(&beef, Some(&txid)) {
        Err(bsv_rs::Error::BeefResourceLimit(_)) => {}
        Err(e) => panic!("refused, but not for the link budget: {e}"),
        Ok(_) => panic!("linked a sweep past the default budget"),
    }
    match Transaction::from_atomic_beef(&beef) {
        Err(bsv_rs::Error::BeefResourceLimit(_)) => {}
        other => panic!("from_atomic_beef: {:?}", other.map(|t| t.id())),
    }
}

/// The storage internalize door parses and judges the same BEEF without
/// linking it: the sweep is accepted and its output stored.
#[tokio::test]
async fn the_storage_internalize_door_takes_the_sweep() {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("beef-link-budget-tests", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();

    let (beef, txid) = sweep_atomic_beef();
    let mock = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(&txid)]))
        .build();
    WalletStorageProvider::set_services(&storage, Arc::new(mock) as Arc<dyn WalletServices>);

    let auth = AuthId {
        identity_key: identity,
        user_id: Some(user.user_id),
        is_active: None,
    };
    let result = storage
        .internalize_action(
            &auth,
            InternalizeActionArgs {
                tx: beef,
                outputs: vec![InternalizeOutput {
                    output_index: 0,
                    protocol: "basket insertion".to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(BasketInsertion {
                        basket: "incoming".to_string(),
                        custom_instructions: None,
                        tags: None,
                    }),
                }],
                description: "a sweep of one parent".to_string(),
                labels: None,
                seek_permission: None,
            },
        )
        .await
        .expect("the sweep is internalized");
    assert!(result.base.accepted);

    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(storage.writer())
        .await
        .unwrap();
    assert_eq!(stored, 1, "the swept output is stored");
}
