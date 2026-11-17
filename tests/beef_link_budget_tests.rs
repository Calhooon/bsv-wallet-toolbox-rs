//! bsv-rs's default BEEF link bounds (GHSA-gvmh-f5gf-5vjj, 0.8.11;
//! GHSA-33fm-jcm5-4m3m, 0.8.12) and this crate's BEEF doors.
//!
//! From 0.8.11, `Transaction::from_beef` and `from_atomic_beef` link a BEEF
//! under a default budget of 16 MiB of re-materialized parent wire bytes,
//! and from 0.8.12 also under a default bound of 56 MiB of the heap those
//! clones hold, and refuse past either with `Error::BeefResourceLimit`. The
//! heap bound is the nearer for a sweep of P2PKH outputs: at 0.8.12 the
//! default links an honest child sweeping 586 P2PKH outputs of one parent
//! and refuses 587 (at 0.8.11 the wire budget alone linked 702 and refused
//! 703). The storage doors that read a BEEF here (`internalize_action`,
//! `create_action`'s input BEEF and stored ancestry) parse it with
//! `Beef::from_binary` and judge it with `verify_valid`, which link nothing,
//! so the budget cannot refuse such a sweep there. These tests pin both
//! halves: the linking door refuses the sweep, and the storage internalize
//! door takes it.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    AuthId, StorageSqlx, WalletServices, WalletStorageProvider, WalletStorageWriter,
};

use common::{accepted, other_lock, sweep_beef, sweep_beef_of};

/// A proven parent of `common::SWEPT` P2PKH outputs and an unmined child
/// spending every one of them into a single output, as an AtomicBEEF.
fn sweep_atomic_beef() -> (Vec<u8>, String) {
    sweep_beef(other_lock())
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

/// The default's onset for a P2PKH sweep of one parent, from both sides:
/// 586 outputs link, 587 are refused at the heap bound bsv-rs 0.8.12 added.
/// RED on bsv-rs 0.8.11, whose wire budget alone links 587.
#[test]
fn the_linking_door_takes_586_swept_outputs_and_refuses_587() {
    let (beef, txid) = sweep_beef_of(586, other_lock());
    if let Err(e) = Transaction::from_beef(&beef, Some(&txid)) {
        panic!("refused a sweep of 586 outputs: {e}");
    }
    let (beef, txid) = sweep_beef_of(587, other_lock());
    match Transaction::from_beef(&beef, Some(&txid)) {
        Err(bsv_rs::Error::BeefResourceLimit(_)) => {}
        Err(e) => panic!("refused 587, but not for the link bounds: {e}"),
        Ok(_) => panic!("linked a sweep of 587 outputs"),
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
