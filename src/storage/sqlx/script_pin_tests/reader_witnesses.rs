//! #396: the live internalize door over two bsv-rs 0.9.0-beta.2 reader changes.
//!
//! Each witness is a mainnet payment whose parent is proven at Chronicle's
//! activation height, the tip six blocks on, so the stream and the live word
//! both judge the spend in the post-Chronicle era.

use super::{proof, signed_payment, FixtureTracker};
use crate::services::mock::MockWalletServices;
use crate::storage::sqlx::StorageSqlx;
use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
use bsv_rs::primitives::{PrivateKey, Writer};
use bsv_rs::script::ProtocolEra;
use bsv_rs::transaction::{Beef, BeefTx, Transaction};
use bsv_rs::wallet::{InternalizeActionArgs, InternalizeOutput, WalletPayment};
use std::sync::Arc;

const HEIGHT: u32 = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;

/// An AtomicBEEF of `txs` in the order given, the subject the last raw one
/// by txid; `Beef::to_binary_atomic` sorts, so the writer is called directly.
fn atomic(
    subject: &Transaction,
    txs: Vec<(Transaction, Option<usize>)>,
    parent: &Transaction,
) -> Vec<u8> {
    let mut beef = Beef::new();
    beef.merge_bump(proof(parent, HEIGHT));
    beef.txs = txs
        .into_iter()
        .map(|(tx, bump)| BeefTx::from_tx(tx, bump))
        .collect();
    let mut writer = Writer::new();
    writer.write_u32_le(0x0101_0101);
    let mut id = hex::decode(subject.id()).unwrap();
    id.reverse();
    writer.write_bytes(&id);
    beef.to_writer(&mut writer);
    writer.into_bytes()
}

/// The output rows the door wrote: vout, satoshis, script, spendable, type.
type Rows = Vec<(i64, i64, Vec<u8>, bool, String)>;

/// Drives `internalize_action_internal` on a fresh mainnet store with
/// `parent`'s root known at [`HEIGHT`].
async fn internalize(bytes: Vec<u8>, parent: &Transaction) -> (crate::Result<bool>, Rows) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7))
        .unwrap()
        .public_key()
        .to_hex();
    let mut root: [u8; 32] = hex::decode(parent.id()).unwrap().try_into().unwrap();
    root.reverse();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("reader-fixture", &key).await.unwrap();
    sqlx::query("UPDATE settings SET chain = 'mainnet'")
        .execute(storage.writer())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user(&key).await.unwrap();
    storage.set_services(Arc::new(MockWalletServices::new()));
    storage
        .set_chain_tracker(Arc::new(FixtureTracker {
            roots: vec![(u64::from(HEIGHT), root)],
            tip: HEIGHT + 6,
        }))
        .await;
    let result = super::super::internalize_action::internalize_action_internal(
        &storage,
        user.user_id,
        InternalizeActionArgs {
            tx: bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: "wallet payment".into(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "dGVzdA==".into(),
                    derivation_suffix: "dGVzdA==".into(),
                    sender_identity_key: key.clone(),
                }),
                insertion_remittance: None,
            }],
            description: "reader witness fixture".into(),
            labels: None,
            seek_permission: None,
        },
    )
    .await;
    let rows: Vec<(i64, i64, Vec<u8>, bool, String)> = sqlx::query_as(
        "SELECT vout, satoshis, locking_script, spendable, type FROM outputs ORDER BY vout",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();
    (result.map(|r| r.base.accepted), rows)
}

/// bsv-rs #240 (0.9.0-beta.2): a BEEF whose child precedes its parent is
/// valid; beta.1 refused it as `InputNamesNoElement`. The door accepts it
/// through the stream and the live word, and records the outputs the
/// in-order BEEF records.
#[tokio::test]
async fn internalize_accepts_a_child_written_ahead_of_its_parent() {
    let (parent, child) = signed_payment(0x61);
    let in_order = atomic(
        &child,
        vec![(parent.clone(), Some(0)), (child.clone(), None)],
        &parent,
    );
    let child_ahead = atomic(
        &child,
        vec![(child.clone(), None), (parent.clone(), Some(0))],
        &parent,
    );
    assert_ne!(in_order, child_ahead);

    let (accepted, expected) = internalize(in_order, &parent).await;
    assert!(accepted.unwrap(), "the in-order BEEF");
    let (accepted, rows) = internalize(child_ahead, &parent).await;
    assert!(accepted.unwrap(), "the BEEF with the child ahead");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows, expected);
}
