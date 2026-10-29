use super::{compute_txid, stored_post_beef, StoredPost};
use crate::services::mock::MockWalletServices;
use crate::storage::sqlx::StorageSqlx;
use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{
    Beef, BeefTx, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
    BEEF_V1,
};
use chrono::Utc;
use std::sync::Arc;

// A post-Genesis mainnet proof, where every era of the node's word lies
// (#426). processAction judges none of the post's unproven spends: they are
// the store's, judged at their own door (the one-door rule, #454).
const PROOF_HEIGHT: u32 = bsv_rs::script::ProtocolEra::MAINNET_GENESIS_HEIGHT + 100;

fn raw_spending(txid: &str) -> Vec<u8> {
    let mut transaction = Transaction::new();
    transaction
        .inputs
        .push(TransactionInput::new(txid.to_string(), 0));
    transaction.outputs.push(TransactionOutput::new(
        1_000,
        LockingScript::from_hex("51").unwrap(),
    ));
    transaction.to_binary()
}

async fn assert_restored_ancestor_order(trimmed: bool) {
    let post_storage = StorageSqlx::in_memory().await.unwrap();
    post_storage
        .migrate("post-order", "test-key")
        .await
        .unwrap();
    let post_services = Arc::new(MockWalletServices::new());
    post_storage.set_services(post_services.clone());

    let ancestor_raw = raw_spending(&"11".repeat(32));
    let ancestor_txid = compute_txid(&ancestor_raw);
    let parent_raw = raw_spending(&ancestor_txid);
    let parent_txid = compute_txid(&parent_raw);
    let child_raw = raw_spending(&parent_txid);
    let child_txid = compute_txid(&child_raw);
    let proof = MerklePath {
        block_height: PROOF_HEIGHT,
        path: vec![vec![MerklePathLeaf {
            offset: 0,
            hash: Some(ancestor_txid.clone()),
            txid: true,
            duplicate: false,
        }]],
    };
    let recorded_at = Utc::now();
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, \
         raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&ancestor_txid)
    .bind(PROOF_HEIGHT)
    .bind("00".repeat(32))
    .bind(&ancestor_txid)
    .bind(proof.to_binary())
    .bind(&ancestor_raw)
    .bind(recorded_at)
    .bind(recorded_at)
    .execute(post_storage.writer())
    .await
    .unwrap();

    let mut input_beef = Beef::new();
    if trimmed {
        input_beef.merge_bump(proof);
        input_beef.merge_txid_only(ancestor_txid.clone());
    }
    input_beef.merge_raw_tx(parent_raw, None);
    let post = StoredPost {
        txid: child_txid.clone(),
        transaction_id: None,
        request_id: None,
        status: "sending",
        raw_tx: Some(child_raw),
        input_beef: Some(input_beef.to_binary()),
    };
    let emitted = stored_post_beef(&post_storage, &[post]).await.unwrap();
    let mut received = Beef::from_binary(&emitted).unwrap();

    // Judge the emitted order directly, without sorting in the receiver.
    assert!(received.verify_valid(false).valid);
    assert_eq!(received.version, BEEF_V1);
    assert_eq!(
        received.txs.iter().map(BeefTx::txid).collect::<Vec<_>>(),
        [ancestor_txid, parent_txid, child_txid]
    );
    assert!(received.txs.iter().all(|tx| !tx.is_txid_only()));
    assert_eq!(post_services.call_count("get_raw_tx"), 0);
    assert_eq!(post_services.call_count("get_merkle_path"), 0);
    assert_eq!(post_services.call_count("post_beef"), 0);
}

#[tokio::test]
async fn post_beef_restores_trimmed_ancestor_in_brc62_order() {
    assert_restored_ancestor_order(true).await;
}

#[tokio::test]
async fn post_beef_restores_missing_ancestor_in_brc62_order() {
    assert_restored_ancestor_order(false).await;
}
