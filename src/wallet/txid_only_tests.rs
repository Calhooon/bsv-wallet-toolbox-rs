//! The create arm's post reads stored BEEF when the response omits it (#466).

use super::busy_door_tests::{fund_tagged, own_lock};
use super::named_input_tests::payment;
use super::Wallet;
use crate::services::mock::MockWalletServices;
use crate::services::*;
use crate::storage::WalletStorageWriter;
use crate::{Result, StorageSqlx};
use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::{Beef, ChainTracker, Transaction};
use bsv_rs::wallet::{CreateActionArgs, CreateActionResult, SendWithResultStatus, WalletInterface};
use std::sync::Mutex;

struct Post {
    beef: Vec<u8>,
    txids: Vec<String>,
}

struct RecordingServices {
    mock: MockWalletServices,
    posts: Mutex<Vec<Post>>,
}

#[async_trait]
impl WalletServices for RecordingServices {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        self.mock.get_chain_tracker().await
    }

    async fn get_height(&self) -> Result<u32> {
        self.mock.get_height().await
    }

    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        self.mock.get_header_for_height(height).await
    }

    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        self.mock.hash_to_header(hash).await
    }

    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult> {
        self.mock.get_raw_tx(txid, use_next).await
    }

    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult> {
        self.mock.get_merkle_path(txid, use_next).await
    }

    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        self.posts.lock().unwrap().push(Post {
            beef: beef.to_vec(),
            txids: txids.to_vec(),
        });
        self.mock.post_beef(beef, txids).await
    }

    async fn get_utxo_status(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        self.mock
            .get_utxo_status(output, format, outpoint, use_next)
            .await
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
        use_next: bool,
    ) -> Result<GetStatusForTxidsResult> {
        self.mock.get_status_for_txids(txids, use_next).await
    }

    async fn get_bsv_exchange_rate(&self) -> Result<f64> {
        self.mock.get_bsv_exchange_rate().await
    }

    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64> {
        self.mock.get_fiat_exchange_rate(currency, base).await
    }

    fn hash_output_script(&self, script: &[u8]) -> String {
        self.mock.hash_output_script(script)
    }

    async fn is_utxo(&self, txid: &str, vout: u32, script: &[u8]) -> UtxoVerdict {
        self.mock.is_utxo(txid, vout, script).await
    }

    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool> {
        self.mock.n_lock_time_is_final(n_lock_time).await
    }

    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool> {
        self.mock.n_lock_time_is_final_for_tx(input).await
    }

    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult> {
        self.mock.get_beef(txid, known_txids).await
    }
}

async fn funded_wallet() -> (Wallet<StorageSqlx, RecordingServices>, String) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("txid-only-test", "fixture").await.unwrap();
    storage.make_available().await.unwrap();
    let key = PrivateKey::from_bytes(&[7; 32]).unwrap();
    let funding = Wallet::new(
        Some(key.clone()),
        storage.clone(),
        MockWalletServices::new(),
    )
    .await
    .unwrap();
    let output = fund_tagged(&funding, &own_lock(&funding), 0).await;
    let parent: String = sqlx::query_scalar("SELECT txid FROM outputs WHERE output_id = ?")
        .bind(output)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let services = RecordingServices {
        mock: MockWalletServices::builder().post_beef_success().build(),
        posts: Mutex::new(Vec::new()),
    };
    let wallet = Wallet::new(Some(key), storage, services).await.unwrap();
    (wallet, parent)
}

fn txid_only_payment(parent: &str) -> CreateActionArgs {
    let mut args = payment(1_000);
    let options = args.options.as_mut().unwrap();
    options.return_txid_only = Some(true);
    options.no_send = Some(false);
    options.accept_delayed_broadcast = Some(false);
    options.known_txids = Some(vec![hex::decode(parent).unwrap().try_into().unwrap()]);
    args
}

fn created(result: bsv_rs::Result<CreateActionResult>) -> CreateActionResult {
    match result {
        Ok(result) => result,
        Err(bsv_rs::Error::WalletError(message)) => panic!("{message}"),
        Err(error) => panic!("{error:?}"),
    }
}

#[tokio::test]
async fn return_txid_only_immediate_posts_stored_beef() {
    let (wallet, parent) = funded_wallet().await;
    let result = created(wallet.create_action(txid_only_payment(&parent), None).await);
    let txid = hex::encode(result.txid.expect("the answer carries its txid"));
    let (raw, input_beef, status): (Vec<u8>, Vec<u8>, String) =
        sqlx::query_as("SELECT raw_tx, input_beef, status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    let signed = Transaction::from_binary(&raw).unwrap();
    assert_eq!(signed.inputs.len(), 1);
    assert!(!signed.inputs[0]
        .unlocking_script
        .as_ref()
        .unwrap()
        .to_binary()
        .is_empty());
    let stored = Beef::from_binary(&input_beef).unwrap();
    assert!(stored.find_txid(&parent).unwrap().tx().is_some());
    assert!(!stored.bumps.is_empty());

    let posts = wallet.services().posts.lock().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].txids.as_slice(), std::slice::from_ref(&txid));
    let posted = Beef::from_binary(&posts[0].beef).unwrap();
    assert_eq!(
        posted.find_txid(&txid).unwrap().tx().unwrap().to_binary(),
        raw
    );
    assert!(posted.find_txid(&parent).unwrap().tx().is_some());
    assert_eq!(posted.txs.len(), 2);
    assert_eq!(posted.bumps.len(), stored.bumps.len());
    assert_eq!(status, "unmined");
    assert!(result.signable_transaction.is_none());
    assert_eq!(wallet.services().mock.call_count("post_beef"), 1);
    assert_eq!(wallet.services().mock.call_count("get_beef"), 0);
    assert_eq!(wallet.services().mock.call_count("get_raw_tx"), 0);
}

async fn assert_side_fields(
    wallet: &Wallet<StorageSqlx, RecordingServices>,
    result: &CreateActionResult,
    no_send: bool,
    delayed: bool,
) {
    let txid = result.txid.expect("the answer retains its txid");
    assert!(result.signable_transaction.is_none());
    if no_send {
        let vouts: Vec<i64> = sqlx::query_scalar(
            "SELECT vout FROM outputs WHERE txid = ? AND change = 1 ORDER BY vout",
        )
        .bind(hex::encode(txid))
        .fetch_all(wallet.storage().pool())
        .await
        .unwrap();
        let change = result.no_send_change.as_ref().unwrap();
        assert!(!change.is_empty());
        assert_eq!(change.len(), vouts.len());
        for (outpoint, vout) in change.iter().zip(vouts) {
            assert_eq!(outpoint.txid, txid);
            assert_eq!(i64::from(outpoint.vout), vout);
        }
    } else {
        assert!(result.no_send_change.is_none());
    }
    let sent = result.send_with_results.as_ref().unwrap();
    if no_send {
        assert!(sent.is_empty());
    } else {
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].txid, txid);
        assert_eq!(
            sent[0].status,
            if delayed {
                SendWithResultStatus::Sending
            } else {
                SendWithResultStatus::Unproven
            }
        );
    }
    assert_eq!(
        wallet.services().mock.call_count("post_beef"),
        usize::from(!no_send && !delayed)
    );
    // Under `returnTXIDOnly` (no `tx`) the answer carries no BEEF either; the
    // other options keep whatever BEEF they carried before #466.
    assert!(
        result.tx.is_some() || result.beef.is_none(),
        "the answer carries no BEEF under returnTXIDOnly"
    );
}

#[tokio::test]
async fn return_txid_only_answer_omits_tx() {
    for (no_send, delayed) in [(true, false), (false, true), (false, false), (true, true)] {
        let (wallet, parent) = funded_wallet().await;
        let mut args = txid_only_payment(&parent);
        let options = args.options.as_mut().unwrap();
        options.no_send = Some(no_send);
        options.accept_delayed_broadcast = Some(delayed);
        let result = created(wallet.create_action(args, None).await);
        assert!(
            result.tx.is_none(),
            "returnTXIDOnly answer carried tx (no_send={no_send}, delayed={delayed})"
        );
        let json = serde_json::to_value(&result).unwrap();
        assert!(json.get("tx").is_none(), "the JSON answer omits tx");
        assert!(json.get("beef").is_none(), "the JSON answer omits beef");
        assert!(json.get("txid").is_some());
        assert_side_fields(&wallet, &result, no_send, delayed).await;
    }
}

#[tokio::test]
async fn other_create_options_keep_tx() {
    for return_txid_only in [None, Some(false)] {
        for (no_send, delayed) in [(true, false), (false, true), (false, false), (true, true)] {
            let (wallet, parent) = funded_wallet().await;
            let mut args = txid_only_payment(&parent);
            let options = args.options.as_mut().unwrap();
            options.return_txid_only = return_txid_only;
            options.no_send = Some(no_send);
            options.accept_delayed_broadcast = Some(delayed);
            let result = created(wallet.create_action(args, None).await);
            let raw: Vec<u8> =
                sqlx::query_scalar("SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?")
                    .bind(hex::encode(result.txid.unwrap()))
                    .fetch_one(wallet.storage().pool())
                    .await
                    .unwrap();
            assert_eq!(result.tx.as_ref(), Some(&raw));
            let json = serde_json::to_value(&result).unwrap();
            assert!(json.get("tx").is_some());
            assert!(result.beef.is_some(), "the other options keep their BEEF");
            assert_side_fields(&wallet, &result, no_send, delayed).await;
        }
    }
}
